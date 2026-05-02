import { spawn } from "node:child_process";
import {
  readdir,
  stat,
} from "node:fs/promises";
import path from "node:path";
import type { Stats } from "node:fs";
import ffmpegStatic from "ffmpeg-static";
import ffprobeStatic = require("ffprobe-static");
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

type OpusTrack = {
  absPath: string;
  timestamp: number;
  fileOrdinal: number;
  durationSec: number;
};

/**
 * Filename format from chunkWriter: <timestampEncoded>-<ordinal>.opus
 * timestampEncoded: digits, dots replaced with p, minus replaced with x
 */
const OPUS_NAME_RE = /^([^-]+)-(\d+)\.opus$/i;

function fileSegmentToTimestamp(segment: string): number {
  return Number(segment.replace(/x/g, "-").replace(/p/g, "."));
}

function parseOpusFilename(absPath: string, stats: Stats): {
  timestamp: number;
  fileOrdinal: number;
} {
  const base = path.basename(absPath);
  const match = base.match(OPUS_NAME_RE);
  if (match) {
    const timestamp = fileSegmentToTimestamp(match[1]!);
    const fileOrdinal = Number(match[2]!);
    if (Number.isFinite(timestamp) && Number.isInteger(fileOrdinal)) {
      return { timestamp, fileOrdinal };
    }
  }
  const birth = stats.birthtimeMs;
  const fallback = Number.isFinite(birth) && birth > 0 ? birth : stats.mtimeMs;
  return { timestamp: fallback, fileOrdinal: 0 };
}

async function collectOpusFiles(rootDir: string): Promise<string[]> {
  const results: string[] = [];

  async function walk(dir: string): Promise<void> {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".opus")) {
        results.push(fullPath);
      }
    }
  }

  await walk(rootDir);
  return results;
}

async function probeDurationSec(ffprobePath: string, filePath: string): Promise<number> {
  const args = [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ];

  return await new Promise((resolve, reject) => {
    const child = spawn(ffprobePath, args);
    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];

    child.stdout.on("data", (chunk: Buffer) => {
      stdoutChunks.push(chunk);
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderrChunks.push(chunk);
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `ffprobe failed for ${filePath} (exit=${code}): ${Buffer.concat(stderrChunks).toString("utf-8")}`,
          ),
        );
        return;
      }

      const text = Buffer.concat(stdoutChunks).toString("utf-8").trim();
      const value = Number.parseFloat(text);
      if (!Number.isFinite(value) || value <= 0) {
        reject(new Error(`ffprobe returned invalid duration for ${filePath}: "${text}"`));
        return;
      }

      resolve(value);
    });
  });
}

function runFfmpeg(ffmpegPath: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(ffmpegPath, args);
    const stderrChunks: Buffer[] = [];

    child.stderr.on("data", (chunk: Buffer) => {
      stderrChunks.push(chunk);
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(
        new Error(
          `ffmpeg failed (exit=${code}): ${Buffer.concat(stderrChunks).toString("utf-8")}`,
        ),
      );
    });
  });
}

function buildTimelineFilter(tracks: OpusTrack[], t0: number): string {
  if (tracks.length === 0) {
    throw new Error("internal: no tracks");
  }

  if (tracks.length === 1) {
    const d = Math.max(0, Math.round((tracks[0]!.timestamp - t0) * 1000));
    return `[0:a]adelay=${d}|${d}[out]`;
  }

  const delayedLabels: string[] = [];
  for (let i = 0; i < tracks.length; i += 1) {
    const delayMs = Math.max(0, Math.round((tracks[i]!.timestamp - t0) * 1000));
    const label = `a${i}`;
    delayedLabels.push(`[${i}:a]adelay=${delayMs}|${delayMs}[${label}]`);
  }

  const mixInputs = delayedLabels.map((_, i) => `[a${i}]`).join("");
  return `${delayedLabels.join(";")};${mixInputs}amix=inputs=${tracks.length}:dropout_transition=0:normalize=0[out]`;
}

async function main(): Promise<void> {
  const ffmpegPath = ffmpegStatic;
  const ffprobePath = ffprobeStatic.path;

  if (!ffmpegPath) {
    throw new Error("ffmpeg-static path missing");
  }

  const argv = await yargs(hideBin(process.argv))
    .scriptName("merge-opus")
    .option("dir", {
      type: "string",
      array: true,
      demandOption: true,
      describe: "One or more directories to scan recursively for .opus files",
    })
    .option("output", {
      type: "string",
      demandOption: false,
      default: "merged.opus",
      describe: "Output .opus path (Ogg Opus)",
    })
    .strict()
    .help()
    .parseAsync();

  const dirs = argv.dir.map((d) => path.resolve(d));
  const outputPath = path.resolve(argv.output);

  const opusPaths: string[] = [];
  for (const dir of dirs) {
    const found = await collectOpusFiles(dir);
    opusPaths.push(...found);
  }

  if (opusPaths.length === 0) {
    throw new Error(`No .opus files found under [${dirs.join(", ")}]`);
  }

  const tracks: OpusTrack[] = [];

  for (const absPath of opusPaths) {
    const [stats, durationSec] = await Promise.all([
      stat(absPath),
      probeDurationSec(ffprobePath, absPath),
    ]);

    const { timestamp, fileOrdinal } = parseOpusFilename(absPath, stats);

    tracks.push({
      absPath,
      timestamp,
      fileOrdinal,
      durationSec,
    });
  }

  tracks.sort(
    (a, b) => a.timestamp - b.timestamp || a.fileOrdinal - b.fileOrdinal,
  );

  const t0 = tracks[0]!.timestamp;

  console.log(`Found ${tracks.length} tracks. t0=${t0} (unix seconds)`);
  for (const t of tracks) {
    const offsetMs = Math.round((t.timestamp - t0) * 1000);
    console.log(`  ${path.basename(t.absPath)}  ts=${t.timestamp}  offset=${offsetMs}ms  dur=${(t.durationSec * 1000).toFixed(0)}ms`);
  }

  const filterComplex = buildTimelineFilter(tracks, t0);

  const args: string[] = ["-y"];

  for (const t of tracks) {
    args.push("-i", t.absPath);
  }

  args.push(
    "-filter_complex",
    filterComplex,
    "-map",
    "[out]",
    "-c:a",
    "libopus",
    "-b:a",
    "128k",
    "-f",
    "opus",
    outputPath,
  );

  await runFfmpeg(ffmpegPath, args);

  console.log(`Wrote ${outputPath} (${tracks.length} tracks on timeline)`);
}

void main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
