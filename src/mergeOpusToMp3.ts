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
  /** packet.timestamp from filename (sort only; timeline uses cumulative duration) */
  sortTimestamp: number;
  /** writer chunk file ordinal */
  fileOrdinal: number;
  durationSec: number;
};

/** chunk-<timestampEncoded>-<fileOrdinal>.opus — ts uses p for . x for - */
const CHUNK_OPUS_NAME_RE = /^chunk-([^-]+)-(\d+)\.opus$/i;

function fileSegmentToTimestamp(segment: string): number {
  return Number(segment.replace(/x/g, "-").replace(/p/g, "."));
}

function sortKeyFromChunkFile(absPath: string, stats: Stats): {
  sortTimestamp: number;
  fileOrdinal: number;
} {
  const base = path.basename(absPath);
  const match = base.match(CHUNK_OPUS_NAME_RE);
  if (match) {
    const sortTimestamp = fileSegmentToTimestamp(match[1]!);
    const fileOrdinal = Number(match[2]!);
    if (Number.isFinite(sortTimestamp) && Number.isInteger(fileOrdinal)) {
      return { sortTimestamp, fileOrdinal };
    }
  }
  return { sortTimestamp: fileCreationMs(stats), fileOrdinal: 0 };
}

function fileCreationMs(stats: Stats): number {
  const birth = stats.birthtimeMs;
  if (Number.isFinite(birth) && birth > 0) {
    return birth;
  }
  return stats.mtimeMs;
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

function buildMixFilter(trackCount: number, delaysMs: number[]): string {
  if (trackCount !== delaysMs.length) {
    throw new Error("internal: delays length mismatch");
  }

  if (trackCount === 0) {
    throw new Error("internal: no tracks");
  }

  if (trackCount === 1) {
    const d = delaysMs[0]!;
    return `[0:a]adelay=${d}|${d}[out]`;
  }

  const delayedLabels: string[] = [];
  for (let i = 0; i < trackCount; i += 1) {
    const d = delaysMs[i]!;
    const label = `a${i}`;
    delayedLabels.push(`[${i}:a]adelay=${d}|${d}[${label}]`);
  }

  const mixInputs = delayedLabels.map((_, i) => `[a${i}]`).join("");
  const filter = `${delayedLabels.join(";")};${mixInputs}amix=inputs=${trackCount}:dropout_transition=0[out]`;

  return filter;
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
      demandOption: true,
      describe: "Directory to scan recursively for .opus files",
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

  const rootDir = path.resolve(argv.dir);
  const outputPath = path.resolve(argv.output);

  const opusPaths = await collectOpusFiles(rootDir);
  if (opusPaths.length === 0) {
    throw new Error(`No .opus files found under ${rootDir}`);
  }

  const tracks: OpusTrack[] = [];

  for (const absPath of opusPaths) {
    const [stats, durationSec] = await Promise.all([
      stat(absPath),
      probeDurationSec(ffprobePath, absPath),
    ]);

    const { sortTimestamp, fileOrdinal } = sortKeyFromChunkFile(absPath, stats);

    tracks.push({
      absPath,
      sortTimestamp,
      fileOrdinal,
      durationSec,
    });
  }

  tracks.sort(
    (a, b) => a.sortTimestamp - b.sortTimestamp || a.fileOrdinal - b.fileOrdinal,
  );

  /** end-to-end playback order: sorted by packet.timestamp only; spacing = clip durations */
  let cumulativeMs = 0;
  const delaysMs = tracks.map((t) => {
    const delay = cumulativeMs;
    cumulativeMs += Math.round(t.durationSec * 1000);
    return delay;
  });

  const filterComplex = buildMixFilter(tracks.length, delaysMs);

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

  console.log(`Wrote ${outputPath} (mixed ${tracks.length} tracks)`);
}

void main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
