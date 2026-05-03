import "dotenv/config";
import { spawn } from "node:child_process";
import {
  readdir,
  stat,
  rm,
  writeFile,
  mkdir,
} from "node:fs/promises";
import path from "node:path";
import type { Stats } from "node:fs";
import ffmpegStatic from "ffmpeg-static";
import ffprobeStatic = require("ffprobe-static");
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} from "@aws-sdk/client-s3";
import { createReadStream } from "node:fs";

type OpusTrack = {
  absPath: string;
  timestamp: number;
  fileOrdinal: number;
  durationSec: number;
};

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

async function downloadS3OpusFiles(
  s3: S3Client,
  bucket: string,
  prefixes: string[],
): Promise<string> {
  const tempDir = path.resolve("output", "tmp");
  await mkdir(tempDir, { recursive: true });

  for (const prefix of prefixes) {
    let continuationToken: string | undefined;

    do {
      const resp = await s3.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        }),
      );

      const normalizedPrefix = prefix.endsWith("/") ? prefix : `${prefix}/`;

      for (const obj of resp.Contents ?? []) {
        if (!obj.Key || !obj.Key.toLowerCase().endsWith(".opus")) {
          continue;
        }

        const relativePath = obj.Key.slice(normalizedPrefix.length);
        if (!relativePath.includes("/")) {
          continue;
        }

        const getResp = await s3.send(
          new GetObjectCommand({ Bucket: bucket, Key: obj.Key }),
        );

        if (!getResp.Body) {
          continue;
        }

        const bodyBytes = await getResp.Body.transformToByteArray();
        const localPath = path.join(tempDir, obj.Key);
        await mkdir(path.dirname(localPath), { recursive: true });
        await writeFile(localPath, bodyBytes);

        console.log(`Downloaded s3://${bucket}/${obj.Key} → ${localPath}`);
      }

      continuationToken = resp.IsTruncated
        ? resp.NextContinuationToken
        : undefined;
    } while (continuationToken);
  }

  return tempDir;
}

const MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024;

async function multipartUpload(
  s3: S3Client,
  bucket: string,
  key: string,
  filePath: string,
): Promise<void> {
  const fileStats = await stat(filePath);
  const fileSize = fileStats.size;

  const { UploadId } = await s3.send(
    new CreateMultipartUploadCommand({
      Bucket: bucket,
      Key: key,
      ContentType: "audio/ogg",
    }),
  );

  if (!UploadId) {
    throw new Error("Failed to initiate multipart upload");
  }

  const parts: { ETag: string; PartNumber: number }[] = [];

  try {
    let partNumber = 1;
    let offset = 0;

    while (offset < fileSize) {
      const end = Math.min(offset + MULTIPART_CHUNK_SIZE, fileSize);

      const chunk = await new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = [];
        const stream = createReadStream(filePath, { start: offset, end: end - 1 });
        stream.on("data", (c: Buffer) => chunks.push(c));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks)));
      });

      const { ETag } = await s3.send(
        new UploadPartCommand({
          Bucket: bucket,
          Key: key,
          UploadId,
          PartNumber: partNumber,
          Body: chunk,
        }),
      );

      if (!ETag) {
        throw new Error(`Missing ETag for part ${partNumber}`);
      }

      parts.push({ ETag, PartNumber: partNumber });
      console.log(`  Uploaded part ${partNumber} (${chunk.length} bytes)`);

      partNumber += 1;
      offset = end;
    }

    await s3.send(
      new CompleteMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        UploadId,
        MultipartUpload: { Parts: parts },
      }),
    );
  } catch (err) {
    await s3.send(
      new AbortMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        UploadId,
      }),
    ).catch(() => {});
    throw err;
  }
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
    .option("source", {
      type: "string",
      choices: ["local", "s3"] as const,
      demandOption: false,
      default: "local",
      describe: "Source of opus files: local directories or S3 prefixes",
    })
    .option("dir", {
      type: "string",
      array: true,
      describe: "One or more local directories to scan recursively (e.g. recordings/sessionA scans all users inside)",
    })
    .option("s3-prefix", {
      type: "string",
      array: true,
      describe: "One or more S3 key prefixes to scan (e.g. recordings/sessionA grabs all users inside)",
    })
    .option("s3-bucket", {
      type: "string",
      demandOption: false,
      default: process.env.S3_BUCKET,
      describe: "S3 bucket name (when --source s3)",
    })
    .option("s3-region", {
      type: "string",
      demandOption: false,
      default: process.env.S3_REGION ?? "us-east-1",
      describe: "S3 region",
    })
    .option("s3-endpoint", {
      type: "string",
      demandOption: false,
      default: process.env.S3_ENDPOINT,
      describe: "Custom S3 endpoint (e.g. MinIO)",
    })
    .option("output", {
      type: "string",
      demandOption: false,
      default: "merged.opus",
      describe: "Output filename (used locally and as S3 object name)",
    })
    .option("upload-to-s3", {
      type: "boolean",
      demandOption: false,
      default: false,
      describe: "Upload merged file to S3 instead of keeping locally (uses first --s3-prefix or --dir as S3 dest path)",
    })
    .check((args) => {
      if (args.source === "local" && (!args.dir || args.dir.length === 0)) {
        throw new Error("--dir is required when --source is local");
      }
      if (args.source === "s3" && (!args["s3-prefix"] || args["s3-prefix"].length === 0)) {
        throw new Error("--s3-prefix is required when --source is s3");
      }
      if ((args.source === "s3" || args["upload-to-s3"]) && !args["s3-bucket"]) {
        throw new Error("--s3-bucket (or S3_BUCKET env) is required when using S3");
      }
      return true;
    })
    .strict()
    .help()
    .parseAsync();

  const outputFilename = argv.output;
  const uploadToS3 = argv["upload-to-s3"];
  let tempDir: string | null = null;
  let localDirs: string[];

  const needsS3 = argv.source === "s3" || uploadToS3;
  const s3 = needsS3
    ? new S3Client({
        region: argv["s3-region"],
        ...(argv["s3-endpoint"] ? { endpoint: argv["s3-endpoint"] } : {}),
        ...(process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY
          ? {
              credentials: {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
              },
            }
          : {}),
      })
    : null;

  if (argv.source === "s3") {
    console.log(`Downloading opus files from s3://${argv["s3-bucket"]} prefixes: [${argv["s3-prefix"]!.join(", ")}]`);
    tempDir = await downloadS3OpusFiles(s3!, argv["s3-bucket"]!, argv["s3-prefix"]!);
    localDirs = [tempDir];
  } else {
    localDirs = argv.dir!.map((d) => path.resolve(d));
  }

  const s3DestPrefix = argv["s3-prefix"]?.[0] ?? argv.dir?.[0] ?? "";
  const outputPath = uploadToS3
    ? path.resolve("output", "tmp", outputFilename)
    : path.resolve(outputFilename);

  if (uploadToS3) {
    await mkdir(path.dirname(outputPath), { recursive: true });
  }

  try {
    const opusPaths: string[] = [];
    for (const dir of localDirs) {
      const found = await collectOpusFiles(dir);
      opusPaths.push(...found);
    }

    if (opusPaths.length === 0) {
      throw new Error(`No .opus files found under [${localDirs.join(", ")}]`);
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

    console.log(`Merged ${tracks.length} tracks on timeline → ${outputPath}`);

    if (uploadToS3 && s3) {
      const s3Key = `${s3DestPrefix}/${path.basename(outputFilename)}`.replace(/^\/+/, "");

      console.log(`Uploading to s3://${argv["s3-bucket"]}/${s3Key} (multipart, ${MULTIPART_CHUNK_SIZE / 1024 / 1024}MB chunks)...`);
      await multipartUpload(s3, argv["s3-bucket"]!, s3Key, outputPath);

      console.log(`Uploaded s3://${argv["s3-bucket"]}/${s3Key}`);
      await rm(outputPath, { force: true });
    }
  } finally {
    if (tempDir) {
      await rm(tempDir, { recursive: true, force: true });
      console.log(`Cleaned up temp dir ${tempDir}`);
    }
  }
}

void main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
