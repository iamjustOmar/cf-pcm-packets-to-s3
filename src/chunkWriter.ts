import { existsSync, mkdirSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import ffmpegStatic from "ffmpeg-static";

function timestampToFileSegment(ts: number): string {
  return String(ts).replace(/-/g, "x").replace(/\./g, "p");
}

export class ChunkWriter {
  private readonly chunkSizeBytes: number;
  private readonly outputDir: string;
  private readonly fileExtension: string;
  private readonly sampleFormat: string;
  private readonly sampleRate: number;
  private readonly channels: number;
  private chunkIndex = 1;
  private currentChunk = Buffer.alloc(0);
  private currentChunkLabelTimestamp: number | null = null;
  private pendingWrite = Promise.resolve();

  constructor(
    outputDir: string,
    chunkSizeBytes: number,
    fileExtension = "bin",
    sampleFormat = "s32le",
    sampleRate = 48_000,
    channels = 1,
  ) {
    this.outputDir = outputDir;
    this.chunkSizeBytes = chunkSizeBytes;
    this.fileExtension = fileExtension;
    this.sampleFormat = sampleFormat;
    this.sampleRate = sampleRate;
    this.channels = channels;

    if (!existsSync(this.outputDir)) {
      mkdirSync(this.outputDir, { recursive: true });
    }
  }

  write(buffer: Buffer, packetTimestamp: number): Promise<void> {
    if (buffer.length === 0) {
      return this.pendingWrite;
    }

    this.pendingWrite = this.pendingWrite.then(async () => {
      let cursor = 0;
      while (cursor < buffer.length) {
        if (this.currentChunk.length === 0) {
          this.currentChunkLabelTimestamp = packetTimestamp;
        }

        const remainingInChunk = this.chunkSizeBytes - this.currentChunk.length;
        const remainingInBuffer = buffer.length - cursor;
        const bytesToWrite = Math.min(remainingInChunk, remainingInBuffer);
        const slice = buffer.subarray(cursor, cursor + bytesToWrite);
        this.currentChunk = Buffer.concat([this.currentChunk, slice]);
        cursor += bytesToWrite;

        if (this.currentChunk.length >= this.chunkSizeBytes) {
          await this.flushChunk();
        }
      }
    });

    return this.pendingWrite;
  }

  async close(): Promise<void> {
    await this.pendingWrite;
    if (this.currentChunk.length > 0) {
      await this.flushChunk();
    }
  }

  private async flushChunk(): Promise<void> {
    const chunkData = this.currentChunk;
    this.currentChunk = Buffer.alloc(0);

    if (this.currentChunkLabelTimestamp === null) {
      throw new Error("internal: chunk flush without packet.timestamp");
    }

    const ts = this.currentChunkLabelTimestamp;
    this.currentChunkLabelTimestamp = null;

    const tsSeg = timestampToFileSegment(ts);
    const fileName = `chunk-${tsSeg}-${this.chunkIndex.toString().padStart(6, "0")}.${this.fileExtension}`;
    const filePath = path.join(this.outputDir, fileName);
    const opusData = await this.convertPcmToOpus(chunkData);
    await writeFile(filePath, opusData);
    this.chunkIndex += 1;
  }

  private convertPcmToOpus(pcmData: Buffer): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const ffmpegPath = ffmpegStatic ?? "ffmpeg";
      const ffmpeg = spawn(ffmpegPath, [
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        this.sampleFormat,
        "-ar",
        String(this.sampleRate),
        "-ac",
        String(this.channels),
        "-i",
        "pipe:0",
        "-c:a",
        "libopus",
        "-f",
        "opus",
        "pipe:1",
      ]);

      const outputChunks: Buffer[] = [];
      const errorChunks: Buffer[] = [];

      ffmpeg.stdout.on("data", (chunk: Buffer) => {
        outputChunks.push(chunk);
      });

      ffmpeg.stderr.on("data", (chunk: Buffer) => {
        errorChunks.push(chunk);
      });

      ffmpeg.on("error", (error) => {
        reject(
          new Error(
            `Failed to start ffmpeg process (${ffmpegPath}). Cause: ${error.message}`,
          ),
        );
      });

      ffmpeg.on("close", (code) => {
        if (code === 0) {
          resolve(Buffer.concat(outputChunks));
          return;
        }

        reject(
          new Error(
            `ffmpeg failed to convert PCM to OPUS (exit=${code}): ${Buffer.concat(errorChunks).toString("utf-8")}`,
          ),
        );
      });

      ffmpeg.stdin.end(pcmData);
    });
  }
}
