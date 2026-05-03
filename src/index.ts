import "dotenv/config";
import path from "node:path";
import { Kafka, logLevel } from "kafkajs";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import IORedis from "ioredis";
import { ChunkWriter } from "./chunkWriter";
import { createUploadWorker, type UploadJobData } from "./uploadWorker";
import type { PacketDto } from "./types";

const ONE_MB = 1024 * 1024;
const PCM_BYTES_PER_SAMPLE = 4;
const PCM_SAMPLE_FORMAT = "s32le";

const cli = yargs(hideBin(process.argv))
  .scriptName("packet-chunker")
  .option("brokers", {
    type: "string",
    demandOption: false,
    default: process.env.KAFKA_BROKERS ?? "localhost:9092",
    describe: "Comma-separated Kafka brokers (host:port,host:port)",
  })
  .option("topic", {
    type: "string",
    demandOption: false,
    default: process.env.KAFKA_TOPIC,
    describe: "Kafka topic name",
  })
  .option("partition", {
    type: "number",
    demandOption: true,
    describe: "Kafka partition to consume",
  })
  .option("key", {
    type: "string",
    demandOption: false,
    default: process.env.KAFKA_KEY,
    describe: "Kafka message key filter (exact match)",
  })
  .option("group-id", {
    type: "string",
    demandOption: false,
    default: process.env.KAFKA_GROUP_ID ?? "packet-local-chunker",
    describe: "Consumer group id (use dedicated group for this app)",
  })
  .option("from-beginning", {
    type: "boolean",
    demandOption: false,
    default: false,
    describe: "Read from beginning when no committed offsets exist",
  })
  .option("commit", {
    type: "boolean",
    demandOption: false,
    default: true,
    describe: "Commit offsets after processing (disable with --no-commit)",
  })
  .option("chunk-size-mb", {
    type: "number",
    demandOption: false,
    default: Number(process.env.CHUNK_SIZE_MB ?? 5),
    describe: "Chunk file size in MB",
  })
  .option("output-dir", {
    type: "string",
    demandOption: false,
    default: process.env.OUTPUT_DIR ?? "./output",
    describe: "Base local output directory",
  })
  .option("pcm-sample-rate", {
    type: "number",
    demandOption: false,
    default: Number(process.env.PCM_SAMPLE_RATE ?? 48_000),
    describe: "PCM input sample rate in Hz (used for OPUS encoding)",
  })
  .option("pcm-channels", {
    type: "number",
    demandOption: false,
    default: Number(process.env.PCM_CHANNELS ?? 1),
    describe: "PCM input channel count (used for OPUS encoding)",
  })
  .option("delete-after-upload", {
    type: "boolean",
    demandOption: false,
    default: true,
    describe: "Delete local chunk files after S3 upload (disable with --no-delete-after-upload)",
  })
  .option("redis-url", {
    type: "string",
    demandOption: false,
    default: process.env.REDIS_URL ?? "redis://localhost:6379",
    describe: "Redis connection URL for BullMQ",
  })
  .option("s3-bucket", {
    type: "string",
    demandOption: false,
    default: process.env.S3_BUCKET,
    describe: "S3 bucket name",
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
  .strict()
  .help();

function sanitizePathSegment(raw: string): string {
  return raw.replace(/[^a-zA-Z0-9._-]/g, "");
}

function parseKafkaKey(raw: string): { sessionId: string; userId: string } | null {
  const sep = raw.indexOf(":");
  if (sep < 1 || sep === raw.length - 1) {
    return null;
  }
  const sessionId = sanitizePathSegment(raw.substring(0, sep));
  const userId = sanitizePathSegment(raw.substring(sep + 1));
  if (!sessionId || !userId) {
    return null;
  }
  return { sessionId, userId };
}

async function main(): Promise<void> {
  const args = await cli.parseAsync();

  if (!args.topic) {
    throw new Error("Missing --topic (or KAFKA_TOPIC in environment)");
  }
  const topicName = args.topic;

  if (!Number.isInteger(args.partition) || args.partition < 0) {
    throw new Error("--partition must be a non-negative integer");
  }

  if (!Number.isFinite(args["chunk-size-mb"]) || args["chunk-size-mb"] <= 0) {
    throw new Error("--chunk-size-mb must be a positive number");
  }

  if (!Number.isInteger(args["pcm-sample-rate"]) || args["pcm-sample-rate"] <= 0) {
    throw new Error("--pcm-sample-rate must be a positive integer");
  }

  if (!Number.isInteger(args["pcm-channels"]) || args["pcm-channels"] <= 0) {
    throw new Error("--pcm-channels must be a positive integer");
  }

  if (!args["s3-bucket"]) {
    throw new Error("Missing --s3-bucket (or S3_BUCKET in environment)");
  }

  const brokers = args.brokers
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x.length > 0);

  if (brokers.length === 0) {
    throw new Error("At least one broker is required");
  }

  const baseOutputDir = path.resolve(args["output-dir"]);
  const deleteAfterUpload = args["delete-after-upload"];

  const instanceId = randomUUID();
  const queueName = `chunk-processor-${instanceId}`;

  const redisConnection = new IORedis(args["redis-url"], {
    maxRetriesPerRequest: null,
  });

  const uploadQueue = new Queue<UploadJobData>(queueName, {
    connection: redisConnection,
  });

  const uploadWorker = createUploadWorker({
    queueName,
    connection: redisConnection,
    s3Bucket: args["s3-bucket"],
    s3Region: args["s3-region"],
    s3Endpoint: args["s3-endpoint"],
    s3AccessKeyId: process.env.AWS_ACCESS_KEY_ID,
    s3SecretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    deleteAfterUpload,
    concurrency: 3,
  });

  const chunkSizeBytes = Math.floor(args["chunk-size-mb"] * ONE_MB);

  const seqTtl = 3600;
  const SEQ_KEY_PREFIX = "seq:";

  async function getLastSequenceNumber(writerKey: string): Promise<number | null> {
    const val = await redisConnection.get(`${SEQ_KEY_PREFIX}${writerKey}`);
    if (val === null) {
      return null;
    }
    const num = Number(val);
    return Number.isInteger(num) ? num : null;
  }

  async function setLastSequenceNumber(writerKey: string, seq: number): Promise<void> {
    await redisConnection.set(`${SEQ_KEY_PREFIX}${writerKey}`, String(seq), "EX", seqTtl);
  }

  type WriterEntry = {
    writer: ChunkWriter;
    pendingPcmRemainder: Buffer;
    lastSequenceNumber: number | null;
  };

  const writers = new Map<string, WriterEntry>();

  function getOrCreateWriter(sessionId: string, userId: string): WriterEntry {
    const writerKey = `${sessionId}:${userId}`;
    let entry = writers.get(writerKey);
    if (!entry) {
      const writer = new ChunkWriter({
        baseOutputDir,
        sessionId,
        userId,
        chunkSizeBytes,
        fileExtension: "opus",
        sampleFormat: PCM_SAMPLE_FORMAT,
        sampleRate: args["pcm-sample-rate"],
        channels: args["pcm-channels"],
        onChunkReady: (result) => {
          const currentEntry = writers.get(writerKey);
          if (currentEntry?.lastSequenceNumber !== null && currentEntry?.lastSequenceNumber !== undefined) {
            void setLastSequenceNumber(writerKey, currentEntry.lastSequenceNumber);
          }
          void uploadQueue.add("upload", {
            localPath: result.localPath,
            s3Key: "recordings/" + result.s3Key,
          });
        },
      });
      entry = { writer, pendingPcmRemainder: Buffer.alloc(0), lastSequenceNumber: null };
      writers.set(writerKey, entry);
    }
    return entry;
  }

  const kafkaUsername = process.env.KAFKA_SASL_USERNAME;
  const kafkaPassword = process.env.KAFKA_SASL_PASSWORD;

  const kafka = new Kafka({
    brokers,
    logLevel: logLevel.INFO,
    ...(kafkaUsername && kafkaPassword
      ? {
          sasl: {
            mechanism: "plain",
            username: kafkaUsername,
            password: kafkaPassword,
          },
          ssl: false,
        }
      : {}),
  });

  const admin = kafka.admin();
  await admin.connect();
  const topicOffsets = await admin.fetchTopicOffsets(topicName);
  await admin.disconnect();

  const availablePartitions = topicOffsets
    .map((x) => Number(x.partition))
    .filter((x) => Number.isInteger(x))
    .sort((a, b) => a - b);

  if (!availablePartitions.includes(args.partition)) {
    throw new Error(
      [
        `Requested partition ${args.partition} is not available for topic ${args.topic}.`,
        `Available partitions: [${availablePartitions.join(", ")}]`,
      ].join(" "),
    );
  }

  const consumer = kafka.consumer({
    groupId: args["group-id"],
    maxBytes: 5242880,
    maxWaitTimeInMs: 5000,
    minBytes: 1,
    maxBytesPerPartition: 5242880,
  });

  consumer.on(consumer.events.GROUP_JOIN, (event) => {
    const assigned = event.payload.memberAssignment[topicName] ?? [];
    const hasRequestedPartition = assigned.includes(args.partition);
    console.log(
      `Group assignment for ${topicName}: [${assigned.join(", ")}]`,
    );
    if (!hasRequestedPartition) {
      console.warn(
        [
          `This consumer was not assigned requested partition ${args.partition}.`,
          "Use a dedicated --group-id (or stop other consumers in same group).",
        ].join(" "),
      );
    }
  });

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`Received ${signal}. Closing consumer, queue, and files...`);
    try {
      await consumer.disconnect();
    } finally {
      for (const [key, entry] of writers) {
        if (entry.pendingPcmRemainder.length > 0) {
          console.warn(
            `Dropping trailing ${entry.pendingPcmRemainder.length} byte(s) for ${key} to preserve ${PCM_SAMPLE_FORMAT} sample alignment.`,
          );
        }
        await entry.writer.close();
      }
      await uploadWorker.close();
      await uploadQueue.close();
      await redisConnection.quit();
    }
    process.exit(0);
  };

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  await consumer.connect();
  await consumer.subscribe({
    topic: args.topic,
    fromBeginning: args["from-beginning"],
  });

  console.log(
    [
      "Consumer started",
      `instanceId=${instanceId}`,
      `queue=${queueName}`,
      `topic=${args.topic}`,
      `partition=${args.partition}`,
      `key=${args.key ?? "(any)"}`,
      `groupId=${args["group-id"]}`,
      `commit=${args.commit ? "on" : "off"}`,
      `chunkSizeMb=${args["chunk-size-mb"]}`,
      `pcmFormat=${PCM_SAMPLE_FORMAT}`,
      `pcmSampleRate=${args["pcm-sample-rate"]}`,
      `pcmChannels=${args["pcm-channels"]}`,
      `deleteAfterUpload=${deleteAfterUpload}`,
      `s3Bucket=${args["s3-bucket"]}`,
      `outputDir=${baseOutputDir}`,
    ].join(" | "),
  );

  const commitProcessedOffset = async (
    topic: string,
    partition: number,
    offset: string,
  ): Promise<void> => {
    if (!args.commit) {
      return;
    }
    await consumer.commitOffsets([
      {
        topic,
        partition,
        offset: (BigInt(offset) + 1n).toString(),
      },
    ]);
  };

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({
      topic,
      partition,
      message,
      heartbeat,
      pause,
    }): Promise<void> => {
      if (partition !== args.partition) {
        pause();
        await heartbeat();
        return;
      }

      const rawKey = message.key?.toString("utf-8") ?? "";

      if (typeof args.key === "string") {
        if (rawKey !== args.key) {
          await commitProcessedOffset(topic, partition, message.offset);
          return;
        }
      }

      const parsedKey = parseKafkaKey(rawKey);
      if (!parsedKey) {
        console.warn(
          `Skipping message at offset ${message.offset}: key "${rawKey}" does not match sessionId:userId format`,
        );
        await commitProcessedOffset(topic, partition, message.offset);
        return;
      }

      if (!message.value) {
        await commitProcessedOffset(topic, partition, message.offset);
        return;
      }

      let parsed: PacketDto;
      try {
        parsed = JSON.parse(message.value.toString("utf-8")) as PacketDto;
      } catch (error) {
        console.error(
          `Skipping non-JSON message at offset ${message.offset}:`,
          error,
        );
        await commitProcessedOffset(topic, partition, message.offset);
        return;
      }

      if (!parsed.packet || !Array.isArray(parsed.packet.payload)) {
        console.warn(
          `Skipping message with invalid packet.payload at offset ${message.offset}`,
        );
        await commitProcessedOffset(topic, partition, message.offset);
        return;
      }

      const payload = parsed.packet.payload;
      let invalidByteFound = false;
      for (let i = 0; i < payload.length; i += 1) {
        const value = payload[i];
        if (!Number.isInteger(value) || value < 0 || value > 255) {
          console.warn(
            `Skipping message at offset ${message.offset}: payload contains non-byte value at index ${i}`,
          );
          invalidByteFound = true;
          break;
        }
      }
      if (invalidByteFound) {
        await commitProcessedOffset(topic, partition, message.offset);
        return;
      }

      if (!Number.isFinite(parsed.unixTimestamp)) {
        console.warn(
          `Skipping message at offset ${message.offset}: unixTimestamp missing or not finite`,
        );
        await commitProcessedOffset(topic, partition, message.offset);
        return;
      }

      if (!Number.isInteger(parsed.packet.sequenceNumber)) {
        console.warn(
          `Skipping message at offset ${message.offset}: packet.sequenceNumber missing or not integer`,
        );
        await commitProcessedOffset(topic, partition, message.offset);
        return;
      }

      // console.log(
      //   `received data | offset=${message.offset} key=${rawKey} seq=${parsed.packet.sequenceNumber} payloadBytes=${payload.length} unixTs=${parsed.unixTimestamp}`,
      // );

      const entry = getOrCreateWriter(parsedKey.sessionId, parsedKey.userId);
      const writerKey = `${parsedKey.sessionId}:${parsedKey.userId}`;
      const seq = parsed.packet.sequenceNumber;

      if (entry.lastSequenceNumber === null) {
        entry.lastSequenceNumber = await getLastSequenceNumber(writerKey);
      }

      if (entry.lastSequenceNumber !== null && seq !== entry.lastSequenceNumber + 1) {
        console.log(
          `Sequence gap for ${writerKey} (expected ${entry.lastSequenceNumber + 1}, got ${seq}). Flushing chunk as new segment.`,
        );
        entry.pendingPcmRemainder = Buffer.alloc(0);
        await entry.writer.forceFlush();
      }
      entry.lastSequenceNumber = seq;

      let bytes = Buffer.from(payload);

      if (entry.pendingPcmRemainder.length > 0) {
        bytes = Buffer.concat([entry.pendingPcmRemainder, bytes]);
        entry.pendingPcmRemainder = Buffer.alloc(0);
      }

      if (bytes.length % PCM_BYTES_PER_SAMPLE !== 0) {
        const alignedLength =
          bytes.length - (bytes.length % PCM_BYTES_PER_SAMPLE);
        entry.pendingPcmRemainder = bytes.subarray(alignedLength);
        bytes = bytes.subarray(0, alignedLength);
      }

      if (bytes.length > 0) {
        await entry.writer.write(bytes, parsed.unixTimestamp);
      }

      await commitProcessedOffset(topic, partition, message.offset);

      await heartbeat();
    },
  });
}

void main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
