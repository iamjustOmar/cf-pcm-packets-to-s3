# Kafka Packet Chunker (TypeScript)

CLI app that:
- consumes JSON `PacketDto` messages from Kafka,
- listens for a specific partition,
- reconstructs `packet.payload` as PCM stream (`s32le`, mono, 48kHz by default),
- converts each chunk to OPUS,
- writes local `.opus` files in 5 MB PCM-equivalent chunks.

## Data shape

Expected Kafka message value (JSON):

```json
{
  "trackId": "08f8f2b2-346e-4d9b-9e6b-930ef66b4d00",
  "userId": "",
  "packet": {
    "sequenceNumber": 27798,
    "timestamp": 3077091851,
    "payload": [244, 255, 244, 255]
  }
}
```

## Setup

1. Use Node 22 (example with `nvs`):
   - `nvs add 22`
   - `nvs use 22`
2. Install dependencies:
   - `npm install`
3. Optional env file:
   - `cp .env.example .env`
4. No system ffmpeg needed; app uses bundled `ffmpeg-static` dependency.

## Run (dev)

```bash
npm run dev -- --topic packets-topic --partition 2 --key my-partition-key --brokers localhost:9092
```

## Run (compiled)

```bash
npm run build
npm start -- --topic packets-topic --partition 2 --key my-partition-key --brokers localhost:9092
```

## Useful flags

- `--partition` (required): partition to process
- `--key`: Kafka message key to include (exact string match)
- `--topic`: topic name (or `KAFKA_TOPIC`)
- `--brokers`: comma-separated brokers (or `KAFKA_BROKERS`)
- `--group-id`: consumer group id
- `--from-beginning`: read from beginning when no committed offset exists
- `--commit` / `--no-commit`: enable/disable committing offsets (default enabled)
- `--chunk-size-mb`: chunk size in MB (default `5`)
- `--output-dir`: base output directory (default `./output`)
- `--pcm-sample-rate`: input PCM sample rate in Hz (default `48000`)
- `--pcm-channels`: input PCM channels (default `1`)

Output path format:

`<output-dir>/<topic>/partition-<n>/chunk-000001.opus`

## Notes

- Offsets are committed only after a message is processed and written.
- Use `--no-commit` to replay the same data again without advancing offsets.
- When `--key` is set, non-matching messages are skipped (and committed only if `--commit` is enabled).
- Payload bytes are validated (`0..255`), and sample alignment is preserved for 32-bit PCM (`s32le`) across packet boundaries.
- A dedicated consumer group is recommended for partition-focused runs.
- This is local chunking only; multipart upload can be added next.

## Troubleshooting

- If you request a partition that does not exist, the app now exits with the list of available partitions.
- If your group has other active consumers, your process may not be assigned the partition you requested; use a dedicated `--group-id`.
- `--from-beginning` only applies when there are no committed offsets for that group/topic/partition. Use a new `--group-id` to replay full history.
