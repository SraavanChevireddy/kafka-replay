# kafka-replay

> Record a Kafka topic to a file, then **replay it later with the exact original timing** — into another topic, another environment, or your laptop. Reproduce a production incident from the real event stream instead of guessing.

[![CI](https://github.com/SraavanChevireddy/kafka-replay/actions/workflows/ci.yml/badge.svg)](https://github.com/SraavanChevireddy/kafka-replay/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-clients-231F20.svg)](https://kafka.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A consumer fell over in prod at 3:02am. By the time you look, the messages are gone (or buried under millions of new ones). **kafka-replay** lets you capture a slice of a topic to a plain JSONL file, then replay it on demand — at original speed, 10x faster, or slow-mo — so you can reproduce the exact sequence and timing against a fix.

- **Capture** any topic to a greppable JSONL file (timestamp, partition, offset, key, value per line).
- **Replay** with faithful inter-message timing, scaled by a `--speed` factor.
- **Retarget** — replay into a different topic or cluster (great for staging repro).
- **Single fat-jar**, no framework. Java 21.

## Build

```bash
git clone https://github.com/SraavanChevireddy/kafka-replay.git
cd kafka-replay
./mvnw clean package      # produces target/kafka-replay.jar
```

## Capture

```bash
java -jar target/kafka-replay.jar capture \
  --topic orders \
  --out orders.jsonl \
  --bootstrap localhost:9092 \
  --max 10000 \
  --idle 5000          # stop after 5s of no new messages
```

Each line of `orders.jsonl` looks like:

```json
{"timestamp":1704110400050,"partition":0,"offset":4412,"key":"order-8821","value":"{...}"}
```

## Replay

```bash
# Original timing, into a scratch topic
java -jar target/kafka-replay.jar replay \
  --in orders.jsonl --topic orders-replay --bootstrap localhost:9092 --speed 1.0

# Replay 5x faster
... --speed 5.0

# Slow-motion (half speed) to watch a race condition unfold
... --speed 0.5
```

## How the timing works

The replay engine reads the recorded timestamps and reproduces the *gaps between messages*, scaled by `speed`. The first message fires immediately; each subsequent message waits `(gap / speed)` ms. So a burst that originally arrived over 2 seconds replays over 2 seconds at `1.0`, or 0.4s at `5.0`.

This logic lives in [`ReplayScheduler`](src/main/java/com/sraavan/kafkareplay/ReplayScheduler.java) and is pure + fully unit-tested — no Kafka or sleeps needed to verify it.

## Use cases

- **Incident reproduction** — capture the offending window, replay against a patched consumer.
- **Load shape testing** — replay real traffic patterns (not synthetic uniform load) into staging.
- **Demo/dev seeding** — capture once, replay realistic data into any dev cluster.

## Notes & roadmap

- Keys/values are captured as strings today. Binary payloads (base64) and headers are an easy next PR.
- Captured offsets/partitions are preserved in the file for analysis, though replay publishes fresh.
- Ideas welcome: time-window filtering on capture, key-based filtering, parallel partition replay.

## License

[MIT](LICENSE) © Sraavan Chevireddy
