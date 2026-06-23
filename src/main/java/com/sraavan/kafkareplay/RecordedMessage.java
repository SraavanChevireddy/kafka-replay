package com.sraavan.kafkareplay;

/**
 * A single captured Kafka message, serialized to one JSON line in the capture
 * file. Keys and values are stored as strings (base64 could be added later for
 * binary payloads).
 *
 * @param timestamp original record timestamp in epoch millis
 * @param partition source partition
 * @param offset    source offset
 * @param key       record key (may be null)
 * @param value     record value (may be null)
 */
public record RecordedMessage(
        long timestamp,
        int partition,
        long offset,
        String key,
        String value
) {
}
