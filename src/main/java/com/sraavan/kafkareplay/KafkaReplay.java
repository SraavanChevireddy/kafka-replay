package com.sraavan.kafkareplay;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Capture messages from a Kafka topic to a file, and replay a captured file
 * into a topic while preserving the original inter-message timing.
 */
public class KafkaReplay {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplay.class);

    private final String bootstrapServers;

    public KafkaReplay(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Consume up to {@code maxMessages} from {@code topic} and write them to
     * {@code outFile}. Stops early after {@code idleMs} of no new records.
     */
    public int capture(String topic, Path outFile, int maxMessages, long idleMs) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "kafka-replay-capture-" + System.currentTimeMillis());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        int captured = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             BufferedWriter writer = Files.newBufferedWriter(outFile)) {
            consumer.subscribe(List.of(topic));
            long lastRecordTime = System.currentTimeMillis();

            while (captured < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    if (System.currentTimeMillis() - lastRecordTime > idleMs) {
                        break;
                    }
                    continue;
                }
                for (ConsumerRecord<String, String> rec : records) {
                    CaptureStore.append(writer, new RecordedMessage(
                            rec.timestamp(), rec.partition(), rec.offset(), rec.key(), rec.value()));
                    captured++;
                    lastRecordTime = System.currentTimeMillis();
                    if (captured >= maxMessages) {
                        break;
                    }
                }
            }
        }
        log.info("Captured {} messages from '{}' to {}", captured, topic, outFile);
        return captured;
    }

    /**
     * Replay a captured file into {@code topic}, sleeping between messages so
     * the original timing is reproduced (scaled by {@code speed}).
     */
    public int replay(Path inFile, String topic, double speed) throws IOException, InterruptedException {
        List<RecordedMessage> messages = CaptureStore.readAll(inFile);
        List<Long> delays = ReplayScheduler.computeDelaysMillis(messages, speed);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        int sent = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messages.size(); i++) {
                long wait = delays.get(i);
                if (wait > 0) {
                    Thread.sleep(wait);
                }
                RecordedMessage m = messages.get(i);
                producer.send(new ProducerRecord<>(topic, m.key(), m.value()));
                sent++;
            }
            producer.flush();
        }
        log.info("Replayed {} messages into '{}' at {}x speed", sent, topic, speed);
        return sent;
    }
}
