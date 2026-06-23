package com.sraavan.kafkareplay;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Tiny CLI entry point.
 *
 * <pre>
 * # Capture
 * java -jar kafka-replay.jar capture --topic orders --out orders.jsonl \
 *      --bootstrap localhost:9092 --max 10000 --idle 5000
 *
 * # Replay (at original speed; use --speed 2 for 2x faster)
 * java -jar kafka-replay.jar replay --in orders.jsonl --topic orders-replay \
 *      --bootstrap localhost:9092 --speed 1.0
 * </pre>
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        String command = args[0];
        Map<String, String> opts = parseOpts(args);
        String bootstrap = opts.getOrDefault("bootstrap", "localhost:9092");
        KafkaReplay tool = new KafkaReplay(bootstrap);

        switch (command) {
            case "capture" -> {
                String topic = required(opts, "topic");
                Path out = Path.of(required(opts, "out"));
                int max = Integer.parseInt(opts.getOrDefault("max", "100000"));
                long idle = Long.parseLong(opts.getOrDefault("idle", "5000"));
                tool.capture(topic, out, max, idle);
            }
            case "replay" -> {
                Path in = Path.of(required(opts, "in"));
                String topic = required(opts, "topic");
                double speed = Double.parseDouble(opts.getOrDefault("speed", "1.0"));
                tool.replay(in, topic, speed);
            }
            default -> {
                System.err.println("Unknown command: " + command);
                usage();
                System.exit(2);
            }
        }
    }

    private static Map<String, String> parseOpts(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                opts.put(args[i].substring(2), args[i + 1]);
            }
        }
        return opts;
    }

    private static String required(Map<String, String> opts, String key) {
        String value = opts.get(key);
        if (value == null) {
            System.err.println("Missing required --" + key);
            System.exit(2);
        }
        return value;
    }

    private static void usage() {
        System.err.println("""
                Usage:
                  capture --topic <t> --out <file> [--bootstrap host:port] [--max N] [--idle ms]
                  replay  --in <file> --topic <t>  [--bootstrap host:port] [--speed 1.0]
                """);
    }
}
