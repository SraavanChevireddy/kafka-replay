package com.sraavan.kafkareplay;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and writes captures as newline-delimited JSON (JSONL): one
 * {@link RecordedMessage} per line. This format streams well and is greppable.
 */
public final class CaptureStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CaptureStore() {
    }

    /** Append a single message to the capture file. */
    public static void append(BufferedWriter writer, RecordedMessage message) throws IOException {
        writer.write(MAPPER.writeValueAsString(message));
        writer.newLine();
    }

    /** Write a whole list of messages to a file (overwrites). */
    public static void writeAll(Path path, List<RecordedMessage> messages) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            for (RecordedMessage message : messages) {
                append(writer, message);
            }
        }
    }

    /** Read all messages from a capture file. */
    public static List<RecordedMessage> readAll(Path path) throws IOException {
        List<RecordedMessage> result = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    result.add(MAPPER.readValue(line, RecordedMessage.class));
                }
            }
        }
        return result;
    }
}
