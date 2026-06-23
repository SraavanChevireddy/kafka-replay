package com.sraavan.kafkareplay;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CaptureStoreTest {

    @Test
    void roundTripsMessagesThroughJsonl(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("capture.jsonl");
        List<RecordedMessage> original = List.of(
                new RecordedMessage(1000L, 0, 10L, "k1", "hello"),
                new RecordedMessage(1500L, 1, 11L, null, "{\"nested\":true}")
        );

        CaptureStore.writeAll(file, original);
        List<RecordedMessage> loaded = CaptureStore.readAll(file);

        assertEquals(original, loaded);
    }

    @Test
    void skipsBlankLines(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("c.jsonl");
        java.nio.file.Files.writeString(file,
                "{\"timestamp\":1,\"partition\":0,\"offset\":0,\"key\":null,\"value\":\"a\"}\n\n");
        assertEquals(1, CaptureStore.readAll(file).size());
    }
}
