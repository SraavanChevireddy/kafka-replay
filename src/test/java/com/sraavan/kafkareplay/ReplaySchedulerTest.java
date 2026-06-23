package com.sraavan.kafkareplay;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReplaySchedulerTest {

    private static RecordedMessage at(long ts) {
        return new RecordedMessage(ts, 0, ts, null, "v");
    }

    @Test
    void firstMessageHasZeroDelay() {
        var delays = ReplayScheduler.computeDelaysMillis(List.of(at(1000), at(1500)), 1.0);
        assertEquals(0L, delays.get(0));
    }

    @Test
    void preservesOriginalGapAtSpeedOne() {
        var delays = ReplayScheduler.computeDelaysMillis(List.of(at(1000), at(1500), at(3000)), 1.0);
        assertEquals(List.of(0L, 500L, 1500L), delays);
    }

    @Test
    void doubleSpeedHalvesDelays() {
        var delays = ReplayScheduler.computeDelaysMillis(List.of(at(0), at(1000), at(2000)), 2.0);
        assertEquals(List.of(0L, 500L, 500L), delays);
    }

    @Test
    void halfSpeedDoublesDelays() {
        var delays = ReplayScheduler.computeDelaysMillis(List.of(at(0), at(1000)), 0.5);
        assertEquals(List.of(0L, 2000L), delays);
    }

    @Test
    void outOfOrderTimestampsClampToZero() {
        var delays = ReplayScheduler.computeDelaysMillis(List.of(at(2000), at(1000)), 1.0);
        assertEquals(List.of(0L, 0L), delays);
    }

    @Test
    void totalDurationSumsDelays() {
        long total = ReplayScheduler.totalDurationMillis(List.of(at(0), at(1000), at(2500)), 1.0);
        assertEquals(2500L, total);
    }

    @Test
    void zeroSpeedIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ReplayScheduler.computeDelaysMillis(List.of(at(0)), 0.0));
    }

    @Test
    void emptyInputYieldsEmptyDelays() {
        assertTrue(ReplayScheduler.computeDelaysMillis(List.of(), 1.0).isEmpty());
    }
}
