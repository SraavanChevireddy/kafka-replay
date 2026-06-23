package com.sraavan.kafkareplay;

import java.util.List;

/**
 * Pure timing logic for replay: given recorded messages and a speed factor,
 * compute how long to wait before emitting each message so the original spacing
 * between messages is preserved (or compressed/expanded by the speed factor).
 *
 * Kept free of Kafka and Thread.sleep so it is fully unit-testable.
 */
public final class ReplayScheduler {

    private ReplayScheduler() {
    }

    /**
     * Compute the delay (in millis) to wait BEFORE sending each message,
     * relative to the previous message, scaled by {@code speed}.
     *
     * <ul>
     *   <li>speed = 1.0 → original timing</li>
     *   <li>speed = 2.0 → twice as fast (half the waits)</li>
     *   <li>speed = 0.5 → half speed (double the waits)</li>
     * </ul>
     *
     * The first message always has a delay of 0. Out-of-order or equal
     * timestamps yield a 0 delay (never negative).
     *
     * @param messages recorded messages in the order they will be replayed
     * @param speed    replay speed multiplier; must be &gt; 0
     * @return a list of waits, same length as {@code messages}
     */
    public static List<Long> computeDelaysMillis(List<RecordedMessage> messages, double speed) {
        if (speed <= 0) {
            throw new IllegalArgumentException("speed must be > 0, got " + speed);
        }
        long[] delays = new long[messages.size()];
        for (int i = 1; i < messages.size(); i++) {
            long gap = messages.get(i).timestamp() - messages.get(i - 1).timestamp();
            if (gap < 0) {
                gap = 0; // tolerate out-of-order timestamps
            }
            delays[i] = Math.round(gap / speed);
        }
        return java.util.Arrays.stream(delays).boxed().toList();
    }

    /**
     * Total wall-clock duration of a replay at the given speed, in millis.
     */
    public static long totalDurationMillis(List<RecordedMessage> messages, double speed) {
        return computeDelaysMillis(messages, speed).stream().mapToLong(Long::longValue).sum();
    }
}
