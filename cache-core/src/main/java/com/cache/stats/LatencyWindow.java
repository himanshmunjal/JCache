package com.cache.stats;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed-size ring buffer of the most recent latency samples, used to report
 * percentiles without keeping an unbounded history.
 *
 * <p>Writers never block. Concurrent writers may occasionally overwrite each
 * other's slot, which is acceptable for monitoring data.
 */
public final class LatencyWindow {

    private final long[] samples;
    private final AtomicLong writes = new AtomicLong();

    /**
     * Creates a window that keeps the last {@code size} samples.
     *
     * @param size number of samples to retain, must be positive
     */
    public LatencyWindow(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Window size must be positive, got: " + size);
        }
        this.samples = new long[size];
    }

    /**
     * Records one sample.
     *
     * @param nanos the observed latency in nanoseconds
     */
    public void record(long nanos) {
        long slot = writes.getAndIncrement() % samples.length;
        samples[(int) slot] = nanos;
    }

    /**
     * Returns the given percentiles over the retained samples, in nanoseconds.
     * All values are 0 if nothing has been recorded.
     *
     * @param percentiles values between 0 and 100
     * @return one result per requested percentile, in the same order
     */
    public long[] percentiles(double... percentiles) {
        int filled = (int) Math.min(writes.get(), samples.length);
        long[] result = new long[percentiles.length];
        if (filled == 0) {
            return result;
        }
        long[] sorted = Arrays.copyOf(samples, filled);
        Arrays.sort(sorted);
        for (int i = 0; i < percentiles.length; i++) {
            int index = (int) Math.ceil(percentiles[i] / 100.0 * filled) - 1;
            result[i] = sorted[Math.max(0, Math.min(index, filled - 1))];
        }
        return result;
    }

    /**
     * Returns the mean of the retained samples in nanoseconds.
     *
     * @return the mean, or 0 if nothing has been recorded
     */
    public double mean() {
        int filled = (int) Math.min(writes.get(), samples.length);
        if (filled == 0) {
            return 0.0;
        }
        long sum = 0;
        for (int i = 0; i < filled; i++) {
            sum += samples[i];
        }
        return (double) sum / filled;
    }

    /** Discards all samples. */
    public void reset() {
        writes.set(0);
    }
}
