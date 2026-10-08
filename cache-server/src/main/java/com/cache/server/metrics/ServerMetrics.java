package com.cache.server.metrics;

import com.cache.stats.LatencyWindow;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Request and connection counters for the server, reported by the
 * {@code STATS} command. Safe to update from every I/O thread at once.
 */
public class ServerMetrics {

    private static final int LATENCY_WINDOW_SIZE = 1000;
    private static final double NANOS_PER_MS = 1_000_000.0;

    private final LongAdder gets = new LongAdder();
    private final LongAdder puts = new LongAdder();
    private final LongAdder deletes = new LongAdder();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LongAdder rateLimited = new LongAdder();
    private final LongAdder totalConnections = new LongAdder();
    private final AtomicLong activeConnections = new AtomicLong();
    private final LatencyWindow latencies = new LatencyWindow(LATENCY_WINDOW_SIZE);
    private final long startTimeMs = System.currentTimeMillis();

    /**
     * Records a GET.
     *
     * @param hit        whether a value was found
     * @param durationNs time spent in the cache
     */
    public void recordGet(boolean hit, long durationNs) {
        gets.increment();
        (hit ? hits : misses).increment();
        latencies.record(durationNs);
    }

    /**
     * Records a PUT.
     *
     * @param durationNs time spent in the cache
     */
    public void recordPut(long durationNs) {
        puts.increment();
        latencies.record(durationNs);
    }

    /**
     * Records a DELETE.
     *
     * @param durationNs time spent in the cache
     */
    public void recordDelete(long durationNs) {
        deletes.increment();
        latencies.record(durationNs);
    }

    /** Records a request that failed. */
    public void recordError() {
        errors.increment();
    }

    /** Records a command rejected by the rate limit. */
    public void recordRateLimited() {
        rateLimited.increment();
    }

    /** Records a newly accepted connection. */
    public void connectionOpened() {
        activeConnections.incrementAndGet();
        totalConnections.increment();
    }

    /** Records a closed connection. */
    public void connectionClosed() {
        activeConnections.updateAndGet(n -> Math.max(0, n - 1));
    }

    /** @return the current values */
    public MetricsSnapshot snapshot() {
        long h = hits.sum();
        long m = misses.sum();
        long g = gets.sum();
        long p = puts.sum();
        long d = deletes.sum();
        long uptimeMs = System.currentTimeMillis() - startTimeMs;
        double hitRate = h + m == 0 ? 0.0 : h * 100.0 / (h + m);
        double opsPerSecond = uptimeMs == 0 ? 0.0 : (g + p + d) * 1000.0 / uptimeMs;
        long[] pct = latencies.percentiles(50, 99);
        return new MetricsSnapshot(g, p, d, h, m, errors.sum(), rateLimited.sum(), hitRate, opsPerSecond,
                activeConnections.get(), totalConnections.sum(), uptimeMs,
                pct[0] / NANOS_PER_MS, pct[1] / NANOS_PER_MS, latencies.mean() / NANOS_PER_MS);
    }

    /**
     * Formats the body of a {@code STATS} reply as space-separated
     * {@code name:value} pairs.
     *
     * @param evictions eviction count reported by the cache
     * @param size      current number of entries
     * @return the formatted metrics
     */
    public String toStatsString(long evictions, int size) {
        MetricsSnapshot s = snapshot();
        return String.format("hits:%d misses:%d evictions:%d size:%d gets:%d puts:%d deletes:%d errors:%d "
                        + "rateLimited:%d hitRate:%.2f%% opsPerSec:%.1f activeConn:%d totalConn:%d uptime:%dms "
                        + "p50:%.3fms p99:%.3fms mean:%.3fms",
                s.hits, s.misses, evictions, size, s.totalGets, s.totalPuts, s.totalDeletes, s.errors,
                s.rateLimited, s.hitRate, s.opsPerSecond, s.activeConnections, s.totalConnections, s.uptimeMs,
                s.p50Ms, s.p99Ms, s.meanMs);
    }

    /** Point-in-time copy of the metrics. */
    public static final class MetricsSnapshot {
        /** GET requests. */
        public final long totalGets;
        /** PUT requests. */
        public final long totalPuts;
        /** DELETE requests. */
        public final long totalDeletes;
        /** GETs that found a value. */
        public final long hits;
        /** GETs that found nothing. */
        public final long misses;
        /** Requests that failed. */
        public final long errors;
        /** Commands rejected by the rate limit. */
        public final long rateLimited;
        /** Hit rate as a percentage between 0 and 100. */
        public final double hitRate;
        /** Average operations per second since start-up. */
        public final double opsPerSecond;
        /** Currently open connections. */
        public final long activeConnections;
        /** Connections accepted since start-up. */
        public final long totalConnections;
        /** Milliseconds since start-up. */
        public final long uptimeMs;
        /** Median cache latency over the recent window, in ms. */
        public final double p50Ms;
        /** 99th percentile cache latency over the recent window, in ms. */
        public final double p99Ms;
        /** Mean cache latency over the recent window, in ms. */
        public final double meanMs;

        private MetricsSnapshot(long totalGets, long totalPuts, long totalDeletes, long hits, long misses,
                                long errors, long rateLimited, double hitRate, double opsPerSecond, long activeConnections,
                                long totalConnections, long uptimeMs, double p50Ms, double p99Ms, double meanMs) {
            this.totalGets = totalGets;
            this.totalPuts = totalPuts;
            this.totalDeletes = totalDeletes;
            this.hits = hits;
            this.misses = misses;
            this.errors = errors;
            this.rateLimited = rateLimited;
            this.hitRate = hitRate;
            this.opsPerSecond = opsPerSecond;
            this.activeConnections = activeConnections;
            this.totalConnections = totalConnections;
            this.uptimeMs = uptimeMs;
            this.p50Ms = p50Ms;
            this.p99Ms = p99Ms;
            this.meanMs = meanMs;
        }

        @Override
        public String toString() {
            return String.format("hits:%d misses:%d gets:%d puts:%d deletes:%d errors:%d rateLimited:%d "
                            + "hitRate:%.2f%% opsPerSec:%.1f activeConn:%d totalConn:%d uptime:%dms p50:%.3fms p99:%.3fms mean:%.3fms",
                    hits, misses, totalGets, totalPuts, totalDeletes, errors, rateLimited, hitRate, opsPerSecond,
                    activeConnections, totalConnections, uptimeMs, p50Ms, p99Ms, meanMs);
        }
    }
}
