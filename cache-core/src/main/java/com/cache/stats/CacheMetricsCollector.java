package com.cache.stats;

import com.cache.api.CacheStats;

import java.time.Instant;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe counters and latency tracking for a single cache instance.
 *
 * <p>Counters use {@link LongAdder}, which scales better than
 * {@code AtomicLong} when many threads update them at once. Latency
 * percentiles are computed over the last {@value #LATENCY_WINDOW_SIZE} timed
 * lookups.
 */
public class CacheMetricsCollector {

    static final int LATENCY_WINDOW_SIZE = 1000;

    private final String cacheName;
    private final Instant createdAt = Instant.now();

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder puts = new LongAdder();
    private final LongAdder policyEvictions = new LongAdder();
    private final LongAdder manualEvictions = new LongAdder();
    private final LatencyWindow latencies = new LatencyWindow(LATENCY_WINDOW_SIZE);

    /**
     * Creates a collector.
     *
     * @param cacheName name shown in reports
     */
    public CacheMetricsCollector(String cacheName) {
        if (cacheName == null || cacheName.isBlank()) {
            throw new IllegalArgumentException("Cache name cannot be null or blank");
        }
        this.cacheName = cacheName;
    }

    /** Creates a collector named {@code default-cache}. */
    public CacheMetricsCollector() {
        this("default-cache");
    }

    /**
     * Returns a start time to pass to {@link #recordHit(long)} or {@link #recordMiss(long)}.
     *
     * @return the current {@link System#nanoTime()}
     */
    public long startTimer() {
        return System.nanoTime();
    }

    /**
     * Records a hit and the time since {@code startNanos}.
     *
     * @param startNanos value returned by {@link #startTimer()}
     */
    public void recordHit(long startNanos) {
        hits.increment();
        latencies.record(System.nanoTime() - startNanos);
    }

    /**
     * Records a miss and the time since {@code startNanos}.
     *
     * @param startNanos value returned by {@link #startTimer()}
     */
    public void recordMiss(long startNanos) {
        misses.increment();
        latencies.record(System.nanoTime() - startNanos);
    }

    /** Records a hit without timing it. */
    public void recordHit() {
        hits.increment();
    }

    /** Records a miss without timing it. */
    public void recordMiss() {
        misses.increment();
    }

    /** Records a put. */
    public void recordPut() {
        puts.increment();
    }

    /** Records an eviction made by the eviction policy. */
    public void recordEviction() {
        policyEvictions.increment();
    }

    /** Records an explicit removal by the caller. */
    public void recordManualEviction() {
        manualEvictions.increment();
    }

    /** Resets all counters and discards latency samples. */
    public void reset() {
        hits.reset();
        misses.reset();
        puts.reset();
        policyEvictions.reset();
        manualEvictions.reset();
        latencies.reset();
    }

    /**
     * Returns a consistent-enough view of every metric.
     *
     * @return the current values
     */
    public MetricsSnapshot snapshot() {
        long[] p = latencies.percentiles(50, 99);
        return new MetricsSnapshot(cacheName, Instant.now(), createdAt,
                hits.sum(), misses.sum(), puts.sum(), policyEvictions.sum(), manualEvictions.sum(),
                latencies.mean() / 1_000.0, p[0] / 1_000.0, p[1] / 1_000.0);
    }

    /**
     * Converts the counters to the generic {@link CacheStats} type.
     *
     * @return the stats; manual evictions are left out, as in every {@link CacheStats}
     */
    public CacheStats toCacheStats() {
        return CacheStats.of(hits.sum(), misses.sum(), policyEvictions.sum());
    }

    /** @return total hits */
    public long getHits() {
        return hits.sum();
    }

    /** @return total misses */
    public long getMisses() {
        return misses.sum();
    }

    /** @return total puts */
    public long getTotalPuts() {
        return puts.sum();
    }

    /** @return evictions made by the policy */
    public long getPolicyEvictions() {
        return policyEvictions.sum();
    }

    /** @return explicit removals */
    public long getManualEvictions() {
        return manualEvictions.sum();
    }

    /** @return policy and manual evictions combined */
    public long getTotalEvictions() {
        return policyEvictions.sum() + manualEvictions.sum();
    }

    /** @return {@code hits / (hits + misses)}, or 0 if there were no lookups */
    public double getHitRate() {
        return toCacheStats().hitRate();
    }

    /** @return the name given at construction */
    public String getCacheName() {
        return cacheName;
    }

    /** @return when this collector was created */
    public Instant getCreatedAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return String.format("CacheMetricsCollector{name='%s', hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d}",
                cacheName, getHits(), getMisses(), getHitRate() * 100, getTotalEvictions());
    }

    /** Immutable view of a collector's metrics at one point in time. */
    public static final class MetricsSnapshot {
        private final String cacheName;
        private final Instant snapshotTime;
        private final Instant cacheCreatedAt;
        private final long hits;
        private final long misses;
        private final long totalPuts;
        private final long policyEvictions;
        private final long manualEvictions;
        private final double avgLoadTimeMicros;
        private final double p50LatencyMicros;
        private final double p99LatencyMicros;

        MetricsSnapshot(String cacheName, Instant snapshotTime, Instant cacheCreatedAt,
                        long hits, long misses, long totalPuts, long policyEvictions, long manualEvictions,
                        double avgLoadTimeMicros, double p50LatencyMicros, double p99LatencyMicros) {
            this.cacheName = cacheName;
            this.snapshotTime = snapshotTime;
            this.cacheCreatedAt = cacheCreatedAt;
            this.hits = hits;
            this.misses = misses;
            this.totalPuts = totalPuts;
            this.policyEvictions = policyEvictions;
            this.manualEvictions = manualEvictions;
            this.avgLoadTimeMicros = avgLoadTimeMicros;
            this.p50LatencyMicros = p50LatencyMicros;
            this.p99LatencyMicros = p99LatencyMicros;
        }

        /** @return the cache name */
        public String getCacheName() {
            return cacheName;
        }

        /** @return when the snapshot was taken */
        public Instant getSnapshotTime() {
            return snapshotTime;
        }

        /** @return when the collector was created */
        public Instant getCacheCreatedAt() {
            return cacheCreatedAt;
        }

        /** @return total hits */
        public long getHits() {
            return hits;
        }

        /** @return total misses */
        public long getMisses() {
            return misses;
        }

        /** @return hits plus misses */
        public long getTotalGets() {
            return hits + misses;
        }

        /** @return total puts */
        public long getTotalPuts() {
            return totalPuts;
        }

        /** @return evictions made by the policy */
        public long getPolicyEvictions() {
            return policyEvictions;
        }

        /** @return explicit removals */
        public long getManualEvictions() {
            return manualEvictions;
        }

        /** @return policy and manual evictions combined */
        public long getTotalEvictions() {
            return policyEvictions + manualEvictions;
        }

        /** @return hit rate between 0 and 1 */
        public double getHitRate() {
            long lookups = hits + misses;
            return lookups == 0 ? 0.0 : (double) hits / lookups;
        }

        /** @return policy evictions per put, between 0 and 1 */
        public double getEvictionRate() {
            return totalPuts == 0 ? 0.0 : (double) policyEvictions / totalPuts;
        }

        /** @return mean timed lookup latency in microseconds */
        public double getAvgLoadTimeMicros() {
            return avgLoadTimeMicros;
        }

        /** @return median lookup latency in microseconds */
        public double getP50LatencyMicros() {
            return p50LatencyMicros;
        }

        /** @return 99th percentile lookup latency in microseconds */
        public double getP99LatencyMicros() {
            return p99LatencyMicros;
        }

        /**
         * Formats the snapshot as {@code name:value} lines.
         *
         * @return the formatted metrics
         */
        public String toStatsString() {
            return String.format("cache:%s%nhits:%d%nmisses:%d%nhit_rate:%.2f%%%nputs:%d%n"
                            + "policy_evictions:%d%nmanual_evictions:%d%navg_load_us:%.3f%np50_us:%.3f%np99_us:%.3f",
                    cacheName, hits, misses, getHitRate() * 100, totalPuts, policyEvictions, manualEvictions,
                    avgLoadTimeMicros, p50LatencyMicros, p99LatencyMicros);
        }

        @Override
        public String toString() {
            return toStatsString();
        }
    }
}
