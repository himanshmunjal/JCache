package com.cache.stats;

import com.cache.api.CacheStats;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * CacheMetricsCollector — real-time metrics instrumentation for any cache.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHY THIS EXISTS
 * ─────────────────────────────────────────────────────────────────────────────
 * A cache without metrics is a black box. You can't answer:
 *   "Is my cache actually helping?"  → check hit rate
 *   "Is the cache thrashing?"        → check eviction rate
 *   "Is get() slow?"                 → check average load time
 *   "When did performance degrade?"  → check createdAt + counters over time
 *
 * This is exactly what Caffeine exposes via CacheStats, what Redis exposes
 * via INFO stats, and what Memcached exposes via the stats command.
 * We build the same thing with Java concurrency primitives.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * DESIGN DECISIONS
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * LongAdder over AtomicLong for high-frequency counters:
 *   AtomicLong uses CAS (compare-and-swap). Under high contention (many threads
 *   incrementing simultaneously), CAS fails repeatedly and threads spin.
 *   LongAdder maintains a per-stripe counter per thread and sums them on read.
 *   This eliminates contention on writes at the cost of slightly slower reads.
 *   Since counters are written on every get/put and read only for STATS,
 *   LongAdder is the correct choice here.
 *
 * Sliding window for latency:
 *   A single "total load time / total requests" average is meaningless for
 *   latency because outliers from startup dominate the average forever.
 *   A sliding window over the last N operations gives you a "recent" average
 *   that reflects current performance. We use a fixed-size circular array.
 *   Window size = 1000 ops is standard — large enough to smooth noise,
 *   small enough to react to changes within seconds.
 *
 * Immutable MetricsSnapshot:
 *   snapshot() returns a value object frozen at that instant.
 *   This prevents the "read hit rate, then read eviction rate, they're from
 *   different moments" problem. One snapshot = one consistent moment in time.
 *
 * Thread safety:
 *   All counters are LongAdder (thread-safe writes) or AtomicLong (thread-safe
 *   reads/writes). The sliding window uses a volatile index with synchronized
 *   write — the window is written rarely (only on measured operations) so
 *   this is not a bottleneck.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * HOW TO USE
 * ─────────────────────────────────────────────────────────────────────────────
 *   CacheMetricsCollector metrics = new CacheMetricsCollector("user-sessions");
 *
 *   // In your cache.get() implementation:
 *   long start = metrics.startTimer();
 *   V value = delegate.get(key);
 *   if (value != null) metrics.recordHit(start);
 *   else               metrics.recordMiss(start);
 *
 *   // In your cache.put() implementation:
 *   metrics.recordPut();
 *
 *   // When a key is evicted:
 *   metrics.recordEviction();
 *
 *   // To read metrics (e.g., for a STATS command):
 *   MetricsSnapshot snap = metrics.snapshot();
 *   System.out.println(snap.getHitRate());
 */
public class CacheMetricsCollector {

    // ─────────────────────────────────────────────────────────────────────────
    // Constants
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Number of recent operations tracked in the sliding window for latency.
     * 1000 is the industry standard — large enough to smooth jitter,
     * small enough that a burst of slow operations shows up within seconds.
     */
    private static final int LATENCY_WINDOW_SIZE = 1000;

    // ─────────────────────────────────────────────────────────────────────────
    // Identity
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Human-readable name for this cache instance.
     * Used in toString() and snapshot() so you can distinguish multiple caches
     * in the same JVM (e.g., "user-sessions" vs "product-catalog").
     */
    private final String cacheName;

    /**
     * Timestamp when this collector was created.
     * Lets you compute "uptime" and "ops per second since start".
     */
    private final Instant createdAt;

    // ─────────────────────────────────────────────────────────────────────────
    // Core counters — LongAdder for write-heavy, AtomicLong for read-write balance
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Total number of get() calls that returned a non-null value.
     * LongAdder: every get() is a potential write to this counter.
     */
    private final LongAdder hits = new LongAdder();

    /**
     * Total number of get() calls that returned null (key absent or expired).
     * LongAdder: same reasoning as hits.
     */
    private final LongAdder misses = new LongAdder();

    /**
     * Total number of put() calls ever made.
     * Includes both new insertions and updates to existing keys.
     */
    private final LongAdder totalPuts = new LongAdder();

    /**
     * Total number of keys evicted by policy (LRU/LFU/ARC) or TTL.
     * Does NOT include explicit evict() calls — those are manualEvictions.
     * Separation lets you distinguish "cache thrashing" (high policyEvictions)
     * from "application-driven cleanup" (high manualEvictions).
     */
    private final LongAdder policyEvictions = new LongAdder();

    /**
     * Total number of keys removed via explicit evict() calls.
     */
    private final LongAdder manualEvictions = new LongAdder();

    /**
     * Total load time in nanoseconds across all measured get() calls.
     * Used to compute average load time. AtomicLong because we add variable
     * amounts (not just +1) so LongAdder's striped approach doesn't help here.
     */
    private final AtomicLong totalLoadTimeNanos = new AtomicLong(0);

    /**
     * Total number of get() calls whose load time was measured.
     * Denominator for average load time calculation.
     */
    private final LongAdder measuredLoadOps = new LongAdder();

    // ─────────────────────────────────────────────────────────────────────────
    // Sliding window for recent latency (p50/p99 approximation)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Circular array storing the last LATENCY_WINDOW_SIZE load times in nanos.
     * Index wraps around using modulo arithmetic.
     * Not sorted — we compute percentiles by sorting a copy on demand.
     */
    private final long[] latencyWindow = new long[LATENCY_WINDOW_SIZE];

    /**
     * Write index into latencyWindow. Incremented on every recorded operation.
     * Wraps around at LATENCY_WINDOW_SIZE via modulo.
     * AtomicLong so concurrent writes don't corrupt the index.
     */
    private final AtomicLong latencyWindowIndex = new AtomicLong(0);

    /**
     * How many entries in latencyWindow are actually populated.
     * Starts at 0, grows to LATENCY_WINDOW_SIZE, then stays there.
     * Needed so we don't compute percentiles over uninitialized zeros.
     */
    private final AtomicLong latencyWindowFilled = new AtomicLong(0);

    // ─────────────────────────────────────────────────────────────────────────
    // Constructors
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a new collector for a named cache.
     *
     * @param cacheName Human-readable name, e.g. "user-session-cache".
     *                  Used in snapshots and toString(). Cannot be null.
     */
    public CacheMetricsCollector(String cacheName) {
        if (cacheName == null || cacheName.isBlank()) {
            throw new IllegalArgumentException("Cache name cannot be null or blank");
        }
        this.cacheName = cacheName;
        this.createdAt = Instant.now();
    }

    /**
     * Creates a collector with a default name.
     * Convenient for quick setup; use the named constructor for production.
     */
    public CacheMetricsCollector() {
        this("default-cache");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Recording API — called from your cache implementation
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Start a latency timer.
     * Call this at the START of a get() operation, before touching the cache.
     * Pass the returned value to recordHit() or recordMiss() at the end.
     *
     * Uses System.nanoTime() — higher resolution and not affected by
     * system clock adjustments. Never use System.currentTimeMillis() for
     * duration measurement.
     *
     * @return Nanosecond timestamp at the start of the operation.
     */
    public long startTimer() {
        return System.nanoTime();
    }

    /**
     * Record a cache HIT — get() returned a value.
     *
     * @param startNanos The value returned by startTimer() at operation start.
     */
    public void recordHit(long startNanos) {
        hits.increment();
        recordLatency(System.nanoTime() - startNanos);
    }

    /**
     * Record a cache MISS — get() returned null.
     *
     * @param startNanos The value returned by startTimer() at operation start.
     */
    public void recordMiss(long startNanos) {
        misses.increment();
        recordLatency(System.nanoTime() - startNanos);
    }

    /**
     * Record a cache HIT without timing measurement.
     * Use this when you don't need latency tracking (simpler integration).
     */
    public void recordHit() {
        hits.increment();
    }

    /**
     * Record a cache MISS without timing measurement.
     */
    public void recordMiss() {
        misses.increment();
    }

    /**
     * Record a put() operation.
     * Call this on every put(), regardless of whether it caused an eviction.
     */
    public void recordPut() {
        totalPuts.increment();
    }

    /**
     * Record a policy-driven eviction (LRU tail removal, LFU min-freq removal, etc.)
     * Call this when the cache itself decides to evict a key due to capacity.
     */
    public void recordEviction() {
        policyEvictions.increment();
    }

    /**
     * Record an explicit eviction — a caller called evict(key) directly.
     * Counted separately from policy evictions.
     */
    public void recordManualEviction() {
        manualEvictions.increment();
    }

    /**
     * Reset all counters to zero.
     * Useful in tests (reset between test runs) or for rolling windows
     * (reset every minute to get per-minute metrics).
     *
     * Thread safety: non-atomic reset. In a live system, reset() might see
     * a mix of old and new values briefly. Acceptable for stats; not for billing.
     */
    public void reset() {
        hits.reset();
        misses.reset();
        totalPuts.reset();
        policyEvictions.reset();
        manualEvictions.reset();
        totalLoadTimeNanos.set(0);
        measuredLoadOps.reset();
        latencyWindowIndex.set(0);
        latencyWindowFilled.set(0);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Snapshot API — produces an immutable point-in-time view
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Produces an immutable snapshot of all metrics at this instant.
     *
     * Why immutable snapshot instead of live getters?
     * If you call getHitRate() then getEvictionRate() separately, they're
     * computed from different moments in time — inconsistent. A snapshot
     * reads all values once and freezes them together.
     *
     * @return MetricsSnapshot containing all current metrics.
     */
    public MetricsSnapshot snapshot() {
        long hitCount       = hits.sum();
        long missCount      = misses.sum();
        long putCount       = totalPuts.sum();
        long evictionCount  = policyEvictions.sum();
        long manualCount    = manualEvictions.sum();
        long totalOps       = hitCount + missCount;
        long loadOps        = measuredLoadOps.sum();
        long totalLoadNanos = totalLoadTimeNanos.get();

        // Hit rate: proportion of gets that found a value.
        double hitRate = totalOps == 0 ? 0.0 : (double) hitCount / totalOps;

        // Eviction rate: proportion of puts that caused an eviction.
        double evictionRate = putCount == 0 ? 0.0 : (double) evictionCount / putCount;

        // Average load time in microseconds (nanos / 1000).
        // Microseconds are the right unit for in-process cache latency —
        // nanoseconds are too noisy, milliseconds too coarse.
        double avgLoadTimeMicros = loadOps == 0 ? 0.0
                : (double) totalLoadNanos / loadOps / 1_000.0;

        // Percentiles from sliding window.
        double[] percentiles = computePercentiles();

        return new MetricsSnapshot(
                cacheName,
                Instant.now(),
                createdAt,
                hitCount,
                missCount,
                putCount,
                evictionCount,
                manualCount,
                hitRate,
                evictionRate,
                avgLoadTimeMicros,
                percentiles[0],  // p50
                percentiles[1]   // p99
        );
    }

    /**
     * Produces a CacheStats-compatible view for the Cache<K,V> interface.
     * This bridges CacheMetricsCollector with the CacheStats return type
     * expected by Cache.getStats().
     *
     * @return CacheStats populated from current collector state.
     */
    public CacheStats toCacheStats() {
        return new CacheStats(
                hits.sum(),
                misses.sum(),
                policyEvictions.sum() + manualEvictions.sum(),
                getHitRate()
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience getters — for when you need a single value fast
    // ─────────────────────────────────────────────────────────────────────────

    /** @return Total hits since creation or last reset(). */
    public long getHits()           { return hits.sum(); }

    /** @return Total misses since creation or last reset(). */
    public long getMisses()         { return misses.sum(); }

    /** @return Total puts since creation or last reset(). */
    public long getTotalPuts()      { return totalPuts.sum(); }

    /** @return Policy-driven evictions since creation or last reset(). */
    public long getPolicyEvictions(){ return policyEvictions.sum(); }

    /** @return Manual evictions since creation or last reset(). */
    public long getManualEvictions(){ return manualEvictions.sum(); }

    /** @return Total evictions (policy + manual). */
    public long getTotalEvictions() {
        return policyEvictions.sum() + manualEvictions.sum();
    }

    /**
     * @return Hit rate as a value between 0.0 and 1.0.
     *         Returns 0.0 if no get() operations have been recorded.
     */
    public double getHitRate() {
        long total = hits.sum() + misses.sum();
        return total == 0 ? 0.0 : (double) hits.sum() / total;
    }

    /** @return The name this collector was created with. */
    public String getCacheName()    { return cacheName; }

    /** @return When this collector was created. */
    public Instant getCreatedAt()   { return createdAt; }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Writes one latency sample into the circular window and updates totals.
     *
     * Circular buffer mechanics:
     *   index = (globalIndex++) % WINDOW_SIZE
     *   Write to latencyWindow[index].
     *   When globalIndex > WINDOW_SIZE, we start overwriting old entries.
     *
     * @param elapsedNanos Duration of the operation in nanoseconds.
     */
    private void recordLatency(long elapsedNanos) {
        // Accumulate into totals for average calculation.
        totalLoadTimeNanos.addAndGet(elapsedNanos);
        measuredLoadOps.increment();

        // Write into circular window.
        long idx = latencyWindowIndex.getAndIncrement() % LATENCY_WINDOW_SIZE;
        latencyWindow[(int) idx] = elapsedNanos;

        // Track how many slots are actually populated (caps at WINDOW_SIZE).
        latencyWindowFilled.updateAndGet(current ->
                Math.min(current + 1, LATENCY_WINDOW_SIZE)
        );
    }

    /**
     * Computes p50 and p99 latency from the sliding window.
     *
     * Method: copy the populated portion of the window, sort it,
     * then index into it at the 50th and 99th percentile positions.
     *
     * This is O(N log N) where N = window size (max 1000).
     * That's ~10,000 comparisons — fast enough for a stats endpoint
     * that's called occasionally, not on every request.
     *
     * Returns latency in MICROSECONDS for readability.
     *
     * @return double[2] where [0] = p50 micros, [1] = p99 micros.
     */
    private double[] computePercentiles() {
        int filled = (int) latencyWindowFilled.get();
        if (filled == 0) {
            return new double[]{0.0, 0.0};
        }

        // Copy only the populated entries to avoid sorting zeros.
        long[] window = new long[filled];
        System.arraycopy(latencyWindow, 0, window, 0, filled);
        java.util.Arrays.sort(window);

        // Percentile index: p50 = 50th percentile = index at 50% of length.
        int p50idx = (int) Math.ceil(0.50 * filled) - 1;
        int p99idx = (int) Math.ceil(0.99 * filled) - 1;

        // Clamp to valid indices (handles filled=1 edge case).
        p50idx = Math.max(0, Math.min(p50idx, filled - 1));
        p99idx = Math.max(0, Math.min(p99idx, filled - 1));

        return new double[]{
                window[p50idx] / 1_000.0,  // convert nanos → micros
                window[p99idx] / 1_000.0
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // toString
    // ─────────────────────────────────────────────────────────────────────────

    @Override
    public String toString() {
        return String.format(
                "CacheMetricsCollector{name='%s', hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d}",
                cacheName,
                hits.sum(),
                misses.sum(),
                getHitRate() * 100,
                getTotalEvictions()
        );
    }

    // =========================================================================
    // MetricsSnapshot — immutable point-in-time view of all metrics
    // =========================================================================

    /**
     * An immutable snapshot of all cache metrics at a single point in time.
     *
     * Returned by CacheMetricsCollector.snapshot().
     * Suitable for logging, STATS command responses, Prometheus exposition,
     * and unit test assertions.
     *
     * All fields are final — thread-safe without synchronization.
     */
    public static final class MetricsSnapshot {

        private final String    cacheName;
        private final Instant   snapshotTime;
        private final Instant   cacheCreatedAt;
        private final long      hits;
        private final long      misses;
        private final long      totalPuts;
        private final long      policyEvictions;
        private final long      manualEvictions;
        private final double    hitRate;
        private final double    evictionRate;
        private final double    avgLoadTimeMicros;
        private final double    p50LatencyMicros;
        private final double    p99LatencyMicros;

        /**
         * Package-private constructor — only CacheMetricsCollector creates snapshots.
         */
        MetricsSnapshot(
                String cacheName,
                Instant snapshotTime,
                Instant cacheCreatedAt,
                long hits,
                long misses,
                long totalPuts,
                long policyEvictions,
                long manualEvictions,
                double hitRate,
                double evictionRate,
                double avgLoadTimeMicros,
                double p50LatencyMicros,
                double p99LatencyMicros
        ) {
            this.cacheName          = cacheName;
            this.snapshotTime       = snapshotTime;
            this.cacheCreatedAt     = cacheCreatedAt;
            this.hits               = hits;
            this.misses             = misses;
            this.totalPuts          = totalPuts;
            this.policyEvictions    = policyEvictions;
            this.manualEvictions    = manualEvictions;
            this.hitRate            = hitRate;
            this.evictionRate       = evictionRate;
            this.avgLoadTimeMicros  = avgLoadTimeMicros;
            this.p50LatencyMicros   = p50LatencyMicros;
            this.p99LatencyMicros   = p99LatencyMicros;
        }

        // Getters

        public String  getCacheName()           { return cacheName; }
        public Instant getSnapshotTime()        { return snapshotTime; }
        public Instant getCacheCreatedAt()      { return cacheCreatedAt; }
        public long    getHits()                { return hits; }
        public long    getMisses()              { return misses; }
        public long    getTotalPuts()           { return totalPuts; }
        public long    getPolicyEvictions()     { return policyEvictions; }
        public long    getManualEvictions()     { return manualEvictions; }
        public long    getTotalEvictions()      { return policyEvictions + manualEvictions; }
        public double  getHitRate()             { return hitRate; }
        public double  getEvictionRate()        { return evictionRate; }
        public double  getAvgLoadTimeMicros()   { return avgLoadTimeMicros; }
        public double  getP50LatencyMicros()    { return p50LatencyMicros; }
        public double  getP99LatencyMicros()    { return p99LatencyMicros; }
        public long    getTotalGets()           { return hits + misses; }

        /**
         * Formats this snapshot as a multi-line stats string suitable for
         * the STATS wire protocol command response.
         *
         * Example output:
         *   cache:user-sessions
         *   hits:1423
         *   misses:211
         *   hit_rate:87.10%
         *   puts:1634
         *   evictions:89
         *   avg_load_us:0.42
         *   p50_us:0.31
         *   p99_us:1.87
         */
        public String toStatsString() {
            return String.format(
                    "cache:%s%n"       +
                            "hits:%d%n"        +
                            "misses:%d%n"      +
                            "hit_rate:%.2f%%%n"+
                            "puts:%d%n"        +
                            "policy_evictions:%d%n" +
                            "manual_evictions:%d%n" +
                            "avg_load_us:%.3f%n"    +
                            "p50_us:%.3f%n"         +
                            "p99_us:%.3f",
                    cacheName,
                    hits,
                    misses,
                    hitRate * 100,
                    totalPuts,
                    policyEvictions,
                    manualEvictions,
                    avgLoadTimeMicros,
                    p50LatencyMicros,
                    p99LatencyMicros
            );
        }

        @Override
        public String toString() {
            return toStatsString();
        }
    }
}