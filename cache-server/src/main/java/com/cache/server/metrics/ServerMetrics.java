package com.cache.server.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * ServerMetrics tracks operational statistics for the running CacheServer.
 *
 * WHAT WE MEASURE AND WHY:
 *
 *   THROUGHPUT COUNTERS (LongAdder):
 *     Total gets, puts, deletes, hits, misses since server start.
 *     These are monotonically increasing — they never decrease.
 *     Used to compute hit rate = hits / (hits + misses).
 *
 *   LATENCY (sliding window):
 *     The last WINDOW_SIZE operation durations in nanoseconds.
 *     From this window we compute p50 (median) and p99 (99th percentile).
 *     p50 tells you typical performance. p99 tells you worst-case for 99%
 *     of users. p99 is what SLAs are written around.
 *
 *   ACTIVE CONNECTIONS (AtomicLong):
 *     Current number of open TCP connections. Set by ConnectionManager.
 *     This is a gauge (can go up and down), not a counter.
 *
 * WHY LongAdder INSTEAD OF AtomicLong FOR COUNTERS?
 *
 *   AtomicLong.incrementAndGet() uses a CAS (compare-and-swap) loop.
 *   Under high contention (many threads incrementing simultaneously),
 *   CAS loops spin-retry, wasting CPU cycles.
 *
 *   LongAdder maintains a Cell array — each thread updates its own cell,
 *   and sum() adds all cells. This eliminates CAS contention almost entirely.
 *   At 32 threads doing 1M ops/sec, LongAdder is ~3-5x faster than AtomicLong
 *   for pure increment workloads.
 *
 *   Tradeoff: sum() is slightly more expensive than AtomicLong.get().
 *   For metrics (read rarely, write constantly), LongAdder is the right choice.
 *
 * WHY ConcurrentLinkedQueue FOR THE LATENCY WINDOW?
 *
 *   We need concurrent writes from multiple Netty worker threads and
 *   occasional reads for STATS commands. ConcurrentLinkedQueue is:
 *   - Lock-free for offer() (writes) — ideal for the hot path
 *   - Weakly consistent for iteration (reads) — fine for approximate metrics
 *   - Unbounded by default, so we manually cap it at WINDOW_SIZE
 *
 *   Alternative: LongAdder-based histogram (pre-allocated buckets for each
 *   latency range). More accurate for p99 but more complex. For a cache server,
 *   ConcurrentLinkedQueue gives good enough accuracy.
 *
 * THREAD SAFETY:
 *   All public methods are safe to call from multiple Netty worker threads
 *   simultaneously. No external synchronization required.
 */
public class ServerMetrics {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /**
     * Number of recent operation latencies kept in the sliding window.
     * 1000 samples gives p99 as the 990th value when sorted.
     * Large enough for statistical accuracy, small enough for O(N log N)
     * sort in snapshot() to be fast (1000 elements ≈ microseconds).
     */
    private static final int WINDOW_SIZE = 1000;

    /**
     * Nanoseconds in one millisecond. Used to convert recorded latencies
     * from nanoseconds to milliseconds for human-readable output.
     */
    private static final double NANOS_PER_MS = 1_000_000.0;

    // -------------------------------------------------------------------------
    // Throughput counters — LongAdder for low contention under high concurrency
    // -------------------------------------------------------------------------

    /** Total GET commands received since server start. */
    private final LongAdder totalGets    = new LongAdder();

    /** Total PUT commands received since server start. */
    private final LongAdder totalPuts    = new LongAdder();

    /** Total DELETE commands received since server start. */
    private final LongAdder totalDeletes = new LongAdder();

    /**
     * GET operations where cache.get() returned a non-null value.
     * hit rate = hits / (hits + misses)
     */
    private final LongAdder hits   = new LongAdder();

    /**
     * GET operations where cache.get() returned null (key not found or expired).
     */
    private final LongAdder misses = new LongAdder();

    /**
     * Commands that caused a parse error or unknown command response.
     * High error count signals a misbehaving client or protocol mismatch.
     */
    private final LongAdder errors = new LongAdder();

    // -------------------------------------------------------------------------
    // Connection gauge — AtomicLong because it goes up AND down
    // -------------------------------------------------------------------------

    /**
     * Current number of active TCP connections.
     * Incremented by ConnectionManager.channelActive().
     * Decremented by ConnectionManager.channelInactive().
     *
     * AtomicLong (not LongAdder) because we need both increment and decrement,
     * and we need the current value to be readable accurately at any time.
     * LongAdder doesn't support decrement.
     */
    private final AtomicLong activeConnections = new AtomicLong(0);

    /**
     * Total connections accepted since server start.
     * Monotonically increasing — never decremented.
     */
    private final LongAdder totalConnections = new LongAdder();

    // -------------------------------------------------------------------------
    // Latency sliding window
    // -------------------------------------------------------------------------

    /**
     * Circular sliding window of recent operation latencies in nanoseconds.
     *
     * We use ConcurrentLinkedQueue (unbounded, lock-free) and manually
     * cap it at WINDOW_SIZE by polling the head when we add a new tail.
     *
     * This gives O(1) amortized offer() — exactly what we need on the hot path
     * where every cache operation records its latency.
     *
     * The window holds Long objects (autoboxed from long). At 1000 entries,
     * this is 1000 × (16 bytes object header + 8 bytes value) ≈ 24KB heap.
     * Negligible.
     */
    private final ConcurrentLinkedQueue<Long> latencyWindowNs =
            new ConcurrentLinkedQueue<>();

    /**
     * Server start time in milliseconds (System.currentTimeMillis()).
     * Used to compute uptime in snapshot().
     */
    private final long startTimeMs = System.currentTimeMillis();

    // -------------------------------------------------------------------------
    // Recording methods — called from CacheServerHandler on every operation
    // -------------------------------------------------------------------------

    /**
     * Records a completed GET operation.
     *
     * Call this AFTER cache.get() returns so you know whether it was a hit or miss.
     *
     * @param hit          true if cache.get() returned a non-null value
     * @param durationNs   wall-clock duration of the operation in nanoseconds
     *                     (System.nanoTime() after - System.nanoTime() before)
     */
    public void recordGet(boolean hit, long durationNs) {
        totalGets.increment();
        if (hit) hits.increment();
        else     misses.increment();
        recordLatency(durationNs);
    }

    /**
     * Records a completed PUT operation.
     *
     * @param durationNs Wall-clock duration of cache.put() in nanoseconds.
     */
    public void recordPut(long durationNs) {
        totalPuts.increment();
        recordLatency(durationNs);
    }

    /**
     * Records a completed DELETE operation.
     *
     * @param durationNs Wall-clock duration of cache.evict() in nanoseconds.
     */
    public void recordDelete(long durationNs) {
        totalDeletes.increment();
        recordLatency(durationNs);
    }

    /**
     * Records a command that resulted in an error response (-ERR ...).
     * Does NOT record a latency sample — error paths are anomalous
     * and would skew p99 if included.
     */
    public void recordError() {
        errors.increment();
    }

    /**
     * Called by ConnectionManager when a new connection is accepted.
     */
    public void connectionOpened() {
        activeConnections.incrementAndGet();
        totalConnections.increment();
    }

    /**
     * Called by ConnectionManager when a connection is closed or dropped.
     */
    public void connectionClosed() {
        // Guard against going negative (shouldn't happen, but defensive coding)
        activeConnections.updateAndGet(current -> Math.max(0, current - 1));
    }

    // -------------------------------------------------------------------------
    // Snapshot — called when a client sends STATS
    // -------------------------------------------------------------------------

    /**
     * Computes and returns a point-in-time snapshot of all metrics.
     *
     * This method does the expensive work (sorting the latency window for
     * percentile computation) only when explicitly requested — not on every
     * operation. This is the "compute on read" pattern for expensive aggregations.
     *
     * @return An immutable MetricsSnapshot with all current values.
     */
    public MetricsSnapshot snapshot() {
        long totalGetsVal    = totalGets.sum();
        long totalPutsVal    = totalPuts.sum();
        long totalDeletesVal = totalDeletes.sum();
        long hitsVal         = hits.sum();
        long missesVal       = misses.sum();
        long errorsVal       = errors.sum();
        long activeConns     = activeConnections.get();
        long totalConns      = totalConnections.sum();
        long uptimeMs        = System.currentTimeMillis() - startTimeMs;

        // Compute hit rate — guard against division by zero when no GETs yet
        double hitRate = 0.0;
        if (hitsVal + missesVal > 0) {
            hitRate = (double) hitsVal / (hitsVal + missesVal) * 100.0;
        }

        // Compute ops per second using uptime
        double opsPerSecond = 0.0;
        if (uptimeMs > 0) {
            long totalOps = totalGetsVal + totalPutsVal + totalDeletesVal;
            opsPerSecond = totalOps / (uptimeMs / 1000.0);
        }

        // Compute latency percentiles from the sliding window
        LatencyStats latency = computeLatencyStats();

        return new MetricsSnapshot(
                totalGetsVal, totalPutsVal, totalDeletesVal,
                hitsVal, missesVal, errorsVal,
                hitRate, opsPerSecond,
                activeConns, totalConns,
                uptimeMs,
                latency.p50Ms, latency.p99Ms, latency.meanMs
        );
    }

    /**
     * Returns a formatted string suitable for sending as a STATS response.
     * Format matches what CacheServerHandler writes to the client.
     *
     * Example output:
     *   hits:1420 misses:380 gets:1800 puts:200 deletes:50 errors:0
     *   hitRate:78.89% opsPerSec:4250.0 activeConn:3 uptime:42000ms
     *   p50:0.08ms p99:0.31ms mean:0.09ms
     */
    public String toStatsString() {
        MetricsSnapshot s = snapshot();
        return String.format(
                "hits:%d misses:%d gets:%d puts:%d deletes:%d errors:%d " +
                        "hitRate:%.2f%% opsPerSec:%.1f activeConn:%d totalConn:%d " +
                        "uptime:%dms p50:%.3fms p99:%.3fms mean:%.3fms",
                s.hits, s.misses, s.totalGets, s.totalPuts, s.totalDeletes, s.errors,
                s.hitRate, s.opsPerSecond, s.activeConnections, s.totalConnections,
                s.uptimeMs, s.p50Ms, s.p99Ms, s.meanMs
        );
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Adds a latency sample to the sliding window.
     *
     * Thread safety: ConcurrentLinkedQueue.offer() is lock-free.
     * The size check + poll() is NOT atomic — two threads might both see
     * size > WINDOW_SIZE and both poll(). This is acceptable: the window
     * might briefly shrink by 2 instead of 1. For metrics, this approximation
     * is fine. We never lose data — we just occasionally trim slightly more.
     *
     * @param durationNs Operation duration in nanoseconds.
     */
    private void recordLatency(long durationNs) {
        latencyWindowNs.offer(durationNs);

        // Trim the window if it exceeds WINDOW_SIZE.
        // poll() removes from the head (oldest sample).
        // This gives O(1) amortized maintenance of a fixed-size window.
        if (latencyWindowNs.size() > WINDOW_SIZE) {
            latencyWindowNs.poll();
        }
    }

    /**
     * Computes p50, p99, and mean from the current latency window.
     *
     * Algorithm:
     *   1. Drain the queue into a list (snapshot the current window).
     *   2. Sort the list.
     *   3. Index into sorted list for percentiles.
     *
     * Time complexity: O(N log N) where N = WINDOW_SIZE = 1000.
     * At 1000 elements, sort takes ~microseconds — fast enough for STATS responses.
     *
     * WHY DRAIN TO A LIST INSTEAD OF SORTING IN PLACE?
     * ConcurrentLinkedQueue's iterator is weakly consistent — elements added
     * during iteration may or may not be included. By draining to a list first,
     * we get a stable snapshot. The queue continues receiving updates while
     * we sort the snapshot.
     *
     * @return LatencyStats with p50, p99, and mean in milliseconds.
     */
    private LatencyStats computeLatencyStats() {
        List<Long> samples = new ArrayList<>(latencyWindowNs);

        if (samples.isEmpty()) {
            return new LatencyStats(0.0, 0.0, 0.0);
        }

        Collections.sort(samples);

        // p50: value at 50th percentile index
        int p50Index = (int) (samples.size() * 0.50);
        int p99Index = (int) (samples.size() * 0.99);

        // Clamp indices to valid range
        p50Index = Math.min(p50Index, samples.size() - 1);
        p99Index = Math.min(p99Index, samples.size() - 1);

        double p50Ms = samples.get(p50Index) / NANOS_PER_MS;
        double p99Ms = samples.get(p99Index) / NANOS_PER_MS;

        // Compute mean — sum all samples and divide
        long sum = 0;
        for (long sample : samples) {
            sum += sample;
        }
        double meanMs = (sum / (double) samples.size()) / NANOS_PER_MS;

        return new LatencyStats(p50Ms, p99Ms, meanMs);
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    /**
     * Temporary holder for latency percentile values during snapshot computation.
     * Private — not exposed to callers. MetricsSnapshot is the public result type.
     */
    private static final class LatencyStats {
        final double p50Ms;
        final double p99Ms;
        final double meanMs;

        LatencyStats(double p50Ms, double p99Ms, double meanMs) {
            this.p50Ms  = p50Ms;
            this.p99Ms  = p99Ms;
            this.meanMs = meanMs;
        }
    }

    /**
     * Immutable snapshot of all server metrics at a point in time.
     *
     * Immutable because:
     *   1. It's safe to pass between threads without synchronization.
     *   2. It represents a consistent moment in time — values shouldn't
     *      change after the snapshot is taken.
     *   3. Tests can assert on it without the underlying counters changing.
     *
     * All fields are public final — no getters needed for a pure value object.
     */
    public static final class MetricsSnapshot {

        public final long   totalGets;
        public final long   totalPuts;
        public final long   totalDeletes;
        public final long   hits;
        public final long   misses;
        public final long   errors;
        public final double hitRate;           // percentage: 0.0 to 100.0
        public final double opsPerSecond;
        public final long   activeConnections;
        public final long   totalConnections;
        public final long   uptimeMs;
        public final double p50Ms;             // median latency in milliseconds
        public final double p99Ms;             // 99th percentile latency
        public final double meanMs;            // average latency

        private MetricsSnapshot(
                long totalGets, long totalPuts, long totalDeletes,
                long hits, long misses, long errors,
                double hitRate, double opsPerSecond,
                long activeConnections, long totalConnections,
                long uptimeMs,
                double p50Ms, double p99Ms, double meanMs
        ) {
            this.totalGets        = totalGets;
            this.totalPuts        = totalPuts;
            this.totalDeletes     = totalDeletes;
            this.hits             = hits;
            this.misses           = misses;
            this.errors           = errors;
            this.hitRate          = hitRate;
            this.opsPerSecond     = opsPerSecond;
            this.activeConnections = activeConnections;
            this.totalConnections = totalConnections;
            this.uptimeMs         = uptimeMs;
            this.p50Ms            = p50Ms;
            this.p99Ms            = p99Ms;
            this.meanMs           = meanMs;
        }

        @Override
        public String toString() {
            return String.format(
                    "MetricsSnapshot{gets=%d, puts=%d, hits=%d, misses=%d, " +
                            "hitRate=%.2f%%, opsPerSec=%.1f, p50=%.3fms, p99=%.3fms}",
                    totalGets, totalPuts, hits, misses, hitRate, opsPerSecond, p50Ms, p99Ms
            );
        }
    }
}