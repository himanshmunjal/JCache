package com.cache.bench;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Random;

/**
 * BenchmarkWorkload generates and holds pre-computed key arrays
 * for use across all benchmark classes.
 *
 * WHY PRE-COMPUTE KEYS?
 * If we generated keys inside the @Benchmark method itself, we'd be
 * measuring key generation + cache operation together. JMH measures
 * wall time, so String.format() or Random.nextInt() inside the hot loop
 * would pollute our cache measurements. Pre-computing in @Setup means
 * the benchmark loop does ONLY the cache operation.
 *
 * WHY AN ARRAY INSTEAD OF GENERATING ON THE FLY?
 * Arrays are CPU-cache friendly (sequential memory layout). A pre-built
 * String[] accessed with index++ is faster than generating random strings
 * in the hot loop. We simulate randomness by shuffling/cycling the array.
 *
 * TWO WORKLOAD DISTRIBUTIONS:
 *
 *   UNIFORM — every key accessed with equal probability.
 *   Models: randomized load balancers, synthetic test traffic.
 *   Reality: almost no real system has this. Every key is equally "hot".
 *   Cache behavior: all eviction policies perform similarly.
 *
 *   ZIPFIAN — a small set of keys dominates access frequency.
 *   Models: web traffic (top pages), databases (hot rows), CDN (viral content).
 *   Formula: P(rank r) ∝ 1 / r^skew
 *   With skew=1.0: rank 1 accessed ~ln(N) times more than rank N.
 *   Cache behavior: LFU and ARC outperform LRU significantly.
 *   This is the distribution that makes eviction policy choice matter.
 *
 * SCOPE.BENCHMARK means one shared BenchmarkWorkload instance is created
 * per benchmark fork, shared across all threads. This is correct here
 * because the workload arrays are read-only after @Setup.
 */
@State(Scope.Benchmark)
public class BenchmarkWorkload {

    // -------------------------------------------------------------------------
    // Configuration constants
    // -------------------------------------------------------------------------

    /**
     * Total number of unique keys in the key space.
     * 1000 keys with a cache capacity of 256 gives a ~74% miss rate on uniform,
     * and a much lower miss rate on Zipfian (hot keys stay cached).
     */
    public static final int KEY_SPACE_SIZE = 1000;

    /**
     * Number of pre-generated operations in each workload array.
     * JMH will cycle through this array in the benchmark loop.
     * Large enough to avoid pattern repetition bias, small enough to fit in L3 cache.
     */
    public static final int WORKLOAD_SIZE = 100_000;

    /**
     * Cache capacity used in all benchmarks.
     * 256 / 1000 = 25.6% of key space fits in cache.
     * This ratio creates meaningful cache pressure — not too easy, not too hard.
     */
    public static final int CACHE_CAPACITY = 256;

    /**
     * Zipfian skew parameter. 1.0 is standard Zipfian.
     * Higher values = more skewed (top key gets even more traffic).
     * Lower values toward 0.0 approach uniform distribution.
     */
    private static final double ZIPFIAN_SKEW = 1.0;

    // -------------------------------------------------------------------------
    // Pre-computed workload arrays
    // -------------------------------------------------------------------------

    /**
     * Pre-built array of keys following Zipfian distribution.
     * Index i contains the key to access on operation i.
     * Read-only after @Setup — safe to share across threads.
     */
    public String[] zipfianKeys;

    /**
     * Pre-built array of keys following Uniform distribution.
     * Each key from the key space appears with equal probability.
     */
    public String[] uniformKeys;

    /**
     * Sequential counter used to index into workload arrays.
     * Each benchmark method increments this and takes modulo WORKLOAD_SIZE.
     *
     * NOTE: In @State(Scope.Benchmark), this counter is SHARED across threads.
     * For single-threaded benchmarks this is fine. For multi-threaded benchmarks,
     * use @State(Scope.Thread) in the subclass and override the counter there.
     */
    public int operationIndex = 0;

    // -------------------------------------------------------------------------
    // Pre-computed key string pool
    // -------------------------------------------------------------------------

    /**
     * All possible keys as pre-built String objects.
     * Format: "key-0", "key-1", ..., "key-999"
     *
     * Pre-building prevents String.format() allocation in the hot loop.
     * String[] lookup by index is O(1) and GC-free.
     */
    private String[] keyPool;

    // -------------------------------------------------------------------------
    // Setup
    // -------------------------------------------------------------------------

    /**
     * Called once before any benchmark methods run in this fork.
     * Builds all workload arrays. Must complete before @Benchmark methods execute.
     *
     * @Setup(Level.Trial) = runs once per fork (not per iteration).
     * This is correct for workload generation — we want the same keys
     * across all measurement iterations for consistency.
     */
    @Setup
    public void setup() {
        buildKeyPool();
        buildZipfianWorkload();
        buildUniformWorkload();
    }

    // -------------------------------------------------------------------------
    // Public workload accessors
    // -------------------------------------------------------------------------

    /**
     * Returns the next key from the Zipfian workload, cycling back to start.
     * Call this from @Benchmark methods for Zipfian-distributed access.
     *
     * @return A key string from the Zipfian distribution.
     */
    public String nextZipfianKey() {
        return zipfianKeys[operationIndex++ % WORKLOAD_SIZE];
    }

    /**
     * Returns the next key from the Uniform workload, cycling back to start.
     * Call this from @Benchmark methods for uniform-distributed access.
     *
     * @return A key string from the Uniform distribution.
     */
    public String nextUniformKey() {
        return uniformKeys[operationIndex++ % WORKLOAD_SIZE];
    }

    /**
     * Returns a value string for a given key.
     * Deterministic: same key always returns same value.
     * Pre-fixed "value-" prefix makes it identifiable in debug output.
     *
     * @param key The cache key.
     * @return A value string.
     */
    public String valueFor(String key) {
        // Simple: value mirrors key with different prefix.
        // This is deterministic and allocation-free at benchmark time
        // because we could also pre-build this — but values aren't
        // on the hot path (put() is less frequent than get() in our benchmarks).
        return "value-" + key.substring(4); // strip "key-" prefix
    }

    // -------------------------------------------------------------------------
    // Private workload builders
    // -------------------------------------------------------------------------

    /**
     * Builds the key pool: ["key-0", "key-1", ..., "key-999"].
     * All workload arrays reference strings from this pool — no duplicate objects.
     */
    private void buildKeyPool() {
        keyPool = new String[KEY_SPACE_SIZE];
        for (int i = 0; i < KEY_SPACE_SIZE; i++) {
            keyPool[i] = "key-" + i;
        }
    }

    /**
     * Builds the Zipfian workload array.
     *
     * Algorithm — Rejection Sampling Zipfian:
     *   1. Compute the harmonic sum H = sum(1/i^skew) for i=1..N
     *      This is the normalizing constant for the Zipfian distribution.
     *   2. For each slot in the workload array:
     *      a. Draw a uniform random u in [0, 1)
     *      b. Find rank r such that CDF(r) >= u
     *         (CDF(r) = sum(1/i^skew for i=1..r) / H)
     *      c. That rank r maps to keyPool[r-1]
     *
     * The result: rank 0 appears ~ln(1000) ≈ 7x more than rank 999.
     * Top 10% of keys (100 keys) will cover ~73% of accesses.
     *
     * This matches the access patterns measured in real web traffic studies
     * (e.g., Breslau et al. "Web caching and Zipf-like distributions").
     */
    private void buildZipfianWorkload() {
        zipfianKeys = new String[WORKLOAD_SIZE];
        Random rng = new Random(42); // fixed seed for reproducibility

        // Step 1: Compute harmonic sum (normalizing constant)
        double harmonicSum = 0.0;
        for (int i = 1; i <= KEY_SPACE_SIZE; i++) {
            harmonicSum += 1.0 / Math.pow(i, ZIPFIAN_SKEW);
        }

        // Step 2: Pre-compute CDF array for fast rank lookup
        // cdf[r] = probability that a random draw falls within rank 0..r
        double[] cdf = new double[KEY_SPACE_SIZE];
        double cumulative = 0.0;
        for (int i = 0; i < KEY_SPACE_SIZE; i++) {
            cumulative += (1.0 / Math.pow(i + 1, ZIPFIAN_SKEW)) / harmonicSum;
            cdf[i] = cumulative;
        }

        // Step 3: Fill workload array using binary search on CDF
        for (int op = 0; op < WORKLOAD_SIZE; op++) {
            double u = rng.nextDouble();
            int rank = binarySearchCDF(cdf, u);
            zipfianKeys[op] = keyPool[rank];
        }
    }

    /**
     * Builds the Uniform workload array.
     * Each key from the key space has equal probability 1/KEY_SPACE_SIZE.
     * Simple: just take Random.nextInt(KEY_SPACE_SIZE) for each slot.
     */
    private void buildUniformWorkload() {
        uniformKeys = new String[WORKLOAD_SIZE];
        Random rng = new Random(42); // same seed as Zipfian for fair comparison

        for (int op = 0; op < WORKLOAD_SIZE; op++) {
            int rank = rng.nextInt(KEY_SPACE_SIZE);
            uniformKeys[op] = keyPool[rank];
        }
    }

    /**
     * Binary search on a CDF array to find the first index where cdf[i] >= target.
     * This is O(log N) per lookup, used only during @Setup (not in the hot path).
     *
     * @param cdf    Monotonically increasing array of probabilities in [0, 1].
     * @param target Uniform random value in [0, 1).
     * @return Index of the rank whose CDF first exceeds target.
     */
    private int binarySearchCDF(double[] cdf, double target) {
        int lo = 0, hi = cdf.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cdf[mid] < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    // -------------------------------------------------------------------------
    // Utility: pre-warm the cache with a baseline dataset
    // -------------------------------------------------------------------------

    /**
     * Pre-populates a cache with CACHE_CAPACITY entries so benchmarks
     * don't start from an empty (cold) cache. Real caches operate in
     * steady state — benchmarking cold start skews results.
     *
     * Call this from @Setup methods in benchmark classes after creating the cache.
     *
     * @param cache The cache to warm up.
     */
    public void warmCache(com.cache.api.Cache<String, String> cache) {
        // Insert one entry per unique rank in order — this creates a baseline
        // hot set matching the Zipfian distribution's top keys.
        for (int i = 0; i < CACHE_CAPACITY; i++) {
            cache.put(keyPool[i], "warmup-value-" + i);
        }
    }
}