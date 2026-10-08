package com.cache.bench;

import com.cache.api.Cache;

import java.util.Random;

/**
 * Pre-generated key sequences shared by the benchmarks, so that key
 * generation does not show up in the measurements.
 *
 * <p>Keys are drawn from {@value #KEY_SPACE_SIZE} distinct keys either
 * uniformly or with a Zipf distribution (s = 1.0), where a handful of keys
 * receive most of the traffic, as in most real caches. Sequences use a fixed
 * seed so runs are comparable.
 */
public final class BenchmarkWorkload {

    /** Number of distinct keys. */
    public static final int KEY_SPACE_SIZE = 1000;
    /** Length of each pre-generated sequence. */
    public static final int WORKLOAD_SIZE = 100_000;
    /** Cache capacity used by the benchmarks: about a quarter of the key space. */
    public static final int CACHE_CAPACITY = 256;

    private static final double ZIPF_SKEW = 1.0;

    private static final String[] KEYS = new String[KEY_SPACE_SIZE];
    private static final String[] ZIPF = new String[WORKLOAD_SIZE];
    private static final String[] UNIFORM = new String[WORKLOAD_SIZE];

    static {
        for (int i = 0; i < KEY_SPACE_SIZE; i++) {
            KEYS[i] = "key-" + i;
        }
        double[] cdf = zipfCdf();
        Random zipfRandom = new Random(42);
        Random uniformRandom = new Random(42);
        for (int i = 0; i < WORKLOAD_SIZE; i++) {
            ZIPF[i] = KEYS[search(cdf, zipfRandom.nextDouble())];
            UNIFORM[i] = KEYS[uniformRandom.nextInt(KEY_SPACE_SIZE)];
        }
    }

    private BenchmarkWorkload() {
    }

    /**
     * @param i any non-negative index; wraps around the sequence
     * @return the i-th key of the Zipf sequence
     */
    public static String zipf(long i) {
        return ZIPF[(int) (i % WORKLOAD_SIZE)];
    }

    /**
     * @param i any non-negative index; wraps around the sequence
     * @return the i-th key of the uniform sequence
     */
    public static String uniform(long i) {
        return UNIFORM[(int) (i % WORKLOAD_SIZE)];
    }

    /**
     * @param i any non-negative index
     * @return the i-th key in ascending key order, for sequential scans
     */
    public static String sequential(long i) {
        return KEYS[(int) (i % KEY_SPACE_SIZE)];
    }

    /**
     * @param key a key
     * @return the value stored for {@code key}
     */
    public static String valueFor(String key) {
        return "value-" + key;
    }

    /**
     * Fills a cache to capacity with the first keys of the key space.
     *
     * @param cache the cache to fill
     */
    public static void warm(Cache<String, String> cache) {
        for (int i = 0; i < CACHE_CAPACITY; i++) {
            cache.put(KEYS[i], valueFor(KEYS[i]));
        }
    }

    private static double[] zipfCdf() {
        double norm = 0;
        for (int i = 1; i <= KEY_SPACE_SIZE; i++) {
            norm += 1.0 / Math.pow(i, ZIPF_SKEW);
        }
        double[] cdf = new double[KEY_SPACE_SIZE];
        double sum = 0;
        for (int i = 0; i < KEY_SPACE_SIZE; i++) {
            sum += 1.0 / Math.pow(i + 1, ZIPF_SKEW) / norm;
            cdf[i] = sum;
        }
        return cdf;
    }

    private static int search(double[] cdf, double u) {
        int lo = 0;
        int hi = cdf.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cdf[mid] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
