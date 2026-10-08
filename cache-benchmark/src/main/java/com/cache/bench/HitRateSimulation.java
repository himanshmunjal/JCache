package com.cache.bench;

import com.cache.CacheFactory;
import com.cache.api.Cache;
import com.cache.api.CachePolicyType;

import java.util.function.LongFunction;

/**
 * Prints the read-through hit rate of each policy on each workload. Hit rate
 * does not depend on timing, so this is a plain program rather than a JMH
 * benchmark:
 *
 * <pre>
 * java -cp cache-benchmark/target/benchmarks.jar com.cache.bench.HitRateSimulation
 * </pre>
 */
public final class HitRateSimulation {

    private static final int OPERATIONS = 1_000_000;

    private HitRateSimulation() {
    }

    /**
     * @param args ignored
     */
    public static void main(String[] args) {
        System.out.printf("%-16s %8s %8s %8s%n", "workload", "LRU", "LFU", "ARC");
        run("zipf", BenchmarkWorkload::zipf);
        run("uniform", BenchmarkWorkload::uniform);
        run("scan + zipf", i -> i % (3L * BenchmarkWorkload.KEY_SPACE_SIZE) < BenchmarkWorkload.KEY_SPACE_SIZE
                ? BenchmarkWorkload.sequential(i)
                : BenchmarkWorkload.zipf(i));
    }

    private static void run(String name, LongFunction<String> keys) {
        StringBuilder row = new StringBuilder(String.format("%-16s", name));
        for (CachePolicyType policy : CachePolicyType.values()) {
            row.append(String.format(" %7.2f%%", hitRate(policy, keys) * 100));
        }
        System.out.println(row);
    }

    static double hitRate(CachePolicyType policy, LongFunction<String> keys) {
        Cache<String, String> cache = CacheFactory.withPolicy(policy, BenchmarkWorkload.CACHE_CAPACITY);
        long hits = 0;
        for (long i = 0; i < OPERATIONS; i++) {
            String key = keys.apply(i);
            if (cache.get(key) != null) {
                hits++;
            } else {
                cache.put(key, BenchmarkWorkload.valueFor(key));
            }
        }
        return (double) hits / OPERATIONS;
    }
}
