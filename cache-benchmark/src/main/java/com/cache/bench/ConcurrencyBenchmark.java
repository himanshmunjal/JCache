package com.cache.bench;

import com.cache.api.Cache;
import com.cache.concurrent.CoarseGrainedCache;
import com.cache.concurrent.LockFreeCache;
import com.cache.concurrent.SegmentedCache;
import com.cache.policy.LRUCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Throughput of the three thread-safe wrappers when many threads share one
 * cache. Runs with 8 threads by default; use {@code -t} to change that:
 *
 * <pre>
 * java -jar cache-benchmark/target/benchmarks.jar Concurrency -t 16
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2, jvmArgs = {"-Xms512m", "-Xmx512m"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Threads(8)
@State(Scope.Benchmark)
public class ConcurrencyBenchmark {

    @Param({"COARSE", "SEGMENTED", "LOCKFREE"})
    private String strategy;

    private Cache<String, String> cache;

    /** Each thread walks the key sequence from its own random offset. */
    @State(Scope.Thread)
    public static class Cursor {
        long index;

        @Setup(Level.Trial)
        public void setUp() {
            index = ThreadLocalRandom.current().nextInt(BenchmarkWorkload.WORKLOAD_SIZE);
        }

        String nextKey() {
            return BenchmarkWorkload.zipf(index++);
        }
    }

    @Setup(Level.Trial)
    public void setUp() {
        int capacity = BenchmarkWorkload.CACHE_CAPACITY;
        cache = switch (strategy) {
            case "COARSE" -> new CoarseGrainedCache<>(new LRUCache<>(capacity));
            case "SEGMENTED" -> new SegmentedCache<>(capacity, 16);
            case "LOCKFREE" -> new LockFreeCache<>(capacity);
            default -> throw new IllegalArgumentException("Unknown strategy " + strategy);
        };
        BenchmarkWorkload.warm(cache);
    }

    @Benchmark
    public String read90_write10(Cursor cursor) {
        String key = cursor.nextKey();
        if (cursor.index % 10 == 0) {
            cache.put(key, BenchmarkWorkload.valueFor(key));
            return key;
        }
        return cache.get(key);
    }

    @Benchmark
    public String read50_write50(Cursor cursor) {
        String key = cursor.nextKey();
        if ((cursor.index & 1) == 0) {
            cache.put(key, BenchmarkWorkload.valueFor(key));
            return key;
        }
        return cache.get(key);
    }

    @Benchmark
    public String readOnly(Cursor cursor) {
        return cache.get(cursor.nextKey());
    }
}
