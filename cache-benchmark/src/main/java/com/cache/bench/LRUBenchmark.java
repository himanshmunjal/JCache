package com.cache.bench;

import com.cache.api.Cache;
import com.cache.policy.LRUCache;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JMH benchmark suite for LRUCache.
 *
 * WHAT WE MEASURE:
 *   1. get() throughput on Zipfian workload   → realistic hot-key scenario
 *   2. get() throughput on Uniform workload   → worst-case, no hot keys
 *   3. put() throughput (write-heavy)          → measures eviction overhead
 *   4. Mixed 80/20 get/put ratio              → closest to real application load
 *   5. Hit rate under Zipfian vs Uniform       → quality metric, not just speed
 *
 * ANNOTATION DECISIONS:
 *
 *   @Fork(2) — run 2 separate JVM processes.
 *   Why? JIT compilation state leaks between benchmarks in the same JVM.
 *   Forking gives each benchmark a clean JVM. 2 forks gives enough samples
 *   without taking hours. Use Fork(1) for quick local runs during development.
 *
 *   @Warmup(iterations=3, time=1) — 3 warmup iterations, 1 second each.
 *   Why 3? The JVM needs roughly 10,000–100,000 invocations to JIT-compile
 *   a method. At millions of ops/sec, 1 second is plenty per iteration.
 *   First iteration: interpreter. Second: C1 compiled. Third: C2 optimized.
 *
 *   @Measurement(iterations=5, time=2) — 5 measurement iterations, 2 seconds each.
 *   More time per iteration reduces noise from GC pauses.
 *   5 iterations gives JMH enough samples to compute a meaningful error interval.
 *
 *   @State(Scope.Benchmark) — the LRUCache instance is shared across all threads.
 *   This is correct — we want threads to contend on the same cache, not
 *   each have their own private copy (that would defeat the concurrency test).
 */
@BenchmarkMode({
        Mode.Throughput,
        Mode.AverageTime
})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = {"-Xms512m", "-Xmx512m"})
// Fixed heap size prevents GC behavior from changing mid-benchmark.
// -Xms = -Xmx means no heap expansion pauses.
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
public class LRUBenchmark {

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * The cache under test. Initialized once per fork in @Setup.
     * Shared across all threads in Scope.Benchmark.
     */
    private Cache<String, String> lruCache;

    /**
     * Shared workload — Zipfian and Uniform key arrays.
     * Also pre-warms the cache in @Setup.
     */
    private BenchmarkWorkload workload;

    /**
     * Operation counter for cycling through the workload array.
     * AtomicLong for thread-safe increment in multi-threaded benchmarks.
     *
     * NOTE: AtomicLong.getAndIncrement() has some overhead vs plain int++.
     * For single-threaded benchmarks, the overhead is negligible.
     * For multi-threaded benchmarks, this is correct — plain int would race.
     */
    private AtomicLong opCounter;

    /**
     * Hit/miss counters for computing hit rate.
     * Updated in the benchmark loop, read after measurement finishes.
     */
    private AtomicLong hits;
    private AtomicLong misses;

    // -------------------------------------------------------------------------
    // Setup & Teardown
    // -------------------------------------------------------------------------

    /**
     * Called once per fork before any benchmark method runs.
     * Creates a fresh LRUCache and pre-warms it with CACHE_CAPACITY entries.
     *
     * WHY PRE-WARM?
     * An empty cache has 100% miss rate — every get() is a miss.
     * Real caches run in steady state. Pre-warming simulates that.
     * Without warming, your first measurement iteration looks 10x worse
     * than the others, inflating the error interval.
     */
    @Setup(Level.Trial)
    public void setup() {
        lruCache  = new LRUCache<>(BenchmarkWorkload.CACHE_CAPACITY);
        workload  = new BenchmarkWorkload();
        workload.setup(); // manually trigger @Setup since we're calling it directly
        opCounter = new AtomicLong(0);
        hits      = new AtomicLong(0);
        misses    = new AtomicLong(0);

        workload.warmCache(lruCache); // fill cache to steady state
    }

    /**
     * Called after all measurement iterations complete.
     * Prints hit rate so you have a quality metric alongside throughput.
     *
     * Hit rate = hits / (hits + misses) × 100%
     * For Zipfian workload on 256-capacity / 1000-key cache:
     *   Expected LRU hit rate: ~55–65%
     *   (hot keys cycle in and out; LRU doesn't protect them perfectly)
     */
    @TearDown(Level.Trial)
    public void printHitRate() {
        long totalOps = hits.get() + misses.get();
        if (totalOps > 0) {
            double hitRate = (double) hits.get() / totalOps * 100.0;
            System.out.printf("[LRU] Hit rate: %.2f%% (%d hits / %d ops)%n",
                    hitRate, hits.get(), totalOps);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Reads
    // -------------------------------------------------------------------------

    /**
     * Measures get() throughput under Zipfian access pattern.
     *
     * Zipfian means: key-0 is accessed ~7x more than key-999.
     * Top 10% of keys account for ~70% of accesses.
     * LRU should keep hot keys in cache most of the time.
     *
     * @param bh Blackhole — consumes the return value to prevent JIT from
     *           eliminating the get() call as dead code.
     */
    @Benchmark
    public void get_zipfian(Blackhole bh) {
        String key    = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = lruCache.get(key);

        // Track hit/miss for hit rate calculation in @TearDown
        if (result != null) hits.incrementAndGet();
        else                misses.incrementAndGet();

        // Blackhole.consume() tells JMH "this value is used".
        // Without this, JIT might see that result is unused and skip get() entirely.
        bh.consume(result);
    }

    /**
     * Measures get() throughput under Uniform access pattern.
     * Every key equally likely — no hot keys, maximum cache pressure.
     * LRU hit rate under uniform is roughly: capacity / key_space_size = 256/1000 = 25.6%
     */
    @Benchmark
    public void get_uniform(Blackhole bh) {
        String key    = workload.uniformKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = lruCache.get(key);
        bh.consume(result);
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Writes
    // -------------------------------------------------------------------------

    /**
     * Measures put() throughput — this exercises eviction logic.
     * Every put() that exceeds capacity triggers an eviction.
     * With 1000 keys cycling through a 256-capacity cache, ~74% of puts
     * cause an eviction (tail removal + HashMap update).
     *
     * This benchmark reveals the cost of LRU's move-to-front operation:
     * every put() must update the doubly linked list (O(1) but with pointer writes).
     */
    @Benchmark
    public void put_zipfian(Blackhole bh) {
        String key   = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String value = workload.valueFor(key);
        lruCache.put(key, value);
        bh.consume(key); // consume key to prevent elimination of the lookup
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Mixed workload
    // -------------------------------------------------------------------------

    /**
     * 80% get / 20% put mixed workload on Zipfian distribution.
     *
     * WHY 80/20?
     * Most real caches are read-heavy. A ratio of 80% reads to 20% writes
     * matches typical web application cache usage (read user data, occasionally update).
     *
     * The modulo trick (opCounter % 5 == 0) gives exactly 20% put probability
     * without branching on random — deterministic and branch-predictor friendly.
     */
    @Benchmark
    public void mixed_80get_20put_zipfian(Blackhole bh) {
        long idx    = opCounter.getAndIncrement();
        String key  = workload.zipfianKeys[(int)(idx % BenchmarkWorkload.WORKLOAD_SIZE)];

        if (idx % 5 == 0) {
            // 20% of ops: write
            lruCache.put(key, workload.valueFor(key));
        } else {
            // 80% of ops: read
            String result = lruCache.get(key);
            bh.consume(result);
        }
    }

    /**
     * 50% get / 50% put — write-heavy scenario.
     * Models a cache used as a write-through buffer or a rate limiter
     * where every request updates a counter (put) and reads a threshold (get).
     */
    @Benchmark
    public void mixed_50get_50put_zipfian(Blackhole bh) {
        long idx   = opCounter.getAndIncrement();
        String key = workload.zipfianKeys[(int)(idx % BenchmarkWorkload.WORKLOAD_SIZE)];

        if (idx % 2 == 0) {
            lruCache.put(key, workload.valueFor(key));
        } else {
            bh.consume(lruCache.get(key));
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public String latency_get_zipfian() {
        String key =
                workload.zipfianKeys[
                        (int)(opCounter.getAndIncrement()
                                % BenchmarkWorkload.WORKLOAD_SIZE)];

        return lruCache.get(key);
    }

    // -------------------------------------------------------------------------
    // Main method for running outside Maven
    // -------------------------------------------------------------------------

    /**
     * Allows running this benchmark directly from an IDE or with java -jar.
     * In production benchmarking, always use the Maven JMH plugin or
     * the benchmarks JAR (mvn package → java -jar benchmarks.jar LRUBenchmark).
     *
     * For quick local runs: right-click main() → Run in IntelliJ.
     * This uses Fork(0) to skip forking — faster but less accurate.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LRUBenchmark.class.getSimpleName())
                .forks(1)          // use 1 fork for quick local runs
                .warmupIterations(2)
                .measurementIterations(3)
                .build();

        new Runner(opt).run();
    }
}