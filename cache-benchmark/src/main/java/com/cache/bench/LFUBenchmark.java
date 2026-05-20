package com.cache.bench;

import com.cache.api.Cache;
import com.cache.policy.LFUCache;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JMH benchmark suite for LFUCache.
 *
 * LFU CHARACTERISTICS TO EXPECT IN RESULTS:
 *
 *   STRENGTHS (where LFU wins):
 *   - Zipfian workload: LFU protects hot keys aggressively. Once key-0
 *     accumulates a high frequency count, it stays in cache even under
 *     pressure from less-accessed keys. Expected hit rate: 70-80% vs LRU's 55-65%.
 *   - Stable hot sets: if the same 10 keys are always popular, LFU locks
 *     them in and never evicts them.
 *
 *   WEAKNESSES (where LFU loses):
 *   - Cold start: a brand new key (frequency=1) is the first to be evicted
 *     even if it's about to become hot. This "frequency bias" means LFU
 *     adapts slowly to shifting access patterns.
 *   - Scan resistance: a sequential scan (key-0, key-1, ..., key-999)
 *     pollutes frequency counts. LFU performs WORSE than LRU on scan workloads.
 *   - Write throughput: LFU maintains frequency buckets (a second doubly linked list).
 *     put() is still O(1) in Shan's algorithm, but constant factors are larger
 *     than LRU's simpler move-to-front. Expect 10-20% lower write throughput than LRU.
 *
 *   In the PolicyComparison benchmark, you'll see LFU win on Zipfian reads
 *   and lose on uniform writes. This is the fundamental LFU tradeoff.
 *
 * SAME ANNOTATION CHOICES AS LRUBenchmark:
 *   @Fork(2), @Warmup(3x1s), @Measurement(5x2s)
 *   Consistent across all policy benchmarks so numbers are comparable.
 *   Never compare benchmarks with different fork/warmup/measurement settings.
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = {"-Xms512m", "-Xmx512m"})
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
public class LFUBenchmark {

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private Cache<String, String> lfuCache;
    private BenchmarkWorkload workload;
    private AtomicLong opCounter;
    private AtomicLong hits;
    private AtomicLong misses;

    // -------------------------------------------------------------------------
    // Setup & Teardown
    // -------------------------------------------------------------------------

    @Setup(Level.Trial)
    public void setup() {
        lfuCache  = new LFUCache<>(BenchmarkWorkload.CACHE_CAPACITY);
        workload  = new BenchmarkWorkload();
        workload.setup();
        opCounter = new AtomicLong(0);
        hits      = new AtomicLong(0);
        misses    = new AtomicLong(0);

        workload.warmCache(lfuCache);

        // IMPORTANT: After warming, LFU's frequency counts are all at 1
        // (each warmup key was put() once). A few get() calls on hot keys
        // will bump their frequency above 1, creating a more realistic
        // initial frequency distribution.
        preHeatFrequencies();
    }

    /**
     * Simulates some prior access history so LFU's frequency buckets
     * reflect a realistic steady-state distribution before measurement starts.
     *
     * Without this, every key starts with frequency=1 and LFU behaves like
     * random eviction for the first few thousand operations. This would make
     * LFU look worse than it really is in steady state.
     *
     * We access the top 10% of keys (the "hot set") 20 times each.
     * This creates the frequency separation LFU needs to do its job.
     */
    private void preHeatFrequencies() {
        int hotSetSize = BenchmarkWorkload.KEY_SPACE_SIZE / 10; // top 10%
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < hotSetSize; i++) {
                // Access top keys repeatedly to build up their frequency counts
                lfuCache.get("key-" + i);
            }
        }
    }

    /**
     * Prints LFU hit rate after measurement.
     *
     * INTERPRETING LFU HIT RATE:
     * On Zipfian workload, LFU should beat LRU by 10-20 percentage points.
     * If LFU hit rate ≈ LRU hit rate, your frequency buckets may not be
     * working correctly (all keys have the same frequency and LFU degrades to random).
     */
    @TearDown(Level.Trial)
    public void printHitRate() {
        long totalOps = hits.get() + misses.get();
        if (totalOps > 0) {
            double hitRate = (double) hits.get() / totalOps * 100.0;
            System.out.printf("[LFU] Hit rate: %.2f%% (%d hits / %d ops)%n",
                    hitRate, hits.get(), totalOps);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Reads
    // -------------------------------------------------------------------------

    /**
     * LFU get() on Zipfian workload.
     *
     * WHERE LFU SHINES: this benchmark.
     * Hot keys (key-0 through key-99) have high frequency counts.
     * When capacity pressure forces an eviction, LFU evicts the cold tail,
     * not a recently-accessed hot key like LRU might.
     *
     * Expected: LFU throughput ≈ LRU throughput (both are O(1)).
     * LFU advantage shows up in hit rate, not raw ops/sec.
     */
    @Benchmark
    public void get_zipfian(Blackhole bh) {
        String key    = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = lfuCache.get(key);

        if (result != null) hits.incrementAndGet();
        else                misses.incrementAndGet();

        bh.consume(result);
    }

    /**
     * LFU get() on Uniform workload.
     *
     * WHERE LFU STRUGGLES: this benchmark.
     * All keys have similar frequency counts under uniform access.
     * LFU's frequency tracking adds overhead with zero benefit —
     * no key deserves protection over any other.
     * Expected: LFU throughput ≈ LRU, hit rate ≈ LRU (both ~25.6%)
     */
    @Benchmark
    public void get_uniform(Blackhole bh) {
        String key    = workload.uniformKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = lfuCache.get(key);
        bh.consume(result);
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Writes
    // -------------------------------------------------------------------------

    /**
     * LFU put() on Zipfian workload.
     *
     * LFU put() does more work than LRU put():
     *   LRU put(): HashMap.put() + LinkedList.addToFront() = 2 operations
     *   LFU put(): HashMap.put() + FrequencyMap.put() + BucketList.addToFront()
     *              + (possibly) MinFrequency update = 3-4 operations
     *
     * All O(1) in Shan's algorithm, but constant factors matter.
     * Expect LFU put() to be 10-20% slower than LRU put().
     * This is the price you pay for frequency tracking.
     */
    @Benchmark
    public void put_zipfian(Blackhole bh) {
        String key   = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String value = workload.valueFor(key);
        lfuCache.put(key, value);
        bh.consume(key);
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Mixed workload
    // -------------------------------------------------------------------------

    /**
     * 80% get / 20% put on Zipfian — same ratio as LRUBenchmark for comparison.
     * LFU's advantage on reads partially offsets its slight disadvantage on writes.
     */
    @Benchmark
    public void mixed_80get_20put_zipfian(Blackhole bh) {
        long idx   = opCounter.getAndIncrement();
        String key = workload.zipfianKeys[(int)(idx % BenchmarkWorkload.WORKLOAD_SIZE)];

        if (idx % 5 == 0) {
            lfuCache.put(key, workload.valueFor(key));
        } else {
            bh.consume(lfuCache.get(key));
        }
    }

    /**
     * Scan workload — sequential key access pattern.
     * This is LFU's worst case: sequential scans pollute frequency counts.
     * Keys accessed during a scan get their frequency bumped, crowding out
     * truly hot keys. LFU performs worse than LRU here.
     *
     * Real-world scenario: a database full-table scan while the app is running.
     * This is why some systems use LIRS or ARC instead of pure LFU.
     *
     * Expected: LFU hit rate drops to near-zero during sustained scans.
     */
    @Benchmark
    public void get_sequential_scan(Blackhole bh) {
        // Sequential scan: key-0, key-1, ..., key-999, key-0, ...
        long idx   = opCounter.getAndIncrement();
        String key = "key-" + (idx % BenchmarkWorkload.KEY_SPACE_SIZE);
        bh.consume(lfuCache.get(key));
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LFUBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(2)
                .measurementIterations(3)
                .build();

        new Runner(opt).run();
    }
}