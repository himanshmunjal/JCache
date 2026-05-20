package com.cache.bench;

import com.cache.api.Cache;
import com.cache.policy.ARCCache;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JMH benchmark suite for ARCCache (Adaptive Replacement Cache).
 *
 * ARC BACKGROUND — why it's in ZFS and macOS:
 *
 *   ARC maintains four lists: T1 (recent, seen once), T2 (frequent, seen 2+),
 *   B1 (ghost of evicted T1 entries), B2 (ghost of evicted T2 entries).
 *   The adaptation parameter `p` controls the split between T1 and T2 capacity.
 *
 *   A B1 hit means: "we recently evicted a key that was only seen once,
 *   and now it's being accessed again — increase T1's share (recency weight)".
 *   A B2 hit means: "we evicted a frequently-accessed key — increase T2's share
 *   (frequency weight)".
 *
 *   This adaptation is why ARC handles workload SHIFTS better than LRU or LFU:
 *   it starts LRU-like and shifts toward LFU-like behavior as hot keys emerge.
 *
 * ARC CHARACTERISTICS TO EXPECT IN RESULTS:
 *
 *   STRENGTHS:
 *   - Adapts to Zipfian workloads without manual tuning (unlike LRU's "size" param)
 *   - Scan-resistant: sequential scans fill T1 but don't evict T2 hot keys
 *   - Workload shift: if access patterns change, p adapts within ~capacity iterations
 *   - Hit rate: typically between LRU and LFU on Zipfian, slightly better than LFU
 *     on workloads with phase shifts
 *
 *   WEAKNESSES:
 *   - Memory overhead: ghost lists (B1, B2) store keys (not values) beyond capacity.
 *     Total memory footprint ≈ 2x capacity (keys only for ghost entries).
 *   - Throughput: more bookkeeping per operation than LRU.
 *     get() must check T1, T2, and potentially update p. Expect 15-25% lower
 *     throughput than LRU in pure speed tests.
 *   - Patent: ARC was patented by IBM (US 6,996,676). Expired in 2023, but
 *     this is why it wasn't in Linux kernel for a long time.
 *
 * INTERVIEW TALKING POINT:
 *   "ARC is used in ZFS (OpenZFS still uses it) and was in macOS's UBC layer.
 *    I benchmarked it against LRU and LFU and found it achieves [X]% better
 *    hit rate than LRU on Zipfian workloads while handling sequential scans
 *    better than LFU, at the cost of [Y]% lower throughput due to ghost list overhead."
 *
 * ARC UNIQUE BENCHMARK — workload phase shift:
 *   We add a benchmark that shifts access patterns mid-run to show ARC's
 *   adaptation advantage. This benchmark doesn't have a LRU/LFU equivalent.
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = {"-Xms512m", "-Xmx512m"})
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
public class ARCBenchmark {

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private Cache<String, String> arcCache;
    private BenchmarkWorkload workload;
    private AtomicLong opCounter;
    private AtomicLong hits;
    private AtomicLong misses;

    // -------------------------------------------------------------------------
    // Setup & Teardown
    // -------------------------------------------------------------------------

    @Setup(Level.Trial)
    public void setup() {
        arcCache  = new ARCCache<>(BenchmarkWorkload.CACHE_CAPACITY);
        workload  = new BenchmarkWorkload();
        workload.setup();
        opCounter = new AtomicLong(0);
        hits      = new AtomicLong(0);
        misses    = new AtomicLong(0);

        // ARC warms differently from LRU/LFU:
        // Initial puts go into T1 (recent list). Subsequent gets on the same keys
        // promote them to T2 (frequent list). This is ARC's natural "learning" phase.
        warmARCNaturally();
    }

    /**
     * Warms ARC by simulating realistic access — not just puts.
     *
     * ARC's design means a cold-start put() fills T1. Only on the second access
     * does a key graduate to T2. If we only call put() in warmup, all keys sit
     * in T1 with equal priority — ARC hasn't "learned" anything yet.
     *
     * This warm-up does 2 passes:
     *   Pass 1 (put): fills T1 with CACHE_CAPACITY keys
     *   Pass 2 (get on hot keys): promotes top 25% of keys to T2
     *
     * After this, ARC's p value has started adapting toward a realistic split.
     */
    private void warmARCNaturally() {
        int cap = BenchmarkWorkload.CACHE_CAPACITY;

        // Pass 1: put all warmup keys (fills T1)
        for (int i = 0; i < cap; i++) {
            arcCache.put("key-" + i, "warmup-" + i);
        }

        // Pass 2: access top 25% of keys again (promotes them to T2)
        // This simulates the "hot set" that ARC will have learned to protect
        int hotSet = cap / 4;
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < hotSet; i++) {
                arcCache.get("key-" + i);
            }
        }
    }

    /**
     * Prints ARC hit rate and current p value after measurement.
     *
     * The p value tells you how ARC adapted:
     *   p close to 0   = ARC heavily weighted toward T2 (frequency-biased)
     *                     → workload had strong hot keys
     *   p close to cap = ARC heavily weighted toward T1 (recency-biased)
     *                     → workload was scan-heavy or uniform
     *   p near cap/2   = ARC found an even balance
     *                     → mixed workload with moderate hot keys
     */
    @TearDown(Level.Trial)
    public void printStats() {
        long totalOps = hits.get() + misses.get();
        if (totalOps > 0) {
            double hitRate = (double) hits.get() / totalOps * 100.0;
            System.out.printf("[ARC] Hit rate: %.2f%% (%d hits / %d ops)%n",
                    hitRate, hits.get(), totalOps);

            // If ARCCache exposes getP() or similar, print it here.
            // This shows the adaptation story during the interview demo.
            // Example: System.out.printf("[ARC] Final p value: %d%n", ((ARCCache)arcCache).getP());
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Core scenarios
    // -------------------------------------------------------------------------

    /**
     * ARC get() on Zipfian workload.
     *
     * EXPECTED RESULT: hit rate between LFU and LRU+10%.
     * ARC adapts p to protect hot keys (like LFU) while also keeping
     * recently-accessed keys (like LRU). The combination beats pure LRU.
     *
     * Throughput: slightly lower than LRU (more bookkeeping per get()).
     * A get() that hits T1 promotes to T2 and updates the list.
     * A get() that hits T2 moves to T2's front. Both update two lists.
     * LRU's get() only updates one list.
     */
    @Benchmark
    public void get_zipfian(Blackhole bh) {
        String key    = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = arcCache.get(key);

        if (result != null) hits.incrementAndGet();
        else                misses.incrementAndGet();

        bh.consume(result);
    }

    /**
     * ARC get() on Uniform workload.
     * No hot keys → p oscillates, no clear adaptation target.
     * ARC's adaptive overhead is pure cost with no benefit here.
     * Expected: ARC ≈ LRU in hit rate, slightly lower throughput.
     */
    @Benchmark
    public void get_uniform(Blackhole bh) {
        String key    = workload.uniformKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = arcCache.get(key);
        bh.consume(result);
    }

    /**
     * ARC put() — measures write overhead including ghost list management.
     *
     * ARC put() is the most complex of the three policies:
     *   1. Check if key is in B1 (ghost of T1) → if yes, increase p, promote to T2
     *   2. Check if key is in B2 (ghost of T2) → if yes, decrease p, promote to T2
     *   3. If cache full: evict from T1 or T2 based on current p, add to ghost list
     *   4. Add new key to T1
     *
     * This is 4-6 data structure operations vs LRU's 2.
     * Expect ARC put() to be 20-35% slower than LRU put().
     * This overhead is the cost of ARC's "memory" of recently evicted keys.
     */
    @Benchmark
    public void put_zipfian(Blackhole bh) {
        String key   = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String value = workload.valueFor(key);
        arcCache.put(key, value);
        bh.consume(key);
    }

    // -------------------------------------------------------------------------
    // ARC-unique benchmark: scan resistance
    // -------------------------------------------------------------------------

    /**
     * Sequential scan followed by Zipfian access — ARC's showcase benchmark.
     *
     * SCENARIO (simulated in a single benchmark):
     *   First half of operations: sequential scan (key-0 through key-999 in order)
     *   Second half: Zipfian access (hot keys dominate again)
     *
     * WHY ARC WINS HERE:
     *   During the scan phase, ARC puts new keys into T1.
     *   T1 fills up, but ARC limits T1's growth because p stays low
     *   (no B1 ghost hits — scan keys are all new).
     *   T2 (the hot set from the warmup phase) is protected.
     *   After the scan, Zipfian access resumes and the hot keys are still in T2.
     *
     *   LRU would have evicted hot keys to make room for scan keys.
     *   LFU would have kept hot keys (scan gives each key freq=1, below hot keys)
     *   but only if frequencies were already differentiated before the scan.
     *   ARC handles this without requiring pre-differentiated frequencies.
     *
     * In a single JMH benchmark we simulate this by interleaving scan and
     * Zipfian keys, using opCounter to determine which phase we're in.
     */
    @Benchmark
    public void scan_then_zipfian(Blackhole bh) {
        long idx = opCounter.getAndIncrement();
        String key;

        // Every 1000 operations, alternate between a scan pass and a Zipfian pass
        long phaseIndex = idx % (2L * BenchmarkWorkload.KEY_SPACE_SIZE);

        if (phaseIndex < BenchmarkWorkload.KEY_SPACE_SIZE) {
            // Scan phase: sequential keys
            key = "key-" + phaseIndex;
        } else {
            // Zipfian phase: hot key access
            key = workload.zipfianKeys[(int)(idx % BenchmarkWorkload.WORKLOAD_SIZE)];
        }

        String result = arcCache.get(key);
        bh.consume(result);
    }

    // -------------------------------------------------------------------------
    // Mixed workload
    // -------------------------------------------------------------------------

    /**
     * 80/20 mixed workload — same as LRU and LFU benchmarks for direct comparison.
     * Compare this number against LRUBenchmark.mixed_80get_20put_zipfian and
     * LFUBenchmark.mixed_80get_20put_zipfian in PolicyComparisonBenchmark.
     */
    @Benchmark
    public void mixed_80get_20put_zipfian(Blackhole bh) {
        long idx   = opCounter.getAndIncrement();
        String key = workload.zipfianKeys[(int)(idx % BenchmarkWorkload.WORKLOAD_SIZE)];

        if (idx % 5 == 0) {
            arcCache.put(key, workload.valueFor(key));
        } else {
            bh.consume(arcCache.get(key));
        }
    }

    // -------------------------------------------------------------------------
    // Main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ARCBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(2)
                .measurementIterations(3)
                .build();

        new Runner(opt).run();
    }
}