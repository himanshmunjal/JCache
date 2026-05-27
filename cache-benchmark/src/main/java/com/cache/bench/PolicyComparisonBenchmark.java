package com.cache.bench;

import com.cache.api.Cache;
import com.cache.policy.ARCCache;
import com.cache.policy.LFUCache;
import com.cache.policy.LRUCache;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PolicyComparisonBenchmark — the most important benchmark in the suite.
 *
 * This is the file you point to in interviews when asked:
 * "How do you know which eviction policy to use?"
 *
 * It runs ALL THREE POLICIES against the SAME workloads in the SAME JVM fork
 * using @Param to switch between them. This guarantees apples-to-apples comparison:
 * same key distribution, same cache capacity, same JVM state.
 *
 * HOW @Param WORKS:
 *   JMH creates one benchmark variant per @Param value.
 *   With @Param({"LRU", "LFU", "ARC"}), JMH runs each @Benchmark method 3 times —
 *   once per policy. The output shows:
 *
 *     PolicyComparisonBenchmark.read_zipfian  policy=ARC  thrpt  10  4231.44 ± 12.3  ops/ms
 *     PolicyComparisonBenchmark.read_zipfian  policy=LFU  thrpt  10  4589.21 ± 18.7  ops/ms
 *     PolicyComparisonBenchmark.read_zipfian  policy=LRU  thrpt  10  5102.33 ± 9.1   ops/ms
 *
 *   From this you can say:
 *   "LRU is fastest in raw throughput (less bookkeeping), but I measured that
 *   LFU achieves 12% higher hit rate under Zipfian, and ARC achieves 8% better
 *   hit rate than LRU while being scan-resistant — at 17% lower throughput."
 *
 * WHAT TO LOOK FOR IN RESULTS:
 *
 *   Throughput ranking (ops/ms, higher = better):
 *     LRU > LFU > ARC  (almost always, due to implementation complexity)
 *
 *   Hit rate ranking under Zipfian (higher = better):
 *     LFU ≥ ARC > LRU  (frequency tracking beats recency tracking for hot keys)
 *
 *   Hit rate ranking under Uniform:
 *     LRU ≈ LFU ≈ ARC  (no hot keys to protect — all policies equal)
 *
 *   Scan workload:
 *     ARC > LFU > LRU  (ARC scan resistance, LFU frequency protection, LRU vulnerable)
 *
 * THE INTERVIEW STATEMENT THIS FILE ENABLES:
 *   "I benchmarked all three policies with JMH on Zipfian and uniform workloads.
 *    LRU achieved [X] ops/ms with [Y]% hit rate. LFU achieved [X-10%] ops/ms
 *    but [Y+12%] hit rate — it's the right choice when your hot set is stable.
 *    ARC was [X-17%] throughput but adaptive — it's the right choice when access
 *    patterns shift unpredictably, which is why ZFS uses it."
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = {"-Xms512m", "-Xmx512m"})
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
public class PolicyComparisonBenchmark {

    // -------------------------------------------------------------------------
    // @Param — the key annotation that makes this a comparison benchmark
    // -------------------------------------------------------------------------

    /**
     * JMH will instantiate and run each benchmark method once per policy value.
     * The policy string is used in @Setup to create the correct cache implementation.
     *
     * Adding a new policy: add its name to this array and a case to createCache().
     * JMH discovers the new variant automatically — no other changes needed.
     */
    @Param({"LRU", "LFU", "ARC"})
    private String policy;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * The cache under test — polymorphic via Cache<K,V> interface.
     * Created in @Setup based on the current @Param policy value.
     * This is the strategy pattern in action: the benchmark loop doesn't
     * know or care which policy it's testing.
     */
    private Cache<String, String> cache;

    private BenchmarkWorkload workload;
    private AtomicLong opCounter;

    // Hit/miss counters split by workload type for per-workload hit rate reporting
    private AtomicLong zipfianHits;
    private AtomicLong zipfianMisses;
    private AtomicLong uniformHits;
    private AtomicLong uniformMisses;

    // -------------------------------------------------------------------------
    // Setup & Teardown
    // -------------------------------------------------------------------------

    /**
     * Creates the appropriate cache based on @Param policy.
     * All caches get the same capacity — fair comparison.
     */
    @Setup(Level.Trial)
    public void setup() {
        cache      = createCache(policy);
        workload   = new BenchmarkWorkload();
        workload.setup();
        opCounter  = new AtomicLong(0);

        zipfianHits   = new AtomicLong(0);
        zipfianMisses = new AtomicLong(0);
        uniformHits   = new AtomicLong(0);
        uniformMisses = new AtomicLong(0);

        // Warm the cache — same warmup for all policies, fair comparison
        workload.warmCache(cache);
    }

    /**
     * Creates a cache instance for the given policy name.
     *
     * All caches share the same capacity (BenchmarkWorkload.CACHE_CAPACITY = 256).
     * This is the ONLY place policy-specific code lives in this benchmark.
     *
     * @param policyName One of "LRU", "LFU", "ARC"
     * @return A fresh cache instance for that policy
     */
    private Cache<String, String> createCache(String policyName) {
        int capacity = BenchmarkWorkload.CACHE_CAPACITY;
        switch (policyName) {
            case "LRU": return new LRUCache<>(capacity);
            case "LFU": return new LFUCache<>(capacity);
            case "ARC": return new ARCCache<>(capacity);
            default:    throw new IllegalArgumentException("Unknown policy: " + policyName);
        }
    }

    /**
     * Prints hit rate per workload type after all measurement iterations complete.
     *
     * This output is what goes into your BenchmarkResults.md and README charts.
     * Copy-paste these numbers into a table like:
     *
     * | Policy | Zipfian Hit Rate | Uniform Hit Rate | Throughput (ops/ms) |
     * |--------|-----------------|------------------|---------------------|
     * | LRU    | 58.3%           | 25.4%            | 5102                |
     * | LFU    | 71.2%           | 25.7%            | 4589                |
     * | ARC    | 66.8%           | 25.5%            | 4231                |
     */
    @TearDown(Level.Trial)
    public void printHitRates() {
        printRate(policy, "Zipfian", zipfianHits.get(), zipfianMisses.get());
        printRate(policy, "Uniform", uniformHits.get(), uniformMisses.get());
    }

    private void printRate(String pol, String workloadName, long h, long m) {
        long total = h + m;
        if (total == 0) return;
        System.out.printf("[%s][%s] Hit rate: %.2f%% (%d/%d)%n",
                pol, workloadName, (double) h / total * 100.0, h, total);
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Reads
    // -------------------------------------------------------------------------

    /**
     * Read throughput on Zipfian workload — the primary comparison benchmark.
     *
     * This single benchmark gives you the most important number:
     * which policy serves the most reads per millisecond on realistic traffic.
     *
     * WHAT TO TELL THE INTERVIEWER:
     * "All three policies are O(1) for get(), but constant factors differ.
     *  LRU does 1 linked list update + 1 HashMap lookup.
     *  LFU does 1 HashMap lookup + 1 frequency map update + 1 bucket list update.
     *  ARC does 1 set lookup (T1 or T2) + possibly a list promotion.
     *  The benchmark shows this theoretical difference as real throughput numbers."
     */
    @Benchmark
    public void read_zipfian(Blackhole bh) {
        String key    = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = cache.get(key);

        if (result != null) zipfianHits.incrementAndGet();
        else                zipfianMisses.incrementAndGet();

        bh.consume(result);
    }

    /**
     * Read throughput on Uniform workload.
     *
     * EXPECTED RESULT: all three policies nearly identical.
     * Under uniform access, every key has equal probability of eviction.
     * No policy has an advantage. The only difference is implementation overhead.
     * This benchmark is the "control group" — differences here are pure overhead,
     * not policy intelligence.
     */
    @Benchmark
    public void read_uniform(Blackhole bh) {
        String key    = workload.uniformKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        String result = cache.get(key);

        if (result != null) uniformHits.incrementAndGet();
        else                uniformMisses.incrementAndGet();

        bh.consume(result);
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Writes
    // -------------------------------------------------------------------------

    /**
     * Write throughput comparison across all three policies.
     *
     * Writes stress eviction logic:
     *   LRU: remove tail, add to head → 2 pointer updates
     *   LFU: find min-frequency bucket, remove from it, add to freq-1 bucket → 3-4 updates
     *   ARC: check ghost lists, possibly update p, evict from T1 or T2 → 4-6 operations
     *
     * EXPECTED: LRU > LFU > ARC in write throughput.
     * The gap reveals how much you pay for better eviction decisions.
     */
    @Benchmark
    public void write_zipfian(Blackhole bh) {
        String key   = workload.zipfianKeys[(int)(opCounter.getAndIncrement() % BenchmarkWorkload.WORKLOAD_SIZE)];
        cache.put(key, workload.valueFor(key));
        bh.consume(key);
    }

    // -------------------------------------------------------------------------
    // Benchmark methods — Mixed workload (the most realistic)
    // -------------------------------------------------------------------------

    /**
     * 80% reads / 20% writes on Zipfian — the most realistic workload.
     *
     * This is THE benchmark that represents real application behavior:
     * web apps, API gateways, and database query caches are all read-heavy
     * with occasional writes (cache misses, updates, TTL refreshes).
     *
     * The final number you quote in interviews should come from this benchmark:
     * "Under realistic 80/20 read/write Zipfian traffic, my cache achieves
     * [LRU: X ops/ms], [LFU: Y ops/ms], [ARC: Z ops/ms] with hit rates of
     * [X%], [Y%], and [Z%] respectively."
     */
    @Benchmark
    public void mixed_80r_20w_zipfian(Blackhole bh) {
        long idx   = opCounter.getAndIncrement();
        String key = workload.zipfianKeys[(int)(idx % BenchmarkWorkload.WORKLOAD_SIZE)];

        if (idx % 5 == 0) {
            // 20% writes
            cache.put(key, workload.valueFor(key));
        } else {
            // 80% reads
            bh.consume(cache.get(key));
        }
    }

    /**
     * 80% reads / 20% writes on Uniform — the worst case for all policies.
     *
     * Under uniform load, all policies struggle equally.
     * This benchmark answers: "what's the floor performance when caching doesn't help?"
     * If your result here is still high (e.g., 4000 ops/ms), you can say:
     * "Even in the worst case — uniform distribution where caching provides no
     *  benefit — the cache still serves 4M ops/sec, purely as a key-value store."
     */
    @Benchmark
    public void mixed_80r_20w_uniform(Blackhole bh) {
        long idx   = opCounter.getAndIncrement();
        String key = workload.uniformKeys[(int)(idx % BenchmarkWorkload.WORKLOAD_SIZE)];

        if (idx % 5 == 0) {
            cache.put(key, workload.valueFor(key));
        } else {
            bh.consume(cache.get(key));
        }
    }

    // -------------------------------------------------------------------------
    // Main — with output format configuration
    // -------------------------------------------------------------------------

    /**
     * Runs the comparison benchmark with JSON output for charting.
     *
     * JSON output can be fed into:
     *   - jmh-visualizer.appspot.com (paste JSON, get bar charts)
     *   - A simple Python matplotlib script for README charts
     *   - IntelliJ's JMH plugin for visual display
     *
     * To generate JSON: add -rf json -rff results.json to the OptionsBuilder,
     * or run: java -jar benchmarks.jar PolicyComparison -rf json -rff results.json
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PolicyComparisonBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(2)
                .measurementIterations(3)
                 .resultFormat(ResultFormatType.JSON)
                 .result("policy-comparison-results.json")
                .build();

        new Runner(opt).run();
    }
}