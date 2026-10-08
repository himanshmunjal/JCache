package com.cache.bench;

import com.cache.CacheFactory;
import com.cache.api.Cache;
import com.cache.api.CachePolicyType;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Single-threaded throughput of LRU, LFU and ARC on the same workloads.
 *
 * <p>The {@code readThrough*} benchmarks behave like an application cache:
 * on a miss the value is "loaded" and put into the cache. The {@code hits}
 * and {@code misses} counters JMH prints alongside each result give the hit
 * rate, which is where the policies actually differ. {@code scanThenZipf}
 * alternates a full sequential scan with Zipf traffic; LRU loses its hot
 * keys to the scan while LFU and ARC are designed to keep them.
 *
 * <pre>
 * java -jar cache-benchmark/target/benchmarks.jar PolicyComparison
 * java -jar cache-benchmark/target/benchmarks.jar PolicyComparison -p policy=ARC
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 2, jvmArgs = {"-Xms512m", "-Xmx512m"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Thread)
public class PolicyComparisonBenchmark {

    @Param({"LRU", "LFU", "ARC"})
    private CachePolicyType policy;

    private Cache<String, String> cache;
    private long index;

    /** Hit and miss counts reported next to the throughput. */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class HitCounters {
        /** Lookups that found a value. */
        public long hits;
        /** Lookups that did not. */
        public long misses;

        @Setup(Level.Iteration)
        public void reset() {
            hits = 0;
            misses = 0;
        }
    }

    @Setup(Level.Trial)
    public void setUp() {
        cache = CacheFactory.withPolicy(policy, BenchmarkWorkload.CACHE_CAPACITY);
        BenchmarkWorkload.warm(cache);
    }

    @Benchmark
    public String get_zipf() {
        return cache.get(BenchmarkWorkload.zipf(index++));
    }

    @Benchmark
    public void put_zipf() {
        String key = BenchmarkWorkload.zipf(index++);
        cache.put(key, BenchmarkWorkload.valueFor(key));
    }

    @Benchmark
    public String readThrough_zipf(HitCounters counters) {
        return readThrough(BenchmarkWorkload.zipf(index++), counters);
    }

    @Benchmark
    public String readThrough_uniform(HitCounters counters) {
        return readThrough(BenchmarkWorkload.uniform(index++), counters);
    }

    @Benchmark
    public String readThrough_scanThenZipf(HitCounters counters) {
        long i = index++;
        boolean scanning = i % (3L * BenchmarkWorkload.KEY_SPACE_SIZE) < BenchmarkWorkload.KEY_SPACE_SIZE;
        String key = scanning ? BenchmarkWorkload.sequential(i) : BenchmarkWorkload.zipf(i);
        return readThrough(key, counters);
    }

    private String readThrough(String key, HitCounters counters) {
        String value = cache.get(key);
        if (value != null) {
            counters.hits++;
            return value;
        }
        counters.misses++;
        value = BenchmarkWorkload.valueFor(key);
        cache.put(key, value);
        return value;
    }
}
