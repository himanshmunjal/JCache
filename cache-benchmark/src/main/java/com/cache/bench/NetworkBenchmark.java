package com.cache.bench;

import com.cache.client.CacheClient;
import com.cache.client.ConnectionPool;
import com.cache.server.CacheServer;
import com.cache.server.ServerConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Round trips through the real TCP server on localhost, using the pooled Java
 * client. The server runs in the benchmark JVM on a free port.
 *
 * <p>{@code SampleTime} mode reports latency percentiles; run with
 * {@code -t N} to vary the number of client threads.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgs = {"-Xms512m", "-Xmx512m"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Threads(4)
@State(Scope.Benchmark)
public class NetworkBenchmark {

    private CacheServer server;
    private ConnectionPool pool;

    /** Each thread walks the key sequence from its own random offset. */
    @State(Scope.Thread)
    public static class Cursor {
        long index;

        @Setup(Level.Trial)
        public void setUp() {
            index = ThreadLocalRandom.current().nextInt(BenchmarkWorkload.WORKLOAD_SIZE);
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        server = new CacheServer(ServerConfig.builder().port(0).cacheCapacity(10_000).build());
        server.startAsync();
        try (CacheClient loader = new CacheClient("localhost", server.getPort())) {
            for (int i = 0; i < BenchmarkWorkload.KEY_SPACE_SIZE; i++) {
                String key = BenchmarkWorkload.sequential(i);
                loader.put(key, BenchmarkWorkload.valueFor(key));
            }
        }
        pool = new ConnectionPool("localhost", server.getPort(), 16);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        pool.close();
        server.shutdown();
    }

    @Benchmark
    public boolean ping() throws Exception {
        CacheClient conn = pool.acquire();
        try {
            return conn.ping();
        } finally {
            pool.release(conn);
        }
    }

    @Benchmark
    public String get(Cursor cursor) throws Exception {
        CacheClient conn = pool.acquire();
        try {
            return conn.get(BenchmarkWorkload.zipf(cursor.index++));
        } finally {
            pool.release(conn);
        }
    }

    @Benchmark
    public void put(Cursor cursor) throws Exception {
        String key = BenchmarkWorkload.zipf(cursor.index++);
        CacheClient conn = pool.acquire();
        try {
            conn.put(key, BenchmarkWorkload.valueFor(key));
        } finally {
            pool.release(conn);
        }
    }
}
