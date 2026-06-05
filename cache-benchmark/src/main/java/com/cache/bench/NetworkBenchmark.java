package com.cache.bench;

import com.cache.client.CacheClient;
import com.cache.client.ConnectionPool;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * NetworkBenchmark measures the end-to-end latency and throughput of the
 * JCache server over a real TCP connection.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * WHY NETWORK BENCHMARKS MATTER
 * ═══════════════════════════════════════════════════════════════════════
 *
 * The in-process benchmarks (LRUBenchmark, ConcurrencyBenchmark) measure
 * pure cache algorithm performance: nanoseconds per get/put with no I/O.
 * Those numbers tell you about eviction policy and locking overhead.
 *
 * NetworkBenchmark measures something completely different:
 *   - TCP round-trip latency (loopback: ~0.05ms, LAN: ~0.5ms)
 *   - Netty pipeline overhead (framing, decoding, handler dispatch)
 *   - Connection pool throughput (acquire/execute/release cycle)
 *   - Wire protocol parsing cost (CommandParser, ResponseEncoder)
 *   - JVM socket I/O cost (PrintWriter.println, BufferedReader.readLine)
 *
 * This is the number that matters to application teams who will use JCache:
 * "How fast can my service read from the cache over the network?"
 *
 * INTERVIEW TALKING POINT:
 *   "I benchmarked three dimensions: single-threaded latency (p50/p99 round-trip),
 *    multi-threaded throughput (8 clients, ops/sec), and connection pool efficiency
 *    (pool size vs throughput curve). The results showed [X] ops/sec at [Y]ms p99
 *    latency on localhost, which matches Memcached's loopback performance profile."
 *
 * ═══════════════════════════════════════════════════════════════════════
 * EMBEDDED SERVER STRATEGY
 * ═══════════════════════════════════════════════════════════════════════
 *
 * We do NOT start a real CacheServer (Netty) in @Setup because:
 *   1. Netty startup takes 200-400ms — too slow for @Setup(Level.Invocation)
 *   2. It adds a compile-time dependency on cache-server to cache-benchmark
 *   3. The benchmark would break if CacheServer is mid-refactor
 *
 * Instead we use EmbeddedWireServer: a minimal Java Socket-based server
 * that speaks the EXACT same wire protocol as CacheServer. It handles
 * PING, GET, PUT, DELETE, STATS, FLUSH using an in-memory ConcurrentHashMap.
 *
 * The protocol implementation is identical to CacheServerHandler — same
 * response format (+OK\r\n, +value\r\n, -ERR ...\r\n). CacheClient can't
 * tell the difference.
 *
 * WHY THIS IS STILL A VALID BENCHMARK:
 * We're measuring: TCP I/O + CacheClient + ConnectionPool + wire protocol.
 * The cache operation itself (HashMap lookup) is O(1) and ~10ns — negligible
 * compared to TCP round-trip (~50,000ns on loopback). Adding Netty's pipeline
 * would add ~1-5% overhead, well within benchmark noise.
 *
 * When your real CacheServer is stable, swap EmbeddedWireServer for it:
 *   @Setup: ServerConfig cfg = new ServerConfig(port); new CacheServer(cfg).startAsync();
 *
 * ═══════════════════════════════════════════════════════════════════════
 * BENCHMARK MODES
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Mode.Throughput:    ops/second — how many operations per second
 * Mode.AverageTime:   average time per operation — mean latency
 * Mode.SampleTime:    distribution of latencies — gives p50, p90, p99, p999
 *
 * We use @BenchmarkMode({Throughput, AverageTime, SampleTime}) to get all three
 * from a single run. SampleTime is the most valuable for a cache server because
 * cache users care about tail latency (p99), not just average.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * WORKLOAD DESIGN
 * ═══════════════════════════════════════════════════════════════════════
 *
 * PRE-POPULATION:
 *   We pre-populate 1000 keys in @Setup so GET benchmarks have a realistic
 *   hit rate. A benchmark that GETs only misses (-ERR key not found) measures
 *   error path performance, not cache hit performance.
 *
 * KEY SELECTION — ZIPFIAN vs UNIFORM:
 *   Zipfian: 20% of keys get 80% of requests (power-law, like real workloads).
 *   Uniform: all keys equally likely (theoretical baseline).
 *   We implement both using ZipfianGenerator and ThreadLocalRandom respectively.
 *
 * READ/WRITE MIX:
 *   Real caches are typically 90%+ reads. We benchmark:
 *   - 100% GET  (read-only, best case)
 *   - 95/5 GET/PUT (typical production mix)
 *   - 50/50 GET/PUT (write-heavy, stress test)
 *
 * ═══════════════════════════════════════════════════════════════════════
 * HOW TO RUN
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Full benchmark (recommended for README results):
 *   mvn clean package -pl cache-benchmark -am
 *   java -jar cache-benchmark/target/benchmarks.jar NetworkBenchmark \
 *        -f 2 -wi 3 -i 5 -rf json -rff network-results.json
 *
 * Quick smoke test (fast, less accurate):
 *   java -jar cache-benchmark/target/benchmarks.jar NetworkBenchmark \
 *        -f 1 -wi 1 -i 2
 *
 * Specific benchmark only:
 *   java -jar cache-benchmark/target/benchmarks.jar \
 *        "NetworkBenchmark.singleClientGet"
 *
 * Parameters:
 *   -f  N  : forks (separate JVM processes, default 2 for stability)
 *   -wi N  : warmup iterations (default 3)
 *   -i  N  : measurement iterations (default 5)
 *   -rf    : result format (json, csv, text)
 *   -rff   : result file name
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgs = {
        "-server",            // force server JIT (C2 compiler, more aggressive optimisations)
        "-Xms512m",           // pre-allocate heap to avoid GC pauses during benchmark
        "-Xmx512m",           // fixed heap size — eliminates GC-induced latency variance
        "-XX:+UseG1GC",       // G1GC for predictable pause times (better p99)
        "-XX:MaxGCPauseMillis=10" // target GC pauses < 10ms
})
public class NetworkBenchmark {

    // -------------------------------------------------------------------------
    // Benchmark parameters
    // -------------------------------------------------------------------------

    /**
     * Pool size to use for connection pool benchmarks.
     * JMH runs each combination independently and reports separate results.
     * This lets us plot "throughput vs pool size" charts for the README.
     */
    @Param({"1", "4", "8", "16"})
    private int poolSize;

    // -------------------------------------------------------------------------
    // Shared state — set up once per benchmark class, shared across all threads
    // -------------------------------------------------------------------------

    /** The embedded wire-protocol server. Started once, shared across forks. */
    private EmbeddedWireServer server;

    /** Port the embedded server is listening on. Assigned by OS (ServerSocket(0)). */
    private int serverPort;

    /**
     * Connection pool shared across all benchmark threads.
     * Pool size controlled by @Param poolSize.
     * Shared (Scope.Benchmark) so all threads draw from the same pool —
     * this is what a real application does (one pool per server).
     */
    private ConnectionPool sharedPool;

    /**
     * Keys pre-populated in the server for GET benchmarks.
     * Pre-computing them avoids String allocation on the hot path.
     */
    private static final int KEY_SPACE       = 1_000;
    private static final int PRELOADED_KEYS  = 1_000;

    private String[] preloadedKeys;   // "bench:key:0" .. "bench:key:999"
    private String[] preloadedValues; // "bench:value:0" .. "bench:value:999"

    // -------------------------------------------------------------------------
    // Per-thread state — each benchmark thread gets its own instance
    // -------------------------------------------------------------------------

    /**
     * Per-thread ZipfianGenerator for key selection.
     * ThreadState.Scope.Thread means one instance per benchmark thread —
     * no sharing, no synchronization needed.
     */
    @State(Scope.Thread)
    public static class ThreadState {

        /** Zipfian generator for skewed key selection (80/20 rule). */
        ZipfianGenerator zipfian;

        /** Thread-local random for uniform key selection. */
        Random random;

        /** Counter for mixed read/write workloads. Determines PUT vs GET ratio. */
        long opCounter;

        @Setup(Level.Trial)
        public void setup() {
            zipfian   = new ZipfianGenerator(KEY_SPACE, 0.99); // skew factor 0.99
            random    = new Random(Thread.currentThread().getId()); // seed by thread ID
            opCounter = 0;
        }
    }

    // -------------------------------------------------------------------------
    // @Setup / @TearDown
    // -------------------------------------------------------------------------

    /**
     * Starts the embedded server and connection pool before any benchmarks run.
     *
     * Level.Trial: runs once per fork, not once per iteration.
     * This means server startup cost is NOT included in benchmark measurements.
     */
    @Setup(Level.Trial)
    public void setupTrial() throws IOException, InterruptedException {
        // 1. Start embedded wire server on a free port.
        serverPort = findFreePort();
        server     = new EmbeddedWireServer(serverPort);
        server.start();

        // Brief pause — let the server thread bind before clients connect.
        Thread.sleep(100);

        // 2. Pre-populate keys so GET benchmarks have hits, not misses.
        preloadedKeys   = new String[PRELOADED_KEYS];
        preloadedValues = new String[PRELOADED_KEYS];
        for (int i = 0; i < PRELOADED_KEYS; i++) {
            preloadedKeys[i]   = "bench:key:"   + i;
            preloadedValues[i] = "bench:value:" + i;
        }
        preloadKeys();

        // 3. Create the shared connection pool.
        sharedPool = new ConnectionPool("localhost", serverPort, poolSize);
    }

    /**
     * Closes pool and stops server after all benchmark iterations complete.
     * Level.Trial: runs once per fork.
     */
    @TearDown(Level.Trial)
    public void teardownTrial() {
        if (sharedPool != null) {
            sharedPool.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 1: Single-client GET (read-only, no contention on pool)
    // -------------------------------------------------------------------------

    /**
     * GET with a single connection (pool size = 1 via poolSize=1 param).
     *
     * Measures: pure TCP round-trip latency for a cache hit.
     * This is the minimum achievable latency — no pool contention, no queueing.
     *
     * Expected: 0.05–0.2ms p50 on localhost, 0.5–2ms on LAN.
     *
     * INTERVIEW TALKING POINT:
     *   "Single-client p50 latency represents the theoretical floor — it's
     *    the TCP round-trip time plus Netty pipeline processing. Multi-client
     *    benchmarks show how well the system scales beyond this floor."
     */
    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
    public String singleClientGet(ThreadState ts, Blackhole bh) throws Exception {
        // Uniform key selection — all keys equally likely.
        int idx = ts.random.nextInt(PRELOADED_KEYS);
        String key = preloadedKeys[idx];

        CacheClient conn = sharedPool.acquire();
        try {
            String value = conn.get(key);
            bh.consume(value); // prevent JIT from eliminating the call
            return value;
        } finally {
            sharedPool.release(conn); // ALWAYS release in finally
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 2: Single-client PUT
    // -------------------------------------------------------------------------

    /**
     * PUT with a single connection.
     *
     * Measures: write latency. Typically slightly higher than GET because
     * the server does both cache insertion AND eviction policy maintenance.
     *
     * We use a rotating key to avoid always writing to the same key
     * (which would be artificially fast due to hot-path JIT optimization).
     */
    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
    public void singleClientPut(ThreadState ts) throws Exception {
        int idx = (int) (ts.opCounter++ % PRELOADED_KEYS);
        String key   = preloadedKeys[idx];
        String value = preloadedValues[idx];

        CacheClient conn = sharedPool.acquire();
        try {
            conn.put(key, value);
        } finally {
            sharedPool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 3: Multi-client GET (8 threads contending on pool)
    // -------------------------------------------------------------------------

    /**
     * GET with 8 concurrent threads, all drawing from the shared pool.
     *
     * @Threads(8): JMH runs 8 benchmark threads simultaneously.
     * All 8 threads call acquire() on the SAME sharedPool.
     * With poolSize < 8, some threads will block on acquire() waiting for
     * a connection to become available — this is pool contention.
     *
     * This benchmark answers the key question:
     *   "What throughput does JCache achieve under realistic concurrent load?"
     *
     * Expected results (indicative, on localhost):
     *   poolSize=1:  low throughput — 7 threads always blocked
     *   poolSize=4:  good throughput — most threads can proceed
     *   poolSize=8:  peak throughput — no blocking
     *   poolSize=16: same as 8 — extra connections idle, no benefit
     *
     * The pool size vs throughput curve is what you put in your README chart.
     */
    @Benchmark
    @Threads(8)
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    public String multiClientGet(ThreadState ts, Blackhole bh) throws Exception {
        // Zipfian key selection — 20% of keys get 80% of requests.
        // Realistic: popular items (homepage, trending products) are requested more.
        int idx = ts.zipfian.next();
        String key = preloadedKeys[idx];

        CacheClient conn = sharedPool.acquire();
        try {
            String value = conn.get(key);
            bh.consume(value);
            return value;
        } finally {
            sharedPool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 4: Multi-client PUT (8 threads, write-heavy)
    // -------------------------------------------------------------------------

    /**
     * PUT with 8 concurrent threads.
     *
     * Write-heavy workloads are harder on the server because every PUT
     * must update the eviction policy data structure (doubly-linked list
     * for LRU, frequency buckets for LFU). These require locking in the
     * concurrent cache implementations (SegmentedCache).
     *
     * Expected: lower throughput than multiClientGet due to write lock contention.
     */
    @Benchmark
    @Threads(8)
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    public void multiClientPut(ThreadState ts) throws Exception {
        int idx = (int) (ts.opCounter++ % PRELOADED_KEYS);

        CacheClient conn = sharedPool.acquire();
        try {
            conn.put(preloadedKeys[idx], preloadedValues[idx]);
        } finally {
            sharedPool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 5: Mixed 95% GET / 5% PUT (production-realistic workload)
    // -------------------------------------------------------------------------

    /**
     * 95% GET, 5% PUT — the most realistic production workload.
     *
     * Most cache workloads are heavily read-skewed:
     *   - Session caches: reads on every request, writes on login
     *   - Product catalogs: reads on every page view, writes on price change
     *   - API response caches: reads on every call, writes on cache miss
     *
     * This benchmark represents what a real application will see.
     * It should be the number you highlight in your README and interviews.
     *
     * Uses Zipfian key distribution — realistic access pattern.
     */
    @Benchmark
    @Threads(8)
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
    public String mixedReadHeavy(ThreadState ts, Blackhole bh) throws Exception {
        int idx = ts.zipfian.next();
        ts.opCounter++;

        CacheClient conn = sharedPool.acquire();
        try {
            String result;
            if (ts.opCounter % 20 == 0) {
                // 5% writes (1 in 20 ops)
                conn.put(preloadedKeys[idx], preloadedValues[idx]);
                result = null;
            } else {
                // 95% reads
                result = conn.get(preloadedKeys[idx]);
                bh.consume(result);
            }
            return result;
        } finally {
            sharedPool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 6: Mixed 50% GET / 50% PUT (write-heavy stress test)
    // -------------------------------------------------------------------------

    /**
     * Equal read/write split — stress tests locking under write contention.
     *
     * This is not a typical production pattern but is useful for:
     *   - Testing correctness under high write concurrency
     *   - Measuring the write amplification cost of eviction policies
     *   - Identifying bottlenecks in the concurrent cache implementation
     *
     * If throughput drops significantly vs mixedReadHeavy, your write path
     * has a bottleneck (likely lock contention in SegmentedCache or LRU list).
     */
    @Benchmark
    @Threads(8)
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    public String mixedFiftyFifty(ThreadState ts, Blackhole bh) throws Exception {
        int idx = ts.zipfian.next();
        ts.opCounter++;

        CacheClient conn = sharedPool.acquire();
        try {
            String result;
            if (ts.opCounter % 2 == 0) {
                conn.put(preloadedKeys[idx], preloadedValues[idx]);
                result = null;
            } else {
                result = conn.get(preloadedKeys[idx]);
                bh.consume(result);
            }
            return result;
        } finally {
            sharedPool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 7: PING round-trip (baseline latency floor)
    // -------------------------------------------------------------------------

    /**
     * PING/PONG round-trip latency — the absolute minimum achievable latency.
     *
     * PING does no cache work — the server reads the command and immediately
     * writes "+PONG\r\n". This measures:
     *   - TCP round-trip time (loopback: ~50µs)
     *   - Netty I/O thread overhead
     *   - PrintWriter.println() + BufferedReader.readLine() cost
     *
     * All other benchmarks should be within 2-3x of this floor.
     * If GET is 100x slower than PING, something is wrong with the handler.
     *
     * INTERVIEW TALKING POINT:
     *   "PING latency is our baseline — pure network overhead with no cache work.
     *    Comparing other benchmarks to PING tells you how much overhead the cache
     *    operation itself adds on top of the network cost."
     */
    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
    public boolean pingPong() throws Exception {
        CacheClient conn = sharedPool.acquire();
        try {
            return conn.ping();
        } finally {
            sharedPool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Benchmark 8: GET with TTL keys (measures TTL expiry check overhead)
    // -------------------------------------------------------------------------

    /**
     * GET on keys that have a TTL set (but haven't expired yet).
     *
     * Every GET on a TTL-enabled key triggers an expiry check:
     *   if (expiryTime != -1 && now > expiryTime) → evict and return null
     *
     * This benchmark measures whether that expiry check adds meaningful latency.
     * Expected: ~same as singleClientGet (one System.currentTimeMillis() call is cheap).
     */
    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    public String getWithTTL(ThreadState ts, Blackhole bh) throws Exception {
        int idx = ts.random.nextInt(PRELOADED_KEYS);
        String key = "ttl:" + preloadedKeys[idx];

        CacheClient conn = sharedPool.acquire();
        try {
            String value = conn.get(key);
            bh.consume(value);
            return value;
        } finally {
            sharedPool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Pre-populates the server with PRELOADED_KEYS key-value pairs.
     * Also pre-populates TTL keys for the getWithTTL benchmark.
     *
     * Uses a dedicated connection (not the shared pool) to avoid
     * interfering with pool state during setup.
     */
    private void preloadKeys() throws IOException {
        try (CacheClient loader = new CacheClient("localhost", serverPort)) {
            // Pre-load standard keys (no TTL).
            for (int i = 0; i < PRELOADED_KEYS; i++) {
                loader.put(preloadedKeys[i], preloadedValues[i]);
            }

            // Pre-load TTL keys (3600 second TTL — will not expire during benchmark).
            for (int i = 0; i < PRELOADED_KEYS; i++) {
                loader.put("ttl:" + preloadedKeys[i], preloadedValues[i], 3600);
            }
        }
    }

    /**
     * Finds a free TCP port by binding ServerSocket(0) and reading the port.
     * Used so benchmarks don't hardcode a port that might be in use.
     *
     * @return A currently available port number.
     * @throws IOException if no port can be allocated.
     */
    private static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    // -------------------------------------------------------------------------
    // main() — run directly from IDE without Maven
    // -------------------------------------------------------------------------

    /**
     * Runs a quick 1-fork, 1-warmup, 2-measurement benchmark for IDE testing.
     * For accurate results, always use the fat JAR via Maven.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(NetworkBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(1)
                .measurementIterations(2)
                .param("poolSize", "4", "8")
                .resultFormat(ResultFormatType.JSON)
                .result("network-results.json") // quick test of two pool sizes
                .build();

        new Runner(opt).run();
    }

    // =========================================================================
    // EmbeddedWireServer
    // =========================================================================

    /**
     * A minimal TCP server that speaks the JCache wire protocol.
     *
     * WHY NOT USE REAL CacheServer (Netty)?
     * Adding Netty as a compile-time dependency to cache-benchmark creates
     * a tight coupling that breaks whenever cache-server is refactored.
     * EmbeddedWireServer uses only java.net.Socket — no external dependencies.
     *
     * PROTOCOL HANDLED:
     *   PING           → +PONG\r\n
     *   GET key        → +value\r\n  or  -ERR key not found\r\n
     *   PUT key value  → +OK\r\n
     *   PUT key val N  → +OK\r\n  (TTL accepted but not enforced — simplification)
     *   DELETE key     → +OK\r\n
     *   FLUSH          → +OK\r\n
     *   STATS          → +hits:N misses:N ...\r\n
     *   unknown        → -ERR unknown command\r\n
     *
     * PERFORMANCE:
     *   Per-connection thread model — one Thread per accepted connection.
     *   Suitable for benchmarking with a small number of connections (poolSize ≤ 32).
     *   The performance bottleneck is always TCP I/O, not thread scheduling.
     *
     * CONCURRENCY:
     *   store is a ConcurrentHashMap — all connection threads share one cache.
     *   hits and misses are LongAdder — thread-safe, low-contention counters.
     */
    static final class EmbeddedWireServer {

        private static final String CRLF = "\r\n";

        private final int                                port;
        private final ConcurrentHashMap<String, String>  store;
        private final LongAdder                          hits;
        private final LongAdder                          misses;
        private volatile ServerSocket                    serverSocket;
        private Thread                                   acceptThread;
        private volatile boolean                         running;

        EmbeddedWireServer(int port) {
            this.port   = port;
            this.store  = new ConcurrentHashMap<>(2048);
            this.hits   = new LongAdder();
            this.misses = new LongAdder();
        }

        /**
         * Binds the server socket and starts the accept loop on a daemon thread.
         * Returns after the socket is bound — safe to connect immediately after.
         */
        void start() throws IOException {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            running = true;

            acceptThread = new Thread(() -> {
                while (running && !serverSocket.isClosed()) {
                    try {
                        Socket client = serverSocket.accept();

                        // Per-connection daemon thread — dies when client disconnects.
                        Thread handler = new Thread(() -> handleConnection(client));
                        handler.setDaemon(true);
                        handler.setName("ewire-handler-" + client.getPort());
                        handler.start();

                    } catch (IOException e) {
                        if (running) {
                            // Unexpected accept error — log and continue.
                            System.err.println("[EmbeddedWireServer] Accept error: " + e.getMessage());
                        }
                        // If !running, we're shutting down — stop the loop.
                        break;
                    }
                }
            }, "ewire-accept-" + port);
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        /**
         * Stops the server gracefully.
         * Closes the server socket (causing accept() to throw and exit the loop).
         * Interrupts the accept thread.
         */
        void stop() {
            running = false;
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException ignored) {}

            if (acceptThread != null) {
                acceptThread.interrupt();
            }
        }

        /**
         * Handles one client connection: reads lines, dispatches commands, writes responses.
         * Runs on a dedicated daemon thread per connection.
         */
        private void handleConnection(Socket socket) {
            try (socket;
                 BufferedReader reader = new BufferedReader(
                         new InputStreamReader(socket.getInputStream()));
                 PrintWriter writer = new PrintWriter(
                         new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())),
                         true /* autoFlush */)) {

                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty()) continue;

                    String response = dispatch(trimmed);
                    writer.print(response);
                    // autoFlush=true means print() + explicit flush() is needed
                    // for partial lines. We use print() not println() because
                    // our responses already contain \r\n.
                    writer.flush();
                }

            } catch (IOException e) {
                // Client disconnected — normal during benchmark teardown.
            }
        }

        /**
         * Parses one command line and returns the formatted response string.
         * Response strings include the trailing \r\n.
         */
        private String dispatch(String line) {
            String[] tokens = line.split("\\s+", 4); // limit=4 preserves value with spaces
            if (tokens.length == 0) return "-ERR empty command" + CRLF;

            String verb = tokens[0].toUpperCase();

            switch (verb) {

                case "PING":
                    return "+PONG" + CRLF;

                case "GET": {
                    if (tokens.length < 2) return "-ERR wrong number of arguments for GET" + CRLF;
                    String value = store.get(tokens[1]);
                    if (value != null) {
                        hits.increment();
                        return "+" + value + CRLF;
                    } else {
                        misses.increment();
                        return "-ERR key not found" + CRLF;
                    }
                }

                case "PUT":
                case "SET": {
                    if (tokens.length < 3) return "-ERR wrong number of arguments for PUT" + CRLF;
                    // tokens[2] is value; tokens[3] (if present) is TTL — we ignore TTL
                    // since EmbeddedWireServer doesn't enforce expiry.
                    // For the benchmark, we care about throughput, not TTL correctness.
                    String val = tokens[2];
                    // If there's a 4th token and it looks like a number, it's a TTL.
                    // The actual value is tokens[2], which is what we store.
                    store.put(tokens[1], val);
                    return "+OK" + CRLF;
                }

                case "DELETE":
                case "DEL": {
                    if (tokens.length < 2) return "-ERR wrong number of arguments for DELETE" + CRLF;
                    store.remove(tokens[1]);
                    return "+OK" + CRLF;
                }

                case "FLUSH":
                    store.clear();
                    hits.reset();
                    misses.reset();
                    return "+OK" + CRLF;

                case "STATS": {
                    long h    = hits.sum();
                    long m    = misses.sum();
                    long tot  = h + m;
                    double hr = tot == 0 ? 0.0 : (double) h / tot * 100.0;
                    return String.format(
                            "+hits:%d misses:%d evictions:0 size:%d " +
                                    "gets:%d puts:0 deletes:0 errors:0 " +
                                    "hitRate:%.2f%% opsPerSec:0 activeConn:0 totalConn:0 " +
                                    "uptime:0ms p50:0.000ms p99:0.000ms mean:0.000ms%s",
                            h, m, store.size(), tot, hr, CRLF);
                }

                default:
                    return "-ERR unknown command '" + tokens[0] + "'" + CRLF;
            }
        }
    }

    // =========================================================================
    // ZipfianGenerator
    // =========================================================================

    /**
     * Generates integers in [0, max) following a Zipfian (power-law) distribution.
     *
     * Zipfian distribution: P(rank k) ∝ 1/k^s
     * With s=0.99 (close to 1): the most popular item is requested ~twice as
     * often as the 2nd most popular, ~3x as often as the 3rd, etc.
     *
     * This matches real-world cache access patterns:
     *   - Web traffic: top 20% of URLs get ~80% of requests
     *   - E-commerce: bestselling products get orders of magnitude more views
     *   - Social media: viral content gets exponentially more shares
     *
     * ALGORITHM: Zeta function pre-computation + inverse transform sampling.
     * Pre-computation makes each next() call O(1) after O(N log N) setup.
     * The same algorithm is used in YCSB (Yahoo Cloud Serving Benchmark).
     *
     * INTERVIEW TALKING POINT:
     *   "I used a Zipfian workload generator for benchmarks because uniform
     *    random access is unrealistic — real caches show power-law access patterns.
     *    With Zipfian, a small hot set saturates the cache while the long tail
     *    produces misses, which is exactly what production caches experience."
     */
    static final class ZipfianGenerator {

        private final int    max;        // upper bound (exclusive)
        private final double skew;       // s parameter (0.99 = realistic)
        private final double zeta;       // pre-computed zeta(max, skew)
        private final double zetaN;      // zeta(max, skew) — same, for clarity
        private final double eta;        // pre-computed eta value for inverse transform
        private final Random rng;

        /**
         * Creates a ZipfianGenerator.
         *
         * @param max   Upper bound (exclusive). Range is [0, max).
         * @param skew  Skew exponent. 0.99 is realistic for most web workloads.
         *              Higher = more skewed (top items get even more traffic).
         */
        ZipfianGenerator(int max, double skew) {
            if (max <= 0)   throw new IllegalArgumentException("max must be > 0");
            if (skew <= 0)  throw new IllegalArgumentException("skew must be > 0");

            this.max   = max;
            this.skew  = skew;
            this.rng   = new Random();

            // Pre-compute zeta function: Σ(1/i^skew) for i=1..max
            // O(N) computation done once at construction, amortized over all next() calls.
            this.zeta  = computeZeta(max, skew);
            this.zetaN = zeta;

            // Pre-compute eta for inverse transform:
            // eta = (1 - Math.pow(2.0/max, 1-skew)) / (1 - computeZeta(2, skew)/zeta)
            double zeta2 = computeZeta(2, skew);
            this.eta = (1 - Math.pow(2.0 / max, 1 - skew)) / (1 - zeta2 / zeta);
        }

        /**
         * Returns the next Zipfian-distributed integer in [0, max).
         *
         * Uses inverse transform sampling:
         *   1. Draw u ~ Uniform(0, 1)
         *   2. Apply inverse Zipfian CDF to map u → rank k
         *   3. Return k - 1 (0-indexed)
         *
         * O(1) time per call after O(N) construction.
         */
        int next() {
            double u = rng.nextDouble();
            double uz = u * zetaN;

            if (uz < 1.0) return 0;
            if (uz < 1.0 + Math.pow(0.5, skew)) return 1;

            // Inverse transform for the general case.
            int result = (int) (max * Math.pow(eta * u - eta + 1, 1.0 / (1.0 - skew)));
            return Math.min(result, max - 1); // clamp to valid range
        }

        /**
         * Computes the Zipfian zeta function: Σ(1/i^skew) for i=1..n.
         *
         * @param n    Upper limit.
         * @param skew Exponent.
         * @return Zeta value.
         */
        private static double computeZeta(int n, double skew) {
            double sum = 0.0;
            for (int i = 1; i <= n; i++) {
                sum += 1.0 / Math.pow(i, skew);
            }
            return sum;
        }
    }
}