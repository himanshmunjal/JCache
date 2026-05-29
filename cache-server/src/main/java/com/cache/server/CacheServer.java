//package com.cache.server;
//
//import com.cache.api.Cache;
//import com.cache.concurrent.SegmentedCache;
//import com.cache.policy.ARCCache;
//import com.cache.policy.LFUCache;
//import com.cache.policy.LRUCache;
//import com.cache.server.handler.CacheServerHandler;
//import com.cache.server.metrics.ServerMetrics;
//import com.cache.ttl.TTLCache;
//import io.netty.bootstrap.ServerBootstrap;
//import io.netty.channel.*;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.nio.NioServerSocketChannel;
//import io.netty.handler.codec.LineBasedFrameDecoder;
//import io.netty.handler.codec.string.StringDecoder;
//import io.netty.handler.codec.string.StringEncoder;
//import io.netty.util.CharsetUtil;
//
//import java.net.InetSocketAddress;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.atomic.AtomicBoolean;
//
///**
// * CacheServer is the main entry point for the distributed cache server.
// *
// * It bootstraps a Netty TCP server that accepts client connections,
// * parses text commands (GET, PUT, DELETE, STATS, FLUSH, PING),
// * delegates to the cache-core engine, and returns responses.
// *
// * ARCHITECTURE OVERVIEW:
// *
// *   main()
// *     ↓
// *   CacheServer.start()
// *     ↓
// *   Netty ServerBootstrap
// *     ↓ binds to port
// *   NioEventLoopGroup (boss) ← accepts TCP connections
// *     ↓ hands channel to
// *   NioEventLoopGroup (workers) ← handles reads/writes
// *     ↓ per connection
// *   ChannelPipeline:
// *     LineBasedFrameDecoder   ← buffers bytes until \n, prevents TCP fragmentation
// *     StringDecoder           ← converts ByteBuf → String
// *     StringEncoder           ← converts String → ByteBuf for responses
// *     CacheServerHandler      ← YOUR CODE: parse command → call cache → write response
// *
// * NETTY THREADING MODEL:
// *   Boss thread:   1 thread, calls accept() in a loop, dispatches new channels to workers.
// *   Worker threads: N threads (default 2×CPU), each handles a fixed set of channels.
// *                   One worker thread handles many channels via Java NIO selectors.
// *                   No blocking I/O on worker threads — only non-blocking reads/writes.
// *
// *   Your CacheServerHandler.channelRead0() is called on a worker thread.
// *   The cache.get()/put() calls happen on that worker thread.
// *   This is why the cache must be thread-safe (we use SegmentedCache as the wrapper).
// *
// * WIRE PROTOCOL:
// *   Commands are newline-terminated ASCII strings. Responses start with + (success)
// *   or - (error). This is a simplified version of Redis's RESP protocol.
// *
// *   Client sends:          Server responds:
// *   PING                → +PONG
// *   GET key             → +value  or  -ERR key not found
// *   PUT key value       → +OK
// *   PUT key value 60    → +OK         (60 = TTL in seconds)
// *   DELETE key          → +OK
// *   STATS               → +hits:N misses:N evictions:N size:N
// *   FLUSH               → +OK         (clears all entries)
// *
// * GRACEFUL SHUTDOWN:
// *   A JVM shutdown hook calls stop() when Ctrl+C is pressed or kill is received.
// *   stop() calls shutdownGracefully() on both EventLoopGroups, which:
// *     1. Stops accepting new connections.
// *     2. Waits for in-flight requests to complete (quiet period = 100ms).
// *     3. Forces shutdown after timeout (2 seconds).
// *
// * USAGE:
// *   // Default config (port 6379, LRU, capacity 10k)
// *   CacheServer server = new CacheServer(ServerConfig.defaults());
// *   server.start();
// *
// *   // Custom config
// *   ServerConfig config = ServerConfig.builder()
// *       .port(7379)
// *       .evictionPolicy("ARC")
// *       .cacheCapacity(100_000)
// *       .build();
// *   CacheServer server = new CacheServer(config);
// *   server.start();       // blocks until server is shut down
// *
// *   // Programmatic shutdown (e.g., from tests)
// *   server.stop();
// */
//public class CacheServer {
//
//    // -------------------------------------------------------------------------
//    // Constants
//    // -------------------------------------------------------------------------
//
//    /**
//     * Maximum line length the server will accept from a client.
//     * Prevents malicious clients from sending arbitrarily large lines
//     * that would exhaust heap in LineBasedFrameDecoder's buffer.
//     *
//     * 8KB is enough for keys and values of reasonable size.
//     * If your use case needs larger values, increase this.
//     */
//    private static final int MAX_LINE_LENGTH = 8 * 1024; // 8KB
//
//    // -------------------------------------------------------------------------
//    // State
//    // -------------------------------------------------------------------------
//
//    /** Immutable configuration — read once at startup, never modified. */
//    private final ServerConfig config;
//
//    /**
//     * The cache engine — created once, shared across all client connections.
//     *
//     * WHY SEGMENTED WRAPPING THE POLICY?
//     * Multiple Netty worker threads call cache.get()/put() concurrently
//     * (one call per connected client, per received command).
//     * SegmentedCache provides fine-grained locking with low contention.
//     * The policy (LRU/LFU/ARC) is the delegate inside SegmentedCache.
//     *
//     * Alternative: CoarseGrainedCache is simpler but more contention.
//     * LockFreeCache has the best throughput but highest complexity.
//     * SegmentedCache is the right default for production.
//     */
//    private final Cache<String, String> cache;
//
//    /**
//     * Netty boss event loop — one thread, accepts TCP connections.
//     * Named "cache-server-boss" for easy identification in thread dumps.
//     */
//    private NioEventLoopGroup bossGroup;
//
//    /**
//     * Netty worker event loop — N threads, handles I/O on accepted channels.
//     * Named "cache-server-worker" for easy identification in thread dumps.
//     */
//    private NioEventLoopGroup workerGroup;
//
//    /**
//     * Channel representing the server socket.
//     * Stored so we can close it cleanly in stop().
//     */
//    private Channel serverChannel;
//
//    /**
//     * Guards against calling start() twice or stop() before start().
//     * AtomicBoolean ensures thread-safe state transitions.
//     */
//    private final AtomicBoolean running = new AtomicBoolean(false);
//
//    /**
//     * Latch released when the server has fully started and is accepting connections.
//     * Tests and programmatic callers can await this instead of sleeping.
//     */
//    private final CountDownLatch startLatch = new CountDownLatch(1);
//    private ServerMetrics metrics = new ServerMetrics();
//
//    // -------------------------------------------------------------------------
//    // Constructor
//    // -------------------------------------------------------------------------
//
//    /**
//     * Creates a CacheServer with the given configuration.
//     * Does NOT start the server — call start() for that.
//     *
//     * @param config Server configuration. Must not be null.
//     */
//    public CacheServer(ServerConfig config) {
//        if (config == null) {
//            throw new IllegalArgumentException("ServerConfig cannot be null");
//        }
//        this.config = config;
//        this.metrics = metrics;
//        this.cache  = buildCache(config);
//    }
//
//    // -------------------------------------------------------------------------
//    // Lifecycle methods
//    // -------------------------------------------------------------------------
//
//    /**
//     * Starts the Netty server and blocks until shutdown.
//     *
//     * This method:
//     *   1. Creates boss and worker EventLoopGroups.
//     *   2. Configures ServerBootstrap with the channel pipeline.
//     *   3. Binds to the configured port.
//     *   4. Registers a JVM shutdown hook for graceful shutdown.
//     *   5. Blocks on channel.closeFuture() until the server is stopped.
//     *
//     * BLOCKING vs NON-BLOCKING:
//     *   This method BLOCKS the calling thread until the server stops.
//     *   This is the standard pattern for a server main() method.
//     *   For tests that need to start/stop programmatically, call
//     *   startAsync() instead (starts in a background thread).
//     *
//     * @throws InterruptedException if the calling thread is interrupted while
//     *                              waiting for server startup or shutdown.
//     */
//    public void start() throws InterruptedException {
//        if (!running.compareAndSet(false, true)) {
//            throw new IllegalStateException("CacheServer is already running");
//        }
//
//        // Create event loop groups with named threads for easier debugging.
//        // Named threads show up as "cache-server-boss-1" in thread dumps and logs.
//        bossGroup   = new NioEventLoopGroup(config.getBossThreads(),
//                (ThreadFactory) r -> new Thread(r, "cache-server-boss"));
//        workerGroup = new NioEventLoopGroup(config.getWorkerThreads(),
//                (ThreadFactory) r -> new Thread(r, "cache-server-worker"));
//
//        try {
//            ServerBootstrap bootstrap = buildServerBootstrap();
//
//            // Bind to the port — this starts the server.
//            // sync() waits until the bind completes (or throws if port is in use).
//            ChannelFuture bindFuture = bootstrap.bind(config.getPort()).sync();
//
//            if (!bindFuture.isSuccess()) {
//                throw new RuntimeException(
//                        "Failed to bind to port " + config.getPort(),
//                        bindFuture.cause()
//                );
//            }
//
//            serverChannel = bindFuture.channel();
//
//            // Log startup information
//            logStartup();
//
//            // Register JVM shutdown hook AFTER successful bind.
//            // If bind fails, we don't want a shutdown hook calling stop()
//            // on an uninitialized server.
//            registerShutdownHook();
//
//            // Release the start latch — tests waiting on awaitStart() can proceed.
//            startLatch.countDown();
//
//            // Block here until the server channel is closed (by stop() or JVM shutdown).
//            // This is what keeps the main thread alive while the server runs.
//            serverChannel.closeFuture().sync();
//
//        } finally {
//            // Always clean up event loops, even if startup or runtime throws.
//            shutdown();
//        }
//    }
//
//    /**
//     * Starts the server in a background daemon thread.
//     * Returns after the server has successfully bound to its port.
//     *
//     * Use this from tests or embedded usage where you don't want to block
//     * the calling thread:
//     *
//     *   CacheServer server = new CacheServer(config);
//     *   server.startAsync();  // returns immediately
//     *   // ... run tests ...
//     *   server.stop();
//     *
//     * @throws InterruptedException if interrupted while waiting for startup.
//     */
//    public void startAsync() throws InterruptedException {
//        Thread serverThread = new Thread(() -> {
//            try {
//                start();
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        }, "cache-server-main");
//
//        // Daemon thread: JVM can exit even if this thread is still running.
//        // This prevents the server from keeping the test JVM alive after tests complete.
//        serverThread.setDaemon(true);
//        serverThread.start();
//
//        // Wait until the server has bound to the port before returning.
//        // Without this, callers might try to connect before the server is ready.
//        startLatch.await();
//    }
//
//    /**
//     * Stops the server gracefully.
//     *
//     * Graceful shutdown sequence:
//     *   1. Close the server socket (stop accepting new connections).
//     *   2. Allow in-flight requests to complete (quiet period = 100ms).
//     *   3. Force-close any remaining connections after timeout (2 seconds).
//     *   4. Shut down boss and worker EventLoopGroups.
//     *
//     * Calling stop() on an already-stopped server is a safe no-op.
//     */
//    public void stop() {
//        if (!running.compareAndSet(true, false)) {
//            return; // already stopped or never started
//        }
//
//        System.out.println("[CacheServer] Shutting down...");
//
//        // Close the server channel — stops accepting new connections.
//        if (serverChannel != null && serverChannel.isOpen()) {
//            serverChannel.close();
//        }
//
//        shutdown();
//
//        System.out.println("[CacheServer] Shutdown complete.");
//    }
//
//    /**
//     * Blocks the calling thread until the server has successfully bound
//     * to its port and is ready to accept connections.
//     *
//     * Use after startAsync() to ensure the server is ready before running tests.
//     *
//     * @throws InterruptedException if interrupted while waiting.
//     */
//    public void awaitStart() throws InterruptedException {
//        startLatch.await();
//    }
//
//    /**
//     * Returns true if the server is currently running and accepting connections.
//     *
//     * @return true if running, false otherwise.
//     */
//    public boolean isRunning() {
//        return running.get();
//    }
//
//    /**
//     * Returns the port this server is (or will be) listening on.
//     * Useful when the server was configured with port 0 (OS-assigned port)
//     * and you need to know the actual port.
//     *
//     * @return The configured port number.
//     */
//    public int getPort() {
//        return config.getPort();
//    }
//
//    // -------------------------------------------------------------------------
//    // Private helpers
//    // -------------------------------------------------------------------------
//
//    /**
//     * Builds the Netty ServerBootstrap with full pipeline configuration.
//     *
//     * PIPELINE EXPLANATION (handlers execute in order for inbound data):
//     *
//     *   1. LineBasedFrameDecoder:
//     *      TCP is a stream — bytes arrive in arbitrary chunks. This decoder
//     *      accumulates bytes in a buffer until it finds a \n or \r\n,
//     *      then emits exactly one complete line downstream.
//     *      Without this: "GET foo\nPUT b" might arrive as one read,
//     *      and "ar baz\n" as the next. LineBasedFrameDecoder handles this.
//     *      maxLineLength prevents memory exhaustion from malicious clients.
//     *
//     *   2. StringDecoder:
//     *      Converts the ByteBuf (raw bytes) emitted by LineBasedFrameDecoder
//     *      into a String using UTF-8 encoding.
//     *      After this handler, your CacheServerHandler receives Strings, not bytes.
//     *
//     *   3. StringEncoder:
//     *      Outbound handler (for responses, not inbound commands).
//     *      Converts String responses back to ByteBuf for sending over the network.
//     *      Must be added BEFORE CacheServerHandler in the pipeline because
//     *      pipeline handlers process outbound data in REVERSE order.
//     *
//     *   4. CacheServerHandler:
//     *      YOUR application logic. Receives a String command, dispatches to cache,
//     *      writes String response. Netty calls this on a worker thread.
//     *
//     * @return A configured ServerBootstrap ready to bind.
//     */
//    private ServerBootstrap buildServerBootstrap() {
//        // CacheServerHandler is NOT @ChannelHandler.Sharable by default.
//        // We pass the cache and config as constructor args so each channel
//        // gets its own handler instance with shared cache reference.
//        // This is the correct Netty pattern for stateful handlers.
//
//        return new ServerBootstrap()
//                .group(bossGroup, workerGroup)
//                .channel(NioServerSocketChannel.class)
//
//                // SO_BACKLOG: how many pending connections the OS will queue
//                // before refusing new ones. Set from config.
//                .option(ChannelOption.SO_BACKLOG, config.getBacklog())
//
//                // SO_REUSEADDR: allows rebinding to a port in TIME_WAIT state.
//                // Prevents "Address already in use" errors when restarting the server quickly.
//                .option(ChannelOption.SO_REUSEADDR, true)
//
//                // TCP_NODELAY: disables Nagle's algorithm.
//                // Nagle buffers small packets to reduce network overhead.
//                // For a cache server, we want responses sent immediately, not batched.
//                // Disabling Nagle reduces latency at the cost of slightly more packets.
//                .childOption(ChannelOption.TCP_NODELAY, true)
//
//                // SO_KEEPALIVE: OS-level heartbeat to detect dead connections.
//                // Cleans up connections from crashed clients without explicit disconnects.
//                .childOption(ChannelOption.SO_KEEPALIVE, true)
//
//                // Channel initializer: called for each new accepted connection.
//                // Creates a fresh ChannelPipeline for that connection.
//                .childHandler(new ChannelInitializer<SocketChannel>() {
//                    @Override
//                    protected void initChannel(SocketChannel ch) {
//                        ChannelPipeline pipeline = ch.pipeline();
//
//                        // INBOUND handlers (process data received from client):
//                        pipeline.addLast("frameDecoder",
//                                new LineBasedFrameDecoder(MAX_LINE_LENGTH));
//
//                        pipeline.addLast("stringDecoder",
//                                new StringDecoder(CharsetUtil.UTF_8));
//
//                        // OUTBOUND handler (processes data sent to client):
//                        // Must be added before CacheServerHandler so it's available
//                        // when CacheServerHandler calls ctx.writeAndFlush(String).
//                        pipeline.addLast("stringEncoder",
//                                new StringEncoder(CharsetUtil.UTF_8));
//
//                        // APPLICATION handler — your cache logic:
//                        pipeline.addLast("cacheHandler",
//                                new CacheServerHandler(cache, metrics ,config));
//                    }
//                });
//    }
//
//    /**
//     * Builds the cache engine based on the configured eviction policy.
//     *
//     * All policies are wrapped in SegmentedCache for thread safety.
//     * SegmentedCache splits the cache into N segments, each with its own lock,
//     * reducing contention compared to a single global lock.
//     *
//     * WHY WRAP WITH SEGMENTED INSTEAD OF USING POLICY DIRECTLY?
//     * LRUCache, LFUCache, and ARCCache are NOT thread-safe by themselves.
//     * SegmentedCache adds thread safety without modifying the policy implementations.
//     * This is the Open/Closed Principle — extend behavior without modifying existing code.
//     *
//     * @param config The ServerConfig specifying which policy to use.
//     * @return A thread-safe cache instance.
//     */
//    private static Cache<String, String> buildCache(ServerConfig config) {
//        int capacity = config.getCacheCapacity();
//
//        // Build the policy-specific delegate
//        Cache<String, String> delegate;
//        switch (config.getEvictionPolicy()) {
//            case "LFU":
//                delegate = new LFUCache<>(capacity);
//                break;
//            case "ARC":
//                delegate = new ARCCache<>(capacity);
//                break;
//            case "LRU":
//            default:
//                delegate = new LRUCache<>(capacity);
//                break;
//        }
//
//        delegate = new TTLCache<>(delegate);
//        // Wrap with segmented locking — 16 segments is a good default
//        // (covers up to 16 concurrent writers with zero contention).
////        return new SegmentedCache<>(capacity, 16);
//        return delegate;
//        // ALTERNATIVE: For maximum throughput in read-heavy workloads:
//        // return new LockFreeCache<>(capacity);
//        //
//        // ALTERNATIVE: For simplicity in development/testing:
//        // return new CoarseGrainedCache<>(delegate);
//    }
//
//    /**
//     * Shuts down both EventLoopGroups gracefully.
//     *
//     * shutdownGracefully() starts a quiet period (100ms by default) during which
//     * no new tasks are accepted, but in-flight tasks complete.
//     * After the quiet period, remaining threads are interrupted and terminated.
//     *
//     * We call sync() on the returned futures to wait for full shutdown before
//     * this method returns. This prevents resource leaks in tests.
//     */
//    public void shutdown() {
//        if (workerGroup != null) {
//            workerGroup.shutdownGracefully().syncUninterruptibly();
//        }
//        if (bossGroup != null) {
//            bossGroup.shutdownGracefully().syncUninterruptibly();
//        }
//    }
//
//    /**
//     * Registers a JVM shutdown hook that calls stop() on Ctrl+C or SIGTERM.
//     *
//     * Without this, the server process hangs on exit because Netty's
//     * EventLoopGroup threads are non-daemon and prevent JVM shutdown.
//     */
//    private void registerShutdownHook() {
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            System.out.println("[CacheServer] Shutdown signal received.");
//            stop();
//        }, "cache-server-shutdown-hook"));
//    }
//
//    /**
//     * Logs startup information to stdout.
//     * Shows port, policy, capacity, and thread counts so operators know
//     * what's running without reading config files.
//     */
//    private void logStartup() {
//        System.out.println("╔══════════════════════════════════════════╗");
//        System.out.println("║          JCache Server Started            ║");
//        System.out.println("╠══════════════════════════════════════════╣");
//        System.out.printf( "║  Port:            %-23d║%n", config.getPort());
//        System.out.printf( "║  Eviction Policy: %-23s║%n", config.getEvictionPolicy());
//        System.out.printf( "║  Cache Capacity:  %-23d║%n", config.getCacheCapacity());
//        System.out.printf( "║  Worker Threads:  %-23d║%n", config.getWorkerThreads());
//        System.out.printf( "║  Max Connections: %-23d║%n", config.getMaxConnections());
//        System.out.println("╚══════════════════════════════════════════╝");
//        System.out.println("Ready. Connect with: telnet localhost " + config.getPort());
//    }
//
//    // -------------------------------------------------------------------------
//    // main() — entry point when running as a standalone server
//    // -------------------------------------------------------------------------
//
//    /**
//     * Starts the cache server from the command line.
//     *
//     * Usage:
//     *   java -jar cache-server.jar                          # defaults
//     *   java -jar cache-server.jar 7379                     # custom port
//     *   java -jar cache-server.jar 7379 LFU 50000          # port + policy + capacity
//     *
//     * After starting, connect with:
//     *   telnet localhost 6379
//     *   > PING
//     *   +PONG
//     *   > PUT name Alice 60
//     *   +OK
//     *   > GET name
//     *   +Alice
//     *   > STATS
//     *   +hits:1 misses:0 evictions:0 size:1
//     */
//    public static void main(String[] args) throws InterruptedException {
//        ServerConfig config = parseArgs(args);
//
//        System.out.println("Starting with config: " + config);
//
//        CacheServer server = new CacheServer(config);
//        server.start(); // blocks until shutdown
//    }
//
//    /**
//     * Parses command-line arguments into a ServerConfig.
//     * Falls back to defaults for any unspecified arguments.
//     *
//     * @param args Command-line arguments.
//     * @return A ServerConfig based on the provided arguments.
//     */
//    private static ServerConfig parseArgs(String[] args) {
//        ServerConfig.Builder builder = ServerConfig.builder();
//
//        if (args.length >= 1) {
//            try {
//                builder.port(Integer.parseInt(args[0]));
//            } catch (NumberFormatException e) {
//                System.err.println("Invalid port: " + args[0] + ". Using default 6379.");
//            }
//        }
//
//        if (args.length >= 2) {
//            try {
//                builder.evictionPolicy(args[1]);
//            } catch (IllegalArgumentException e) {
//                System.err.println("Invalid policy: " + args[1] + ". Using default LRU.");
//            }
//        }
//
//        if (args.length >= 3) {
//            try {
//                builder.cacheCapacity(Integer.parseInt(args[2]));
//            } catch (NumberFormatException e) {
//                System.err.println("Invalid capacity: " + args[2] + ". Using default 10000.");
//            }
//        }
//
//        return builder.build();
//    }
//}

package com.cache.server;

import com.cache.api.Cache;
import com.cache.api.CachePolicyType;
import com.cache.concurrent.SegmentedCache;
import com.cache.server.handler.CacheServerHandler;
import com.cache.server.handler.ConnectionManager;
import com.cache.server.metrics.ServerMetrics;
import com.cache.ttl.TTLCache;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.util.concurrent.CountDownLatch;

/**
 * CacheServer is the entry point and lifecycle manager for the JCache network server.
 *
 * WHAT THIS CLASS DOES:
 *   1. Reads configuration (from ServerConfig — built programmatically or from a file).
 *   2. Builds the cache engine from cache-core (LRU/LFU/ARC + TTL wrapper).
 *   3. Bootstraps a Netty TCP server on the configured port.
 *   4. Wires the Netty pipeline: framing → decoding → connection management → command handling.
 *   5. Registers a JVM shutdown hook for graceful shutdown on Ctrl+C or SIGTERM.
 *   6. Blocks until shutdown is requested, then releases resources cleanly.
 *
 * NETTY ARCHITECTURE — TWO THREAD GROUPS:
 *
 *   BossGroup (1 thread by default):
 *     Runs the ServerSocketChannel. Accepts incoming TCP connections.
 *     For each accepted connection, registers the new SocketChannel
 *     with a worker thread and moves on. Very lightweight.
 *
 *   WorkerGroup (8 threads by default):
 *     Each worker thread runs an event loop (NIO Selector).
 *     Handles all I/O for channels assigned to it: reading bytes,
 *     running pipeline handlers, writing responses.
 *     A channel stays on its assigned worker thread for its entire lifetime
 *     — this is what makes Netty handlers safe to write without per-channel locking.
 *
 * PIPELINE FOR EACH ACCEPTED CONNECTION:
 *
 *   Inbound (client → server):
 *     LineBasedFrameDecoder   → buffers bytes until \n, emits one frame per line
 *     StringDecoder           → converts ByteBuf frame to java.lang.String (UTF-8)
 *     ConnectionManager       → enforces max connections, tracks active channels
 *     CacheServerHandler      → parses command, calls cache engine, writes response
 *
 *   Outbound (server → client):
 *     StringEncoder           → converts String response to ByteBuf (UTF-8)
 *     (then Netty writes ByteBuf bytes to the TCP socket)
 *
 *   Why LineBasedFrameDecoder?
 *     TCP is a stream protocol — there are no message boundaries in raw bytes.
 *     A client sending "GET foo\r\n" might arrive as two reads: "GET f" then "oo\r\n".
 *     LineBasedFrameDecoder buffers bytes until it sees \n, then emits the complete
 *     line as one ByteBuf. This is the same framing Redis uses for its inline protocol.
 *
 *   Why StringDecoder/StringEncoder?
 *     Our protocol is text-based. Converting ByteBuf<->String at the codec layer
 *     keeps CacheServerHandler clean — it works with plain Java Strings.
 *
 * GRACEFUL SHUTDOWN:
 *   We register a JVM shutdown hook (Runtime.addShutdownHook) that:
 *     1. Closes all active client connections via ConnectionManager.
 *     2. Shuts down the TTL sweeper thread.
 *     3. Calls shutdownGracefully() on both Netty event loop groups.
 *     4. Signals the main thread (which is blocking on serverChannel.closeFuture())
 *        to proceed past the await and exit.
 *
 *   Why shutdownGracefully() instead of shutdown()?
 *     shutdownGracefully() waits for in-flight tasks to complete before stopping
 *     the event loop. This prevents cutting off a response mid-write.
 *     It accepts a quiet period (default 2s) and timeout (default 15s).
 *     We use shorter values for faster shutdown in test environments.
 *
 * USAGE:
 *
 *   Programmatic (tests, embedding):
 *     ServerConfig config = ServerConfig.builder().port(6379).cacheCapacity(10000).build();
 *     CacheServer server = new CacheServer(config);
 *     server.start(); // blocks until shutdown
 *
 *   From command line:
 *     java -jar jcache.jar                          # all defaults
 *     java -jar jcache.jar --port 6380              # custom port
 *     java -jar jcache.jar --config jcache.properties  # properties file
 *
 *   With Docker:
 *     docker run -p 6379:6379 yourname/jcache:latest
 *
 *   Verify with telnet:
 *     telnet localhost 6379
 *     PUT name Alice
 *     GET name
 *     STATS
 *     QUIT
 */
public class CacheServer {

    // =========================================================================
    // Constants
    // =========================================================================

    /**
     * Maximum bytes per line from a client.
     * Prevents memory exhaustion if a misbehaving client sends a very long line
     * without a newline. 8KB is generous for key+value in a text protocol.
     */
    private static final int MAX_LINE_LENGTH = 8 * 1024; // 8KB

    /**
     * Quiet period for Netty graceful shutdown (milliseconds).
     * During this period, the event loop keeps running to process in-flight tasks.
     * Shorter than Netty default (2000ms) for faster shutdown in tests.
     */
    private static final long SHUTDOWN_QUIET_PERIOD_MS = 200L;

    /**
     * Timeout for Netty graceful shutdown (milliseconds).
     * If tasks are still running after this, shutdown is forced.
     */
    private static final long SHUTDOWN_TIMEOUT_MS = 3000L;

    // =========================================================================
    // Instance state
    // =========================================================================

    /** Configuration — immutable, shared safely across all threads. */
    private final ServerConfig config;

    /**
     * The cache engine — built from CacheFactory based on config.evictionPolicy.
     * Wrapped in TTLCache for TTL support and SegmentedCache for thread safety.
     * Shared across all CacheServerHandler instances (all client connections).
     * Must be thread-safe — SegmentedCache provides this.
     */
    private final Cache<String, String> cache;

    /**
     * Server-level metrics — shared across all handler instances.
     * Thread-safe via LongAdder and AtomicLong internally.
     */
    private final ServerMetrics metrics;

    /**
     * Tracks active connections and enforces maxConnections limit.
     * Sharable handler — one instance shared across all pipelines.
     */
    private final ConnectionManager connectionManager;

    /**
     * Netty boss event loop group — accepts new connections.
     * One thread is sufficient for any realistic workload.
     */
    private NioEventLoopGroup bossGroup;

    /**
     * Netty worker event loop group — handles I/O for accepted connections.
     * Number of threads set from ServerConfig.workerThreads.
     */
    private NioEventLoopGroup workerGroup;

    /**
     * The bound server channel — represents the listening socket.
     * We close this on shutdown to stop accepting new connections.
     */
    private Channel serverChannel;

    /**
     * The actual port the server is listening on.
     * Differs from config.getPort() when port 0 was configured —
     * in that case the OS assigns a free port and we read it here
     * from serverChannel.localAddress() after bind() completes.
     * Initialized to -1 before the server binds.
     */
    private volatile int actualPort = -1;

    /**
     * Latch used to signal the start() blocking call that shutdown is complete.
     * Initialized to 1 in start(), counted down in shutdown().
     * This lets us cleanly unblock start() from a shutdown hook thread.
     */
    private volatile CountDownLatch shutdownLatch;

    /**
     * Tracks whether the server is currently running.
     * Prevents double-start and makes shutdown idempotent.
     */
    private volatile boolean running = false;

    // =========================================================================
    // Constructors
    // =========================================================================

    /**
     * Creates a CacheServer with the given configuration.
     *
     * This constructor builds the cache engine and metrics immediately,
     * but does NOT bind to any port. Call start() to begin accepting connections.
     *
     * @param config Server configuration. Must not be null.
     */
    public CacheServer(ServerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ServerConfig cannot be null");
        }
        this.config  = config;
        this.cache   = buildCache(config);
        this.metrics = new ServerMetrics();
        this.connectionManager = new ConnectionManager(config.getMaxConnections(), metrics);
    }

    /**
     * Creates a CacheServer with default configuration.
     * Equivalent to new CacheServer(ServerConfig.defaults()).
     * Useful for quick-start and integration tests.
     */
    public CacheServer() {
        this(ServerConfig.defaults());
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /**
     * Starts the server and blocks until shutdown.
     *
     * This method:
     *   1. Creates Netty event loop groups.
     *   2. Configures the ServerBootstrap with our pipeline.
     *   3. Binds to the configured port.
     *   4. Registers a JVM shutdown hook.
     *   5. Blocks on serverChannel.closeFuture() until shutdown.
     *   6. Returns after all resources are released.
     *
     * To start the server without blocking, run this method in a separate thread:
     *   Thread serverThread = new Thread(server::start);
     *   serverThread.setDaemon(true);
     *   serverThread.start();
     *
     * @throws RuntimeException if the server fails to bind to the port.
     *         Common causes: port in use (EADDRINUSE), insufficient privileges
     *         for ports < 1024.
     */
    public void start() {
        if (running) {
            throw new IllegalStateException(
                    "Server is already running on port " + actualPort);
        }

        // Initialize shutdown latch — counted down in shutdown()
        shutdownLatch = new CountDownLatch(1);

        // Create event loop groups
        // NioEventLoopGroup uses Java NIO Selector — non-blocking I/O,
        // efficient for many concurrent connections with varying activity.
        bossGroup   = new NioEventLoopGroup(config.getBossThreads());
        workerGroup = new NioEventLoopGroup(config.getWorkerThreads());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap
                    .group(bossGroup, workerGroup)
                    // NioServerSocketChannel is the Netty abstraction over
                    // Java's ServerSocketChannel (non-blocking TCP server socket).
                    .channel(NioServerSocketChannel.class)

                    // Socket options for the server socket itself (listening socket):
                    // SO_BACKLOG: max length of the queue of pending connections.
                    // 128 is the Linux default; increase for very high connection rates.
                    .option(ChannelOption.SO_BACKLOG, 128)

                    // Socket options for accepted child channels (client connections):
                    // SO_KEEPALIVE: enables TCP keepalive probes. Detects dead connections
                    //   that closed without sending FIN (e.g., network partition, crash).
                    // TCP_NODELAY: disables Nagle's algorithm. Sends small packets immediately
                    //   instead of buffering for 200ms. Critical for low-latency cache ops.
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)

                    // ChannelInitializer is called once per accepted connection.
                    // It configures the pipeline for that specific channel, then
                    // removes itself from the pipeline (it's a one-shot initializer).
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            buildPipeline(ch.pipeline());
                        }
                    });

            // Bind and start accepting connections.
            // sync() blocks until the bind completes (or throws on failure).
            ChannelFuture bindFuture = bootstrap.bind(config.getPort()).sync();

            if (!bindFuture.isSuccess()) {
                throw new RuntimeException(
                        "Failed to bind to port " + config.getPort(),
                        bindFuture.cause()
                );
            }

            serverChannel = bindFuture.channel();

            // Read the actual bound port from the channel's local address.
            // When config.getPort() == 0, the OS assigns a port and this is
            // how we find out which one. When a specific port was configured,
            // this just confirms it. Either way, getPort() reads actualPort.
            actualPort = ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
            running    = true;

            printStartupBanner();
            registerShutdownHook();

            // Block here until the server channel is closed (by shutdown()).
            // This is the "server is running" state.
            serverChannel.closeFuture().sync();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[CacheServer] Server interrupted during startup or operation");
        } finally {
            // These run after serverChannel.closeFuture() unblocks —
            // i.e., after shutdown() has closed the server channel.
            performCleanup();
            shutdownLatch.countDown(); // signal that cleanup is complete
        }
    }

    /**
     * Shuts down the server gracefully.
     *
     * Safe to call from any thread, including the JVM shutdown hook thread.
     * Idempotent — calling shutdown() on an already-stopped server is a no-op.
     *
     * Shutdown sequence:
     *   1. Stop accepting new connections (close server channel).
     *   2. Close all existing client connections.
     *   3. Shut down TTL sweeper if cache is a TTLCache.
     *   4. Shut down Netty event loop groups.
     *
     * The start() method unblocks and returns after this method completes.
     */
    public void shutdown() {
        if (!running) {
            return; // already stopped or never started
        }

        running = false;
        System.out.println("[CacheServer] Shutting down...");

        // Step 1: Stop accepting new connections
        if (serverChannel != null && serverChannel.isOpen()) {
            serverChannel.close();
        }

        // Step 2: Close all existing client connections cleanly
        connectionManager.closeAllConnections();

        // Step 3: Shut down TTL sweeper if applicable
        // TTLCache.shutdown() stops the background sweeper thread.
        // Without this, the sweeper thread may delay JVM exit.
        if (cache instanceof TTLCache) {
            ((TTLCache<String, String>) cache).shutdown();
            System.out.println("[CacheServer] TTL sweeper stopped");
        }

        System.out.println("[CacheServer] Shutdown complete");
    }

    /**
     * Waits for the server to fully start (i.e., be bound and accepting connections).
     * Useful in tests that start the server in a background thread and need to
     * wait before sending commands.
     *
     * @return true if the server is running, false if it never started.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns the port the server is actually listening on.
     *
     * When port 0 was configured, the OS assigns a free ephemeral port.
     * This method returns that actual port — NOT the configured 0.
     * Always use this method in tests, never config.getPort().
     *
     * Returns -1 if the server has not yet bound (i.e., start() or
     * startAsync() has not been called yet).
     *
     * @return Actual bound port, or -1 if not yet started.
     */
    public int getPort() {
        return actualPort;
    }

    /**
     * Starts the server in a background daemon thread and returns immediately.
     *
     * This is what tests use — @BeforeAll cannot block forever waiting for
     * start() to return, because start() only returns on shutdown.
     *
     * This method blocks until the server is fully bound and ready to accept
     * connections (up to timeoutMs milliseconds), then returns.
     * If the server fails to start within the timeout, it throws RuntimeException.
     *
     * Usage in tests:
     *   server.startAsync();                   // returns as soon as server is bound
     *   int port = server.getPort();           // safe to call — server is ready
     *   CacheClient client = new CacheClient("localhost", port);
     *
     * @param timeoutMs Maximum milliseconds to wait for the server to bind.
     * @throws RuntimeException if the server does not start within timeoutMs.
     */
    public void startAsync(long timeoutMs) {
        Thread serverThread = new Thread(this::start, "jcache-server-main");
        serverThread.setDaemon(true); // won't block JVM exit if tests finish
        serverThread.start();

        // Poll until running == true (actualPort is set) or timeout elapses.
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!running && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for server to start");
            }
        }

        if (!running) {
            throw new RuntimeException(
                    "Server did not start within " + timeoutMs + "ms"
            );
        }
    }

    /**
     * Starts the server asynchronously with a default 5-second timeout.
     * Equivalent to startAsync(5000).
     */
    public void startAsync() {
        startAsync(5000);
    }

    /**
     * Returns the ServerMetrics instance for this server.
     * Used by tests to assert on hit counts, error counts, etc.
     *
     * @return The metrics collector.
     */
    public ServerMetrics getMetrics() {
        return metrics;
    }

    /**
     * Returns the ConnectionManager.
     * Used by tests to assert on active connection counts.
     *
     * @return The connection manager.
     */
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    // =========================================================================
    // main() — command-line entry point
    // =========================================================================

    /**
     * Command-line entry point.
     *
     * Supported arguments:
     *   (no args)                    → start with all defaults
     *   --port <number>              → override port
     *   --config <path>              → load config from properties file
     *   --capacity <number>          → override cache capacity
     *   --policy <LRU|LFU|ARC|FIFO> → override eviction policy
     *   --verbose                    → enable verbose logging
     *
     * Examples:
     *   java -jar jcache.jar
     *   java -jar jcache.jar --port 6380 --capacity 50000
     *   java -jar jcache.jar --config /etc/jcache/jcache.properties
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {
        ServerConfig config = parseArgs(args);
        CacheServer server  = new CacheServer(config);
        server.start(); // blocks until shutdown
    }

    // =========================================================================
    // Private — pipeline construction
    // =========================================================================

    /**
     * Configures the Netty pipeline for a newly accepted client connection.
     *
     * Handler order matters. Inbound handlers fire top-to-bottom.
     * Outbound handlers (StringEncoder) fire bottom-to-top.
     *
     * Pipeline (inbound direction, top to bottom):
     *   1. LineBasedFrameDecoder — splits byte stream into lines
     *   2. StringDecoder         — converts ByteBuf to String
     *   3. StringEncoder         — converts String back to ByteBuf (outbound)
     *   4. ConnectionManager     — enforces limits, tracks channels
     *   5. CacheServerHandler    — command dispatch, cache calls, response
     *
     * We create a new CacheServerHandler per connection (not sharable).
     * ConnectionManager is sharable — one instance for the whole server.
     *
     * @param pipeline The pipeline of the newly accepted SocketChannel.
     */
    private void buildPipeline(ChannelPipeline pipeline) {
        // Framing: split TCP byte stream into lines.
        // MAX_LINE_LENGTH prevents memory exhaustion on runaway clients.
        // stripDelimiter=true removes the \n from the emitted ByteBuf.
        pipeline.addLast("framer",
                new LineBasedFrameDecoder(MAX_LINE_LENGTH, true, true));

        // Decode ByteBuf → String using UTF-8
        pipeline.addLast("decoder",
                new StringDecoder(CharsetUtil.UTF_8));

        // Encode String → ByteBuf using UTF-8 (for outbound responses)
        // Added before ConnectionManager so it's available throughout the pipeline.
        pipeline.addLast("encoder",
                new StringEncoder(CharsetUtil.UTF_8));

        // Connection management — SHARABLE, single instance for all connections.
        // Must come BEFORE CacheServerHandler so limits are enforced before
        // any commands are processed.
        pipeline.addLast("connectionManager", connectionManager);

        // Command handler — NOT sharable, one instance per connection.
        // Receives clean Strings, dispatches to cache engine, writes responses.
        pipeline.addLast("handler",
                new CacheServerHandler(cache, metrics, config));
    }

    // =========================================================================
    // Private — cache construction
    // =========================================================================

    /**
     * Builds the cache engine from configuration.
     *
     * The cache stack (from outer to inner):
     *   TTLCache (TTL expiry decorator)
     *     SegmentedCache (thread-safe wrapper, 16 segments)
     *       LRUCache / LFUCache / ARCCache (eviction policy)
     *
     * Why this order?
     *   - The policy cache is pure logic, not thread-safe.
     *   - SegmentedCache wraps it to make it thread-safe.
     *   - TTLCache wraps the thread-safe cache to add expiry behaviour.
     *   - CacheServerHandler sees Cache<String, String> — the full stack.
     *
     * CacheFactory.withTTL and CacheFactory.withSegmented are convenience
     * methods that apply these wrappers. We call them in the correct order here.
     *
     * @param config The server configuration specifying policy and capacity.
     * @return A fully configured, thread-safe, TTL-aware cache.
     */
    private static Cache<String, String> buildCache(ServerConfig config) {
        int capacity = config.getCacheCapacity();

        // Map CachePolicyType enum → SegmentedCache.PolicyType enum.
        // SegmentedCache does NOT accept a Cache delegate — it creates its own
        // internal cache per segment using its own PolicyType enum.
        // This is why we cannot pass a pre-built LRUCache/LFUCache/ARCCache in.
        SegmentedCache.PolicyType segmentPolicy;
        switch (config.getEvictionPolicy()) {
            case LFU:  segmentPolicy = SegmentedCache.PolicyType.LFU; break;
            case ARC:  segmentPolicy = SegmentedCache.PolicyType.ARC; break;
            case LRU:  // fall through
            default:   segmentPolicy = SegmentedCache.PolicyType.LRU; break;
        }

        // Build SegmentedCache directly with (totalCapacity, numSegments, policy).
        // - totalCapacity: from ServerConfig — total entries across ALL segments
        // - numSegments:   16 — each segment gets capacity/16 entries
        // - policy:        mapped above from config.getEvictionPolicy()
        //
        // DO NOT pass base.size() — size() is the current entry count (0 at startup).
        // DO NOT pass a Cache delegate — SegmentedCache creates its own per-segment caches.
        Cache<String, String> threadSafe = new SegmentedCache<>(capacity, 16, segmentPolicy);

        // Wrap with TTLCache using the (Cache, long) constructor.
        // long = sweepIntervalMs, NOT a java.time.Duration.
        TTLCache<String, String> withTTL = new TTLCache<>(
                threadSafe,
                config.getSweepIntervalMs()
        );

        System.out.printf("[CacheServer] Cache built: policy=%s, capacity=%d, " +
                        "sweepInterval=%dms%n",
                config.getEvictionPolicy(),
                config.getCacheCapacity(),
                config.getSweepIntervalMs()
        );

        return withTTL;
    }

    // =========================================================================
    // Private — shutdown infrastructure
    // =========================================================================

    /**
     * Registers a JVM shutdown hook that calls shutdown() when the JVM exits.
     *
     * The shutdown hook runs when:
     *   - The user presses Ctrl+C (SIGINT)
     *   - The process receives SIGTERM (Docker stop, kill)
     *   - System.exit() is called from application code
     *   - The last non-daemon thread exits
     *
     * WHY A SHUTDOWN HOOK?
     *   Without this, Ctrl+C would kill the JVM immediately, leaving Netty threads
     *   running and client connections open. The hook gives us ~30 seconds
     *   (default JVM shutdown timeout) to clean up.
     *
     * CAUTION:
     *   Shutdown hooks run concurrently with each other and with application threads.
     *   Our shutdown() method is idempotent and uses volatile/atomic state,
     *   so concurrent calls are safe.
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[CacheServer] Shutdown hook triggered");
            shutdown();
        }, "jcache-shutdown-hook"));
    }

    /**
     * Releases Netty resources after the server channel has been closed.
     *
     * shutdownGracefully() with our shorter quiet period and timeout values
     * ensures tests don't wait the full Netty default (2s quiet, 15s timeout)
     * during teardown.
     *
     * This method is called from the finally block in start(), so it always
     * runs whether start() exits normally or via exception/interrupt.
     */
    private void performCleanup() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(SHUTDOWN_QUIET_PERIOD_MS,
                    SHUTDOWN_TIMEOUT_MS,
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(SHUTDOWN_QUIET_PERIOD_MS,
                    SHUTDOWN_TIMEOUT_MS,
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        System.out.println("[CacheServer] Netty event loops stopped");
    }

    // =========================================================================
    // Private — startup banner
    // =========================================================================

    /**
     * Prints a startup banner to stdout when the server is ready to accept connections.
     *
     * This is the visible confirmation that everything worked.
     * Matches the style of Redis's startup output — operators know to look for this.
     */
    private void printStartupBanner() {
        System.out.println("==========================================");
        System.out.println("  JCache Server started successfully");
        System.out.println("==========================================");
        System.out.printf("  Port      : %d%n",   config.getPort());
        System.out.printf("  Policy    : %s%n",   config.getEvictionPolicy());
        System.out.printf("  Capacity  : %,d%n",  config.getCacheCapacity());
        System.out.printf("  Workers   : %d%n",   config.getWorkerThreads());
        System.out.printf("  Max Conn  : %,d%n",  config.getMaxConnections());
        System.out.printf("  Verbose   : %b%n",   config.isVerbose());
        System.out.printf("  Persist   : %b%n",   config.isPersistenceEnabled());
        System.out.println("==========================================");
        System.out.println("  Ready to accept connections");
        System.out.println("  Use Ctrl+C or SIGTERM to stop");
        System.out.println("==========================================");
    }

    // =========================================================================
    // Private — command-line argument parsing
    // =========================================================================

    /**
     * Parses command-line arguments into a ServerConfig.
     *
     * Supported flags:
     *   --config <path>              Load from properties file (other flags override)
     *   --port <number>              Override port
     *   --capacity <number>          Override cache capacity
     *   --policy <LRU|LFU|ARC|FIFO> Override eviction policy
     *   --verbose                    Enable verbose logging
     *
     * @param args Command-line arguments from main().
     * @return Configured ServerConfig.
     */
    private static ServerConfig parseArgs(String[] args) {
        if (args.length == 0) {
            return ServerConfig.defaults();
        }

        // Check for --config first — it loads a base config we can then override
        String configPath = null;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i])) {
                configPath = args[i + 1];
                break;
            }
        }

        ServerConfig.Builder builder = (configPath != null)
                ? rebuildFromConfig(configPath)
                : ServerConfig.builder();

        // Apply individual flag overrides on top of the base config
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length) {
                        builder.port(Integer.parseInt(args[++i]));
                    }
                    break;
                case "--capacity":
                    if (i + 1 < args.length) {
                        builder.cacheCapacity(Integer.parseInt(args[++i]));
                    }
                    break;
                case "--policy":
                    if (i + 1 < args.length) {
                        try {
                            builder.evictionPolicy(
                                    com.cache.api.CachePolicyType.valueOf(args[++i].toUpperCase()));
                        } catch (IllegalArgumentException e) {
                            System.err.printf("[CacheServer] Unknown policy: %s, using LRU%n", args[i]);
                        }
                    }
                    break;
                case "--verbose":
                    builder.verbose(true);
                    break;
                case "--config":
                    i++; // already handled above, skip the value
                    break;
                default:
                    System.err.printf("[CacheServer] Unknown argument: %s (ignored)%n", args[i]);
            }
        }

        return builder.build();
    }

    /**
     * Loads config from a properties file and returns a Builder with those values,
     * ready for further command-line overrides.
     *
     * We cannot directly get a Builder from fromProperties() (it returns ServerConfig),
     * so we load the config and re-apply its values to a new Builder.
     *
     * @param path Path to the properties file.
     * @return Builder populated from the properties file.
     */
    private static ServerConfig.Builder rebuildFromConfig(String path) {
        ServerConfig loaded = ServerConfig.fromProperties(path);
        return ServerConfig.builder()
                .port(loaded.getPort())
                .bossThreads(loaded.getBossThreads())
                .workerThreads(loaded.getWorkerThreads())
                .maxConnections(loaded.getMaxConnections())
                .cacheCapacity(loaded.getCacheCapacity())
                .evictionPolicy(loaded.getEvictionPolicy())
                .sweepIntervalMs(loaded.getSweepIntervalMs())
                .persistenceEnabled(loaded.isPersistenceEnabled())
                .snapshotPath(loaded.getSnapshotPath())
                .verbose(loaded.isVerbose());
    }
}