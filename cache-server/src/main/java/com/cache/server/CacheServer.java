package com.cache.server;

import com.cache.api.Cache;
import com.cache.concurrent.SegmentedCache;
import com.cache.policy.ARCCache;
import com.cache.policy.LFUCache;
import com.cache.policy.LRUCache;
import com.cache.server.handler.CacheServerHandler;
import com.cache.server.metrics.ServerMetrics;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CacheServer is the main entry point for the distributed cache server.
 *
 * It bootstraps a Netty TCP server that accepts client connections,
 * parses text commands (GET, PUT, DELETE, STATS, FLUSH, PING),
 * delegates to the cache-core engine, and returns responses.
 *
 * ARCHITECTURE OVERVIEW:
 *
 *   main()
 *     ↓
 *   CacheServer.start()
 *     ↓
 *   Netty ServerBootstrap
 *     ↓ binds to port
 *   NioEventLoopGroup (boss) ← accepts TCP connections
 *     ↓ hands channel to
 *   NioEventLoopGroup (workers) ← handles reads/writes
 *     ↓ per connection
 *   ChannelPipeline:
 *     LineBasedFrameDecoder   ← buffers bytes until \n, prevents TCP fragmentation
 *     StringDecoder           ← converts ByteBuf → String
 *     StringEncoder           ← converts String → ByteBuf for responses
 *     CacheServerHandler      ← YOUR CODE: parse command → call cache → write response
 *
 * NETTY THREADING MODEL:
 *   Boss thread:   1 thread, calls accept() in a loop, dispatches new channels to workers.
 *   Worker threads: N threads (default 2×CPU), each handles a fixed set of channels.
 *                   One worker thread handles many channels via Java NIO selectors.
 *                   No blocking I/O on worker threads — only non-blocking reads/writes.
 *
 *   Your CacheServerHandler.channelRead0() is called on a worker thread.
 *   The cache.get()/put() calls happen on that worker thread.
 *   This is why the cache must be thread-safe (we use SegmentedCache as the wrapper).
 *
 * WIRE PROTOCOL:
 *   Commands are newline-terminated ASCII strings. Responses start with + (success)
 *   or - (error). This is a simplified version of Redis's RESP protocol.
 *
 *   Client sends:          Server responds:
 *   PING                → +PONG
 *   GET key             → +value  or  -ERR key not found
 *   PUT key value       → +OK
 *   PUT key value 60    → +OK         (60 = TTL in seconds)
 *   DELETE key          → +OK
 *   STATS               → +hits:N misses:N evictions:N size:N
 *   FLUSH               → +OK         (clears all entries)
 *
 * GRACEFUL SHUTDOWN:
 *   A JVM shutdown hook calls stop() when Ctrl+C is pressed or kill is received.
 *   stop() calls shutdownGracefully() on both EventLoopGroups, which:
 *     1. Stops accepting new connections.
 *     2. Waits for in-flight requests to complete (quiet period = 100ms).
 *     3. Forces shutdown after timeout (2 seconds).
 *
 * USAGE:
 *   // Default config (port 6379, LRU, capacity 10k)
 *   CacheServer server = new CacheServer(ServerConfig.defaults());
 *   server.start();
 *
 *   // Custom config
 *   ServerConfig config = ServerConfig.builder()
 *       .port(7379)
 *       .evictionPolicy("ARC")
 *       .cacheCapacity(100_000)
 *       .build();
 *   CacheServer server = new CacheServer(config);
 *   server.start();       // blocks until server is shut down
 *
 *   // Programmatic shutdown (e.g., from tests)
 *   server.stop();
 */
public class CacheServer {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /**
     * Maximum line length the server will accept from a client.
     * Prevents malicious clients from sending arbitrarily large lines
     * that would exhaust heap in LineBasedFrameDecoder's buffer.
     *
     * 8KB is enough for keys and values of reasonable size.
     * If your use case needs larger values, increase this.
     */
    private static final int MAX_LINE_LENGTH = 8 * 1024; // 8KB

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Immutable configuration — read once at startup, never modified. */
    private final ServerConfig config;

    /**
     * The cache engine — created once, shared across all client connections.
     *
     * WHY SEGMENTED WRAPPING THE POLICY?
     * Multiple Netty worker threads call cache.get()/put() concurrently
     * (one call per connected client, per received command).
     * SegmentedCache provides fine-grained locking with low contention.
     * The policy (LRU/LFU/ARC) is the delegate inside SegmentedCache.
     *
     * Alternative: CoarseGrainedCache is simpler but more contention.
     * LockFreeCache has the best throughput but highest complexity.
     * SegmentedCache is the right default for production.
     */
    private final Cache<String, String> cache;

    /**
     * Netty boss event loop — one thread, accepts TCP connections.
     * Named "cache-server-boss" for easy identification in thread dumps.
     */
    private NioEventLoopGroup bossGroup;

    /**
     * Netty worker event loop — N threads, handles I/O on accepted channels.
     * Named "cache-server-worker" for easy identification in thread dumps.
     */
    private NioEventLoopGroup workerGroup;

    /**
     * Channel representing the server socket.
     * Stored so we can close it cleanly in stop().
     */
    private Channel serverChannel;

    /**
     * Guards against calling start() twice or stop() before start().
     * AtomicBoolean ensures thread-safe state transitions.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Latch released when the server has fully started and is accepting connections.
     * Tests and programmatic callers can await this instead of sleeping.
     */
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private ServerMetrics metrics = new ServerMetrics();

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a CacheServer with the given configuration.
     * Does NOT start the server — call start() for that.
     *
     * @param config Server configuration. Must not be null.
     */
    public CacheServer(ServerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ServerConfig cannot be null");
        }
        this.config = config;
        this.metrics = metrics;
        this.cache  = buildCache(config);
    }

    // -------------------------------------------------------------------------
    // Lifecycle methods
    // -------------------------------------------------------------------------

    /**
     * Starts the Netty server and blocks until shutdown.
     *
     * This method:
     *   1. Creates boss and worker EventLoopGroups.
     *   2. Configures ServerBootstrap with the channel pipeline.
     *   3. Binds to the configured port.
     *   4. Registers a JVM shutdown hook for graceful shutdown.
     *   5. Blocks on channel.closeFuture() until the server is stopped.
     *
     * BLOCKING vs NON-BLOCKING:
     *   This method BLOCKS the calling thread until the server stops.
     *   This is the standard pattern for a server main() method.
     *   For tests that need to start/stop programmatically, call
     *   startAsync() instead (starts in a background thread).
     *
     * @throws InterruptedException if the calling thread is interrupted while
     *                              waiting for server startup or shutdown.
     */
    public void start() throws InterruptedException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("CacheServer is already running");
        }

        // Create event loop groups with named threads for easier debugging.
        // Named threads show up as "cache-server-boss-1" in thread dumps and logs.
        bossGroup   = new NioEventLoopGroup(config.getBossThreads(),
                (ThreadFactory) r -> new Thread(r, "cache-server-boss"));
        workerGroup = new NioEventLoopGroup(config.getWorkerThreads(),
                (ThreadFactory) r -> new Thread(r, "cache-server-worker"));

        try {
            ServerBootstrap bootstrap = buildServerBootstrap();

            // Bind to the port — this starts the server.
            // sync() waits until the bind completes (or throws if port is in use).
            ChannelFuture bindFuture = bootstrap.bind(config.getPort()).sync();

            if (!bindFuture.isSuccess()) {
                throw new RuntimeException(
                        "Failed to bind to port " + config.getPort(),
                        bindFuture.cause()
                );
            }

            serverChannel = bindFuture.channel();

            // Log startup information
            logStartup();

            // Register JVM shutdown hook AFTER successful bind.
            // If bind fails, we don't want a shutdown hook calling stop()
            // on an uninitialized server.
            registerShutdownHook();

            // Release the start latch — tests waiting on awaitStart() can proceed.
            startLatch.countDown();

            // Block here until the server channel is closed (by stop() or JVM shutdown).
            // This is what keeps the main thread alive while the server runs.
            serverChannel.closeFuture().sync();

        } finally {
            // Always clean up event loops, even if startup or runtime throws.
            shutdown();
        }
    }

    /**
     * Starts the server in a background daemon thread.
     * Returns after the server has successfully bound to its port.
     *
     * Use this from tests or embedded usage where you don't want to block
     * the calling thread:
     *
     *   CacheServer server = new CacheServer(config);
     *   server.startAsync();  // returns immediately
     *   // ... run tests ...
     *   server.stop();
     *
     * @throws InterruptedException if interrupted while waiting for startup.
     */
    public void startAsync() throws InterruptedException {
        Thread serverThread = new Thread(() -> {
            try {
                start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "cache-server-main");

        // Daemon thread: JVM can exit even if this thread is still running.
        // This prevents the server from keeping the test JVM alive after tests complete.
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait until the server has bound to the port before returning.
        // Without this, callers might try to connect before the server is ready.
        startLatch.await();
    }

    /**
     * Stops the server gracefully.
     *
     * Graceful shutdown sequence:
     *   1. Close the server socket (stop accepting new connections).
     *   2. Allow in-flight requests to complete (quiet period = 100ms).
     *   3. Force-close any remaining connections after timeout (2 seconds).
     *   4. Shut down boss and worker EventLoopGroups.
     *
     * Calling stop() on an already-stopped server is a safe no-op.
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return; // already stopped or never started
        }

        System.out.println("[CacheServer] Shutting down...");

        // Close the server channel — stops accepting new connections.
        if (serverChannel != null && serverChannel.isOpen()) {
            serverChannel.close();
        }

        shutdown();

        System.out.println("[CacheServer] Shutdown complete.");
    }

    /**
     * Blocks the calling thread until the server has successfully bound
     * to its port and is ready to accept connections.
     *
     * Use after startAsync() to ensure the server is ready before running tests.
     *
     * @throws InterruptedException if interrupted while waiting.
     */
    public void awaitStart() throws InterruptedException {
        startLatch.await();
    }

    /**
     * Returns true if the server is currently running and accepting connections.
     *
     * @return true if running, false otherwise.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Returns the port this server is (or will be) listening on.
     * Useful when the server was configured with port 0 (OS-assigned port)
     * and you need to know the actual port.
     *
     * @return The configured port number.
     */
    public int getPort() {
        return config.getPort();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the Netty ServerBootstrap with full pipeline configuration.
     *
     * PIPELINE EXPLANATION (handlers execute in order for inbound data):
     *
     *   1. LineBasedFrameDecoder:
     *      TCP is a stream — bytes arrive in arbitrary chunks. This decoder
     *      accumulates bytes in a buffer until it finds a \n or \r\n,
     *      then emits exactly one complete line downstream.
     *      Without this: "GET foo\nPUT b" might arrive as one read,
     *      and "ar baz\n" as the next. LineBasedFrameDecoder handles this.
     *      maxLineLength prevents memory exhaustion from malicious clients.
     *
     *   2. StringDecoder:
     *      Converts the ByteBuf (raw bytes) emitted by LineBasedFrameDecoder
     *      into a String using UTF-8 encoding.
     *      After this handler, your CacheServerHandler receives Strings, not bytes.
     *
     *   3. StringEncoder:
     *      Outbound handler (for responses, not inbound commands).
     *      Converts String responses back to ByteBuf for sending over the network.
     *      Must be added BEFORE CacheServerHandler in the pipeline because
     *      pipeline handlers process outbound data in REVERSE order.
     *
     *   4. CacheServerHandler:
     *      YOUR application logic. Receives a String command, dispatches to cache,
     *      writes String response. Netty calls this on a worker thread.
     *
     * @return A configured ServerBootstrap ready to bind.
     */
    private ServerBootstrap buildServerBootstrap() {
        // CacheServerHandler is NOT @ChannelHandler.Sharable by default.
        // We pass the cache and config as constructor args so each channel
        // gets its own handler instance with shared cache reference.
        // This is the correct Netty pattern for stateful handlers.

        return new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)

                // SO_BACKLOG: how many pending connections the OS will queue
                // before refusing new ones. Set from config.
                .option(ChannelOption.SO_BACKLOG, config.getBacklog())

                // SO_REUSEADDR: allows rebinding to a port in TIME_WAIT state.
                // Prevents "Address already in use" errors when restarting the server quickly.
                .option(ChannelOption.SO_REUSEADDR, true)

                // TCP_NODELAY: disables Nagle's algorithm.
                // Nagle buffers small packets to reduce network overhead.
                // For a cache server, we want responses sent immediately, not batched.
                // Disabling Nagle reduces latency at the cost of slightly more packets.
                .childOption(ChannelOption.TCP_NODELAY, true)

                // SO_KEEPALIVE: OS-level heartbeat to detect dead connections.
                // Cleans up connections from crashed clients without explicit disconnects.
                .childOption(ChannelOption.SO_KEEPALIVE, true)

                // Channel initializer: called for each new accepted connection.
                // Creates a fresh ChannelPipeline for that connection.
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // INBOUND handlers (process data received from client):
                        pipeline.addLast("frameDecoder",
                                new LineBasedFrameDecoder(MAX_LINE_LENGTH));

                        pipeline.addLast("stringDecoder",
                                new StringDecoder(CharsetUtil.UTF_8));

                        // OUTBOUND handler (processes data sent to client):
                        // Must be added before CacheServerHandler so it's available
                        // when CacheServerHandler calls ctx.writeAndFlush(String).
                        pipeline.addLast("stringEncoder",
                                new StringEncoder(CharsetUtil.UTF_8));

                        // APPLICATION handler — your cache logic:
                        pipeline.addLast("cacheHandler",
                                new CacheServerHandler(cache, metrics ,config));
                    }
                });
    }

    /**
     * Builds the cache engine based on the configured eviction policy.
     *
     * All policies are wrapped in SegmentedCache for thread safety.
     * SegmentedCache splits the cache into N segments, each with its own lock,
     * reducing contention compared to a single global lock.
     *
     * WHY WRAP WITH SEGMENTED INSTEAD OF USING POLICY DIRECTLY?
     * LRUCache, LFUCache, and ARCCache are NOT thread-safe by themselves.
     * SegmentedCache adds thread safety without modifying the policy implementations.
     * This is the Open/Closed Principle — extend behavior without modifying existing code.
     *
     * @param config The ServerConfig specifying which policy to use.
     * @return A thread-safe cache instance.
     */
    private static Cache<String, String> buildCache(ServerConfig config) {
        int capacity = config.getCacheCapacity();

        // Build the policy-specific delegate
        Cache<String, String> delegate;
        switch (config.getEvictionPolicy()) {
            case "LFU":
                delegate = new LFUCache<>(capacity);
                break;
            case "ARC":
                delegate = new ARCCache<>(capacity);
                break;
            case "LRU":
            default:
                delegate = new LRUCache<>(capacity);
                break;
        }

        // Wrap with segmented locking — 16 segments is a good default
        // (covers up to 16 concurrent writers with zero contention).
        return new SegmentedCache<>(capacity, 16);

        // ALTERNATIVE: For maximum throughput in read-heavy workloads:
        // return new LockFreeCache<>(capacity);
        //
        // ALTERNATIVE: For simplicity in development/testing:
        // return new CoarseGrainedCache<>(delegate);
    }

    /**
     * Shuts down both EventLoopGroups gracefully.
     *
     * shutdownGracefully() starts a quiet period (100ms by default) during which
     * no new tasks are accepted, but in-flight tasks complete.
     * After the quiet period, remaining threads are interrupted and terminated.
     *
     * We call sync() on the returned futures to wait for full shutdown before
     * this method returns. This prevents resource leaks in tests.
     */
    private void shutdown() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    /**
     * Registers a JVM shutdown hook that calls stop() on Ctrl+C or SIGTERM.
     *
     * Without this, the server process hangs on exit because Netty's
     * EventLoopGroup threads are non-daemon and prevent JVM shutdown.
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[CacheServer] Shutdown signal received.");
            stop();
        }, "cache-server-shutdown-hook"));
    }

    /**
     * Logs startup information to stdout.
     * Shows port, policy, capacity, and thread counts so operators know
     * what's running without reading config files.
     */
    private void logStartup() {
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║          JCache Server Started            ║");
        System.out.println("╠══════════════════════════════════════════╣");
        System.out.printf( "║  Port:            %-23d║%n", config.getPort());
        System.out.printf( "║  Eviction Policy: %-23s║%n", config.getEvictionPolicy());
        System.out.printf( "║  Cache Capacity:  %-23d║%n", config.getCacheCapacity());
        System.out.printf( "║  Worker Threads:  %-23d║%n", config.getWorkerThreads());
        System.out.printf( "║  Max Connections: %-23d║%n", config.getMaxConnections());
        System.out.println("╚══════════════════════════════════════════╝");
        System.out.println("Ready. Connect with: telnet localhost " + config.getPort());
    }

    // -------------------------------------------------------------------------
    // main() — entry point when running as a standalone server
    // -------------------------------------------------------------------------

    /**
     * Starts the cache server from the command line.
     *
     * Usage:
     *   java -jar cache-server.jar                          # defaults
     *   java -jar cache-server.jar 7379                     # custom port
     *   java -jar cache-server.jar 7379 LFU 50000          # port + policy + capacity
     *
     * After starting, connect with:
     *   telnet localhost 6379
     *   > PING
     *   +PONG
     *   > PUT name Alice 60
     *   +OK
     *   > GET name
     *   +Alice
     *   > STATS
     *   +hits:1 misses:0 evictions:0 size:1
     */
    public static void main(String[] args) throws InterruptedException {
        ServerConfig config = parseArgs(args);

        System.out.println("Starting with config: " + config);

        CacheServer server = new CacheServer(config);
        server.start(); // blocks until shutdown
    }

    /**
     * Parses command-line arguments into a ServerConfig.
     * Falls back to defaults for any unspecified arguments.
     *
     * @param args Command-line arguments.
     * @return A ServerConfig based on the provided arguments.
     */
    private static ServerConfig parseArgs(String[] args) {
        ServerConfig.Builder builder = ServerConfig.builder();

        if (args.length >= 1) {
            try {
                builder.port(Integer.parseInt(args[0]));
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[0] + ". Using default 6379.");
            }
        }

        if (args.length >= 2) {
            try {
                builder.evictionPolicy(args[1]);
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid policy: " + args[1] + ". Using default LRU.");
            }
        }

        if (args.length >= 3) {
            try {
                builder.cacheCapacity(Integer.parseInt(args[2]));
            } catch (NumberFormatException e) {
                System.err.println("Invalid capacity: " + args[2] + ". Using default 10000.");
            }
        }

        return builder.build();
    }
}