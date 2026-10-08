package com.cache.server;

import com.cache.api.Cache;
import com.cache.concurrent.SegmentedCache;
import com.cache.persistence.PersistenceManager;
import com.cache.persistence.SnapshotLoader;
import com.cache.server.handler.CacheServerHandler;
import com.cache.server.handler.ConnectionManager;
import com.cache.server.handler.ProtocolDetector;
import com.cache.server.handler.RespCommandHandler;
import com.cache.server.metrics.ServerMetrics;
import com.cache.ttl.TTLCache;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Netty TCP server that exposes a JCache instance over a line-based text
 * protocol (see {@link com.cache.common.protocol.CommandParser}) and over
 * RESP, so Redis clients can connect. Both are served on the same port; the
 * first byte of a connection picks the protocol.
 *
 * <p>The cache stack is {@code TTLCache -> SegmentedCache -> LRU/LFU/ARC}.
 * Each connection starts with
 * <pre>
 * StringEncoder -&gt; ProtocolDetector -&gt; ConnectionManager
 * </pre>
 * and {@link ProtocolDetector} then adds either a line decoder and
 * {@link CacheServerHandler}, or the RESP codec and {@link RespCommandHandler}.
 *
 * <p>Typical use:
 * <pre>{@code
 * CacheServer server = new CacheServer(ServerConfig.builder().port(0).build());
 * server.startAsync();          // returns once the port is bound
 * int port = server.getPort();
 * ...
 * server.shutdown();
 * }</pre>
 */
public class CacheServer {

    private static final Logger log = Logger.getLogger(CacheServer.class.getName());

    /** Longest accepted command line, in bytes. */
    static final int MAX_LINE_LENGTH = 1024 * 1024;

    private static final long QUIET_PERIOD_MS = 200;
    private static final long SHUTDOWN_TIMEOUT_MS = 3000;

    private final ServerConfig config;
    private final TTLCache<String, String> cache;
    private final ServerMetrics metrics = new ServerMetrics();
    private final ConnectionManager connectionManager;
    private final PersistenceManager persistence;

    private final CountDownLatch started = new CountDownLatch(1);
    private final Thread shutdownHook = new Thread(this::shutdown, "jcache-shutdown");
    private volatile Throwable startupFailure;
    private volatile boolean running;
    private final AtomicBoolean startCalled = new AtomicBoolean();
    private volatile int boundPort = -1;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    /**
     * Builds the cache and, if enabled, restores it from disk. Does not open
     * the port; call {@link #start()} or {@link #startAsync()} for that.
     *
     * @param config server configuration
     */
    public CacheServer(ServerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ServerConfig cannot be null");
        }
        this.config = config;
        this.cache = buildCache(config);
        this.connectionManager = new ConnectionManager(config.getMaxConnections(), metrics);
        this.persistence = config.isPersistenceEnabled() ? openPersistence(config, cache) : null;
    }

    /** Creates a server with {@link ServerConfig#defaults()}. */
    public CacheServer() {
        this(ServerConfig.defaults());
    }

    /**
     * Binds the port and blocks until the server is shut down. A server
     * instance can only be started once.
     *
     * @throws IllegalStateException if {@code start} was already called
     * @throws RuntimeException      if the port cannot be bound
     */
    public void start() {
        if (!startCalled.compareAndSet(false, true)) {
            throw new IllegalStateException("A CacheServer can only be started once");
        }
        bossGroup = new NioEventLoopGroup(config.getBossThreads());
        workerGroup = new NioEventLoopGroup(config.getWorkerThreads());
        try {
            serverChannel = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            configurePipeline(ch.pipeline());
                        }
                    })
                    .bind(config.getPort())
                    .sync()
                    .channel();

            boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
            running = true;
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            logStartup();
            started.countDown();

            serverChannel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            startupFailure = e;
            throw e;
        } finally {
            started.countDown();
            workerGroup.shutdownGracefully(QUIET_PERIOD_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            bossGroup.shutdownGracefully(QUIET_PERIOD_MS, SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Starts the server on a background thread and waits up to five seconds
     * for the port to be bound.
     */
    public void startAsync() {
        startAsync(5000);
    }

    /**
     * Starts the server on a background thread and waits for the port to be bound.
     *
     * @param timeoutMs how long to wait
     * @throws IllegalStateException if the server failed to start or did not
     *                               start in time
     */
    public void startAsync(long timeoutMs) {
        Thread thread = new Thread(() -> {
            try {
                start();
            } catch (RuntimeException e) {
                log.log(Level.SEVERE, "Server failed to start", e);
            }
        }, "jcache-server");
        thread.setDaemon(true);
        thread.start();
        try {
            if (!started.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Server did not start within " + timeoutMs + " ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the server to start", e);
        }
        if (!running) {
            throw new IllegalStateException("Server failed to start on port " + config.getPort(), startupFailure);
        }
    }

    /**
     * Stops accepting connections, closes the open ones, stops the TTL sweeper
     * and writes a final snapshot. Safe to call more than once and from any thread.
     */
    public synchronized void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        log.info("Shutting down");
        if (Thread.currentThread() != shutdownHook) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // The JVM is already shutting down; the hook will run regardless.
            }
        }
        serverChannel.close().syncUninterruptibly();
        connectionManager.closeAllConnections();
        cache.shutdown();
        if (persistence != null) {
            persistence.close();
        }
        log.info("Shutdown complete");
    }

    /** @return whether the server is accepting connections */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns the port the server is listening on. When configured with port
     * 0 this is the port the operating system picked.
     *
     * @return the bound port, or -1 before the server has started
     */
    public int getPort() {
        return boundPort;
    }

    /** @return the server's metrics */
    public ServerMetrics getMetrics() {
        return metrics;
    }

    /** @return the connection manager */
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    /**
     * Command-line entry point. See {@link #usage()} for the flags.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null) {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$-7s %3$s - %5$s%6$s%n");
        }
        ServerConfig config;
        try {
            config = parseArgs(args);
        } catch (IllegalArgumentException | UncheckedIOException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println(usage());
            System.exit(2);
            return;
        }
        if (config == null) {
            System.out.println(usage());
            return;
        }
        new CacheServer(config).start();
    }

    /**
     * Builds the configuration from the properties file named by
     * {@code --config} (if any), then {@code JCACHE_*} environment variables,
     * then the remaining flags.
     *
     * @return the configuration, or {@code null} if {@code --help} was given
     */
    static ServerConfig parseArgs(String[] args) {
        ServerConfig.Builder builder = ServerConfig.builder();
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i])) {
                builder.applyProperties(args[i + 1]);
            }
        }
        builder.applyEnvironment(System.getenv());

        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            switch (flag) {
                case "--help", "-h" -> {
                    return null;
                }
                case "--verbose" -> builder.verbose(true);
                case "--persist" -> builder.persistenceEnabled(true);
                case "--config", "--port", "--capacity", "--policy", "--segments", "--default-ttl", "--data-dir",
                     "--rate-limit", "--rate-limit-burst" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(flag + " needs a value");
                    }
                    String value = args[++i];
                    switch (flag) {
                        case "--port" -> builder.port(Integer.parseInt(value));
                        case "--capacity" -> builder.cacheCapacity(Integer.parseInt(value));
                        case "--policy" -> builder.evictionPolicy(value);
                        case "--segments" -> builder.segments(Integer.parseInt(value));
                        case "--default-ttl" -> builder.defaultTtlSeconds(Long.parseLong(value));
                        case "--data-dir" -> builder.snapshotPath(value);
                        case "--rate-limit" -> builder.rateLimitPerSecond(Integer.parseInt(value));
                        case "--rate-limit-burst" -> builder.rateLimitBurst(Integer.parseInt(value));
                        default -> { } // --config was applied first
                    }
                }
                default -> throw new IllegalArgumentException("Unknown option: " + flag);
            }
        }
        return builder.build();
    }

    static String usage() {
        return String.join(System.lineSeparator(),
                "Usage: java -jar cache-server.jar [options]",
                "",
                "  --port <n>          TCP port (default 6379)",
                "  --capacity <n>      maximum number of keys (default 10000)",
                "  --policy <name>     LRU, LFU or ARC (default LRU)",
                "  --segments <n>      lock segments, a power of two (default 16)",
                "  --default-ttl <s>   TTL in seconds for writes without one (default 0 = none)",
                "  --rate-limit <n>    commands per second per connection (default 0 = no limit)",
                "  --rate-limit-burst <n>  commands a connection may send at once (default = rate)",
                "  --persist           enable snapshot + append-only-file persistence",
                "  --data-dir <path>   persistence directory (default ./jcache-data)",
                "  --config <file>     read settings from a properties file",
                "  --verbose           log every command",
                "",
                "Every option can also be set with a JCACHE_* environment variable; see the README.");
    }

    private void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
        pipeline.addLast(new ProtocolDetector(MAX_LINE_LENGTH,
                () -> new CacheServerHandler(cache, metrics, config, persistence),
                () -> new RespCommandHandler(cache, metrics, config, persistence)));
        pipeline.addLast(connectionManager);
    }

    private static TTLCache<String, String> buildCache(ServerConfig config) {
        Cache<String, String> segmented = new SegmentedCache<>(
                config.getCacheCapacity(), config.getSegments(), config.getEvictionPolicy());
        // The handler passes the default TTL explicitly so it can also log it to the AOF.
        return new TTLCache<>(segmented, config.getSweepIntervalMs(), Duration.ZERO);
    }

    private static PersistenceManager openPersistence(ServerConfig config, TTLCache<String, String> cache) {
        Path dir = Path.of(config.getSnapshotPath());
        Cache<String, String> persistable = TTLCache.asPersistable(cache);
        SnapshotLoader.LoadResult recovery = new SnapshotLoader(dir, persistable).load();
        log.info("Recovered from " + dir.toAbsolutePath() + ": " + recovery);
        try {
            return new PersistenceManager(dir, persistable, config.getSnapshotIntervalMs());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open data directory " + dir.toAbsolutePath(), e);
        }
    }

    private void logStartup() {
        log.info(startupMessage(boundPort, config));
    }

    /** The startup log line; it lists the settings an operator most often needs to confirm. */
    static String startupMessage(int port, ServerConfig config) {
        String rateLimit = config.getRateLimitPerSecond() == 0 ? "off"
                : config.getRateLimitPerSecond() + "/s burst " + config.getRateLimitBurst();
        return String.format("JCache listening on port %d (policy=%s, capacity=%,d, segments=%d, "
                        + "defaultTtl=%ds, workers=%d, maxConnections=%,d, rateLimit=%s, persistence=%s)",
                port, config.getEvictionPolicy(), config.getCacheCapacity(), config.getSegments(),
                config.getDefaultTtlSeconds(), config.getWorkerThreads(), config.getMaxConnections(), rateLimit,
                config.isPersistenceEnabled() ? Path.of(config.getSnapshotPath()).toAbsolutePath() : "off");
    }
}
