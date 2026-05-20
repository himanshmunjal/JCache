package com.cache.server;

/**
 * ServerConfig holds all runtime configuration for CacheServer.
 *
 * DESIGN DECISION — Plain Java object, not a framework:
 * We deliberately avoid Spring @ConfigurationProperties or similar frameworks.
 * This is a systems library — adding a DI framework as a dependency would
 * bloat the server and add startup overhead. Plain Java with a builder pattern
 * gives the same ergonomics with zero dependencies.
 *
 * DESIGN DECISION — Immutable after build:
 * ServerConfig is constructed via a Builder and then never modified.
 * CacheServer reads config once at startup. Mutable config would require
 * synchronization everywhere config is read — needless complexity for
 * values that don't change at runtime.
 *
 * CONFIGURATION VALUES AND THEIR RATIONALE:
 *
 *   port (default 6379):
 *     6379 is Redis's default port. Using the same default means anyone
 *     familiar with Redis immediately knows where to look. Your CLI client
 *     can default to the same port.
 *
 *   bossThreads (default 1):
 *     The boss thread group only accepts incoming TCP connections — it calls
 *     accept() and hands the channel to a worker thread. One thread can handle
 *     tens of thousands of accept() calls per second. More boss threads add
 *     complexity with zero benefit for most workloads.
 *
 *   workerThreads (default 2 × CPU cores):
 *     Worker threads do the actual I/O: reading bytes, parsing commands,
 *     calling cache.get()/put(), writing responses. The JVM default for
 *     Netty is 2 × available processors — matches the number of hardware
 *     threads, saturating CPU without excessive context switching.
 *
 *   maxConnections (default 1000):
 *     Prevents a single slow client or connection leak from exhausting
 *     file descriptors. Linux default ulimit is 1024 open files per process.
 *     Setting maxConnections below that leaves room for other FDs (logs, etc.).
 *
 *   cacheCapacity (default 10,000):
 *     10k entries is large enough for real workloads (session cache, rate limiting)
 *     without consuming excessive heap. At ~100 bytes per entry average,
 *     10k entries ≈ 1MB heap — negligible on any modern JVM.
 *
 *   evictionPolicy (default "LRU"):
 *     LRU is the right default because:
 *       - Most predictable behavior under unknown workloads
 *       - Lowest implementation complexity (fewest bugs)
 *       - Well-understood by ops teams
 *     LFU or ARC can be selected via config for known Zipfian workloads.
 *
 *   backlog (default 128):
 *     TCP connection backlog — how many pending (not yet accepted) connections
 *     the OS will queue before refusing new ones. 128 is the Linux default
 *     (SOMAXCONN). For high-traffic servers, increase to 512 or 1024.
 *
 * USAGE:
 *   // Default config (good for development and testing)
 *   ServerConfig config = ServerConfig.defaults();
 *
 *   // Custom config (production)
 *   ServerConfig config = ServerConfig.builder()
 *       .port(7379)
 *       .cacheCapacity(100_000)
 *       .workerThreads(16)
 *       .evictionPolicy("ARC")
 *       .maxConnections(5000)
 *       .build();
 */
public final class ServerConfig {

    // -------------------------------------------------------------------------
    // Fields — all final, set once by Builder
    // -------------------------------------------------------------------------

    /** TCP port the server listens on. */
    private final int port;

    /**
     * Number of Netty boss threads (accept new connections).
     * Almost always 1 — accepting connections is not CPU-intensive.
     */
    private final int bossThreads;

    /**
     * Number of Netty worker threads (handle I/O on accepted connections).
     * Default: 2 × available CPU processors.
     */
    private final int workerThreads;

    /**
     * Maximum concurrent client connections before new connections are rejected.
     * Prevents resource exhaustion.
     */
    private final int maxConnections;

    /**
     * Maximum number of entries the cache will hold before eviction fires.
     * This is passed to LRUCache/LFUCache/ARCCache constructor.
     */
    private final int cacheCapacity;

    /**
     * Which eviction policy to use: "LRU", "LFU", or "ARC".
     * Determines which cache implementation CacheServer instantiates.
     */
    private final String evictionPolicy;

    /**
     * TCP accept() backlog — how many pending connections the OS queues.
     * Increase for high-traffic servers that accept connections in bursts.
     */
    private final int backlog;

    /**
     * Whether to print verbose startup information and connection logs.
     * Set to false in production to avoid log spam.
     */
    private final boolean verbose;

    // -------------------------------------------------------------------------
    // Private constructor — only Builder can instantiate
    // -------------------------------------------------------------------------

    private ServerConfig(Builder builder) {
        this.port           = builder.port;
        this.bossThreads    = builder.bossThreads;
        this.workerThreads  = builder.workerThreads;
        this.maxConnections = builder.maxConnections;
        this.cacheCapacity  = builder.cacheCapacity;
        this.evictionPolicy = builder.evictionPolicy;
        this.backlog        = builder.backlog;
        this.verbose        = builder.verbose;
    }

    // -------------------------------------------------------------------------
    // Static factory methods
    // -------------------------------------------------------------------------

    /**
     * Returns a ServerConfig with all values set to their defaults.
     * Suitable for development, testing, and quick demos.
     *
     * @return Default ServerConfig instance.
     */
    public static ServerConfig defaults() {
        return builder().build();
    }

    /**
     * Returns a new Builder with all defaults pre-set.
     * Call setter methods to override specific values, then call build().
     *
     * @return A fresh Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    // -------------------------------------------------------------------------
    // Getters — no setters, this object is immutable
    // -------------------------------------------------------------------------

    /** @return TCP port the server will bind to. */
    public int getPort() { return port; }

    /** @return Number of boss (acceptor) threads in the Netty boss group. */
    public int getBossThreads() { return bossThreads; }

    /** @return Number of I/O worker threads in the Netty worker group. */
    public int getWorkerThreads() { return workerThreads; }

    /** @return Maximum number of concurrent client connections. */
    public int getMaxConnections() { return maxConnections; }

    /** @return Maximum cache capacity (number of entries). */
    public int getCacheCapacity() { return cacheCapacity; }

    /** @return Eviction policy name: "LRU", "LFU", or "ARC". */
    public String getEvictionPolicy() { return evictionPolicy; }

    /** @return TCP accept backlog size. */
    public int getBacklog() { return backlog; }

    /** @return Whether verbose logging is enabled. */
    public boolean isVerbose() { return verbose; }

    // -------------------------------------------------------------------------
    // toString — useful for startup logging
    // -------------------------------------------------------------------------

    /**
     * Returns a human-readable summary of this config.
     * Printed by CacheServer on startup so operators know what's running.
     *
     * Example output:
     *   ServerConfig{port=6379, policy=LRU, capacity=10000,
     *     workers=8, maxConn=1000, backlog=128}
     */
    @Override
    public String toString() {
        return String.format(
                "ServerConfig{port=%d, policy=%s, capacity=%d, " +
                        "bossThreads=%d, workerThreads=%d, maxConnections=%d, backlog=%d, verbose=%b}",
                port, evictionPolicy, cacheCapacity,
                bossThreads, workerThreads, maxConnections, backlog, verbose
        );
    }

    // =========================================================================
    // Builder
    // =========================================================================

    /**
     * Builder for ServerConfig.
     *
     * All fields have sensible defaults — you only need to set what you want to change.
     *
     * WHY BUILDER PATTERN HERE?
     * ServerConfig has 8 fields. A constructor with 8 parameters is unreadable:
     *   new ServerConfig(6379, 1, 8, 1000, 10000, "LRU", 128, false) // what are these?
     *
     * A builder makes intent explicit:
     *   ServerConfig.builder().port(6379).evictionPolicy("ARC").build()
     *
     * It also allows partial configuration — only override what you care about.
     */
    public static final class Builder {

        // Defaults — match the field documentation above
        private int    port           = 6379;
        private int    bossThreads    = 1;
        private int    workerThreads  = Runtime.getRuntime().availableProcessors() * 2;
        private int    maxConnections = 1000;
        private int    cacheCapacity  = 10_000;
        private String evictionPolicy = "LRU";
        private int    backlog        = 128;
        private boolean verbose       = false;

        // Private constructor — force use of ServerConfig.builder()
        private Builder() {}

        /**
         * Sets the TCP port.
         *
         * @param port Port number (1024–65535 for non-root processes).
         *             Ports below 1024 require root privileges on Linux.
         * @return this Builder for chaining.
         */
        public Builder port(int port) {
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException(
                        "Port must be between 1 and 65535, got: " + port
                );
            }
            this.port = port;
            return this;
        }

        /**
         * Sets the number of boss (acceptor) threads.
         * Rarely needs to be changed. Keep at 1 unless profiling shows accept() bottleneck.
         *
         * @param bossThreads Must be >= 1.
         */
        public Builder bossThreads(int bossThreads) {
            if (bossThreads < 1) {
                throw new IllegalArgumentException("bossThreads must be >= 1");
            }
            this.bossThreads = bossThreads;
            return this;
        }

        /**
         * Sets the number of worker I/O threads.
         * Default is 2 × CPU cores. Increase for I/O-bound workloads
         * where workers spend time waiting on the cache (e.g., slow disk-backed cache).
         * For an in-memory cache, default is optimal.
         *
         * @param workerThreads Must be >= 1.
         */
        public Builder workerThreads(int workerThreads) {
            if (workerThreads < 1) {
                throw new IllegalArgumentException("workerThreads must be >= 1");
            }
            this.workerThreads = workerThreads;
            return this;
        }

        /**
         * Sets the maximum number of concurrent client connections.
         *
         * @param maxConnections Must be >= 1.
         */
        public Builder maxConnections(int maxConnections) {
            if (maxConnections < 1) {
                throw new IllegalArgumentException("maxConnections must be >= 1");
            }
            this.maxConnections = maxConnections;
            return this;
        }

        /**
         * Sets the maximum number of entries in the cache.
         *
         * @param cacheCapacity Must be >= 1.
         */
        public Builder cacheCapacity(int cacheCapacity) {
            if (cacheCapacity < 1) {
                throw new IllegalArgumentException("cacheCapacity must be >= 1");
            }
            this.cacheCapacity = cacheCapacity;
            return this;
        }

        /**
         * Sets the eviction policy.
         *
         * @param evictionPolicy One of "LRU", "LFU", "ARC" (case-insensitive).
         */
        public Builder evictionPolicy(String evictionPolicy) {
            if (evictionPolicy == null) {
                throw new IllegalArgumentException("evictionPolicy cannot be null");
            }
            String upper = evictionPolicy.toUpperCase();
            if (!upper.equals("LRU") && !upper.equals("LFU") && !upper.equals("ARC")) {
                throw new IllegalArgumentException(
                        "evictionPolicy must be LRU, LFU, or ARC, got: " + evictionPolicy
                );
            }
            this.evictionPolicy = upper;
            return this;
        }

        /**
         * Sets the TCP accept backlog.
         *
         * @param backlog Must be >= 1. Typically 128–1024.
         */
        public Builder backlog(int backlog) {
            if (backlog < 1) {
                throw new IllegalArgumentException("backlog must be >= 1");
            }
            this.backlog = backlog;
            return this;
        }

        /**
         * Enables verbose logging (connection events, per-command logging).
         * Use during development; disable in production.
         *
         * @param verbose true to enable verbose output.
         */
        public Builder verbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /**
         * Validates all fields and constructs the ServerConfig.
         *
         * @return An immutable ServerConfig.
         * @throws IllegalStateException if any field combination is invalid.
         */
        public ServerConfig build() {
            // Cross-field validation (individual field validation is in each setter)
            if (workerThreads < bossThreads) {
                // Not strictly invalid, but almost certainly a misconfiguration.
                // Log a warning rather than throwing.
                System.err.println("[ServerConfig] WARNING: workerThreads (" + workerThreads +
                        ") < bossThreads (" + bossThreads + "). This is unusual.");
            }
            return new ServerConfig(this);
        }
    }
}