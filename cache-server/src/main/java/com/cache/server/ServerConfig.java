package com.cache.server;

import com.cache.api.CachePolicyType;
import com.cache.persistence.PersistenceConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * ServerConfig is the single source of truth for all CacheServer configuration.
 *
 * DESIGN — WHY ONE CONFIG CLASS?
 *   Every tunable value lives here. CacheServer, CacheServerHandler,
 *   ConnectionManager, and the persistence layer all read from this object.
 *   This means:
 *     - No magic numbers scattered across classes
 *     - One place to look when tuning performance
 *     - Easy to serialize for "what config is this server running?" diagnostics
 *     - Simple to mock in tests (just construct with different values)
 *
 * TWO WAYS TO BUILD A ServerConfig:
 *
 *   1. Builder pattern (programmatic — used in tests and CacheServer.main()):
 *        ServerConfig config = ServerConfig.builder()
 *            .port(6379)
 *            .cacheCapacity(50_000)
 *            .evictionPolicy(CachePolicyType.LRU)
 *            .build();
 *
 *   2. Properties file (for production deployments):
 *        ServerConfig config = ServerConfig.fromProperties("jcache.properties");
 *
 *      jcache.properties example:
 *        server.port=6379
 *        server.boss.threads=1
 *        server.worker.threads=8
 *        server.max.connections=1000
 *        cache.capacity=100000
 *        cache.policy=LRU
 *        cache.sweep.interval.ms=500
 *        persistence.enabled=false
 *        persistence.snapshot.path=./jcache.snapshot
 *        server.verbose=false
 *
 * IMMUTABILITY:
 *   All fields are final after construction via the Builder.
 *   ServerConfig is safe to share across all Netty threads without
 *   synchronization — Netty worker threads read it concurrently.
 *
 * DEFAULTS:
 *   All defaults mirror Redis defaults where applicable (port 6379,
 *   no persistence by default) so the server is usable out of the box
 *   without any configuration.
 */
public final class ServerConfig {

    // =========================================================================
    // Default values — mirrors Redis defaults where applicable
    // =========================================================================

    /** Default TCP port. 6379 matches Redis so tooling works without reconfiguration. */
    public static final int     DEFAULT_PORT                = 6379;

    /**
     * Boss threads accept incoming TCP connections.
     * 1 is always sufficient — the OS queues incoming SYNs in the backlog.
     * Multiple boss threads only help if you are binding to multiple ports.
     */
    public static final int     DEFAULT_BOSS_THREADS        = 1;

    /**
     * Worker threads handle I/O (read/write) for all accepted connections.
     * Netty default is 2 x CPU cores. We default to 8 which fits most
     * development machines and small production deployments.
     * Tune upward if CPU utilization is high under load.
     */
    public static final int     DEFAULT_WORKER_THREADS      = 8;

    /**
     * Maximum concurrent client connections.
     * 1000 is conservative — Redis defaults to 10,000.
     * Increase for production workloads; lower for resource-constrained environments.
     */
    public static final int     DEFAULT_MAX_CONNECTIONS     = 1000;

    /**
     * Maximum number of key-value pairs in the cache.
     * When capacity is reached, the eviction policy removes one entry.
     * 10,000 entries at ~100 bytes each = ~1MB — safe default.
     */
    public static final int     DEFAULT_CACHE_CAPACITY      = 10_000;

    /**
     * Default eviction policy. LRU is the most widely understood and
     * appropriate for most workloads with temporal locality.
     */
    public static final CachePolicyType DEFAULT_POLICY      = CachePolicyType.LRU;

    /**
     * How often the TTL background sweeper runs in milliseconds.
     * 500ms means an expired key is cleaned up within 500ms of expiry
     * (lazy eviction handles it immediately on reads, sweeper handles the rest).
     */
    public static final long    DEFAULT_SWEEP_INTERVAL_MS   = 500L;

    /**
     * Persistence off by default — matches Redis default (RDB/AOF disabled).
     * Enable for "durable cache" deployments where data must survive restart.
     */
    public static final boolean DEFAULT_PERSISTENCE_ENABLED = false;

    /** Default path for snapshot files when persistence is enabled. */
    public static final String  DEFAULT_SNAPSHOT_PATH       = "./jcache.snapshot";

    /**
     * Verbose logging off by default.
     * When true, every command received and connection event is logged to stdout.
     * Useful during development; too noisy for production.
     */
    public static final boolean DEFAULT_VERBOSE             = false;

    // =========================================================================
    // Configuration fields — all final, set once by Builder
    // =========================================================================

    /** TCP port the server listens on. */
    private final int             port;

    /** Number of Netty boss threads (accept new connections). */
    private final int             bossThreads;

    /** Number of Netty worker threads (handle I/O for accepted connections). */
    private final int             workerThreads;

    /** Maximum number of concurrent client connections before rejection. */
    private final int             maxConnections;

    /** Maximum number of entries in the cache before eviction fires. */
    private final int             cacheCapacity;

    /** Which eviction policy to use: LRU, LFU, ARC, or FIFO. */
    private final CachePolicyType evictionPolicy;

    /** How often the TTL sweeper thread runs, in milliseconds. */
    private final long            sweepIntervalMs;

    /** Whether to persist cache state to disk (AOF log + snapshots). */
    private final boolean         persistenceEnabled;

    /** Filesystem path for snapshot files when persistence is enabled. */
    private final String          snapshotPath;

    /**
     * When true, every received command and connection lifecycle event
     * is logged to stdout. Set to false in production.
     */
    private final boolean         verbose;

    // =========================================================================
    // Private constructor — only the Builder or fromProperties() call this
    // =========================================================================

    /**
     * Private constructor. Use {@link Builder} or {@link #fromProperties(String)}.
     */
    private ServerConfig(Builder builder) {
        this.port               = builder.port;
        this.bossThreads        = builder.bossThreads;
        this.workerThreads      = builder.workerThreads;
        this.maxConnections     = builder.maxConnections;
        this.cacheCapacity      = builder.cacheCapacity;
        this.evictionPolicy     = builder.evictionPolicy;
        this.sweepIntervalMs    = builder.sweepIntervalMs;
        this.persistenceEnabled = builder.persistenceEnabled;
        this.snapshotPath       = builder.snapshotPath;
        this.verbose            = builder.verbose;
    }

    // =========================================================================
    // Static factory methods
    // =========================================================================

    /**
     * Returns a Builder pre-populated with all default values.
     * Call .build() immediately for a fully-default config, or chain setter
     * calls before .build() to override specific values.
     *
     * Example:
     *   ServerConfig config = ServerConfig.builder().port(6380).build();
     *
     * @return A new Builder with default values.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a ServerConfig with all default values.
     * Equivalent to ServerConfig.builder().build().
     * Useful for tests and quick-start scenarios.
     *
     * @return Default ServerConfig instance.
     */
    public static ServerConfig defaults() {
        return new Builder().build();
    }

    /**
     * Loads a ServerConfig from a .properties file on the filesystem.
     *
     * All properties are optional — any property not in the file falls back
     * to the default value. This means a partial properties file is valid.
     *
     * Property keys:
     *   server.port               (int,     default: 6379)
     *   server.boss.threads       (int,     default: 1)
     *   server.worker.threads     (int,     default: 8)
     *   server.max.connections    (int,     default: 1000)
     *   server.verbose            (boolean, default: false)
     *   cache.capacity            (int,     default: 10000)
     *   cache.policy              (string,  default: LRU)
     *   cache.sweep.interval.ms   (long,    default: 500)
     *   persistence.enabled       (boolean, default: false)
     *   persistence.snapshot.path (string,  default: ./jcache.snapshot)
     *
     * @param propertiesFilePath Absolute or relative path to the .properties file.
     * @return Populated ServerConfig, with defaults for any missing properties.
     * @throws RuntimeException if the file cannot be read (wraps IOException).
     */
    public static ServerConfig fromProperties(String propertiesFilePath) {
        Properties props = new Properties();

        try (InputStream in = new FileInputStream(propertiesFilePath)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to load ServerConfig from: " + propertiesFilePath, e
            );
        }

        Builder b = new Builder();

        b.port(parseInt(props, "server.port", DEFAULT_PORT));
        b.bossThreads(parseInt(props, "server.boss.threads", DEFAULT_BOSS_THREADS));
        b.workerThreads(parseInt(props, "server.worker.threads", DEFAULT_WORKER_THREADS));
        b.maxConnections(parseInt(props, "server.max.connections", DEFAULT_MAX_CONNECTIONS));
        b.verbose(parseBoolean(props, "server.verbose", DEFAULT_VERBOSE));

        b.cacheCapacity(parseInt(props, "cache.capacity", DEFAULT_CACHE_CAPACITY));
        b.sweepIntervalMs(parseLong(props, "cache.sweep.interval.ms", DEFAULT_SWEEP_INTERVAL_MS));

        // Parse eviction policy — convert string to enum, default to LRU on invalid value
        String policyStr = props.getProperty("cache.policy", DEFAULT_POLICY.name());
        try {
            b.evictionPolicy(CachePolicyType.valueOf(policyStr.toUpperCase()));
        } catch (IllegalArgumentException e) {
            System.err.printf(
                    "[ServerConfig] Unknown policy '%s', defaulting to LRU%n", policyStr);
            b.evictionPolicy(CachePolicyType.LRU);
        }

        b.persistenceEnabled(parseBoolean(props, "persistence.enabled", DEFAULT_PERSISTENCE_ENABLED));
        b.snapshotPath(props.getProperty("persistence.snapshot.path", DEFAULT_SNAPSHOT_PATH));

        return b.build();
    }

    // =========================================================================
    // Getters — all read-only
    // =========================================================================

    /** @return TCP port the server listens on (1-65535). */
    public int getPort() { return port; }

    /** @return Number of Netty boss threads (usually 1). */
    public int getBossThreads() { return bossThreads; }

    /** @return Number of Netty worker threads for I/O. */
    public int getWorkerThreads() { return workerThreads; }

    /** @return Maximum concurrent client connections before rejection. */
    public int getMaxConnections() { return maxConnections; }

    /** @return Maximum number of entries before eviction fires. */
    public int getCacheCapacity() { return cacheCapacity; }

    /** @return The eviction policy in use (LRU/LFU/ARC/FIFO). */
    public CachePolicyType getEvictionPolicy() { return evictionPolicy; }

    /** @return How often the TTL background sweeper runs (milliseconds). */
    public long getSweepIntervalMs() { return sweepIntervalMs; }

    /** @return Whether AOF + snapshot persistence is enabled. */
    public boolean isPersistenceEnabled() { return persistenceEnabled; }

    /** @return Filesystem path for snapshot files. */
    public String getSnapshotPath() { return snapshotPath; }

    /**
     * Whether verbose command/connection logging is enabled.
     * Read by CacheServerHandler and ConnectionManager for per-event logging.
     *
     * @return true if verbose logging is on.
     */
    public boolean isVerbose() { return verbose; }

    // =========================================================================
    // toString — logged by CacheServer on startup
    // =========================================================================

    /**
     * Returns a human-readable summary of all config values.
     * Logged by CacheServer on startup so operators know exactly what is running.
     *
     * @return Multi-field string representation.
     */
    @Override
    public String toString() {
        return String.format(
                "ServerConfig{port=%d, bossThreads=%d, workerThreads=%d, " +
                        "maxConnections=%d, cacheCapacity=%d, policy=%s, " +
                        "sweepIntervalMs=%d, persistence=%b, snapshotPath='%s', verbose=%b}",
                port, bossThreads, workerThreads,
                maxConnections, cacheCapacity, evictionPolicy,
                sweepIntervalMs, persistenceEnabled, snapshotPath, verbose
        );
    }

    // =========================================================================
    // Builder
    // =========================================================================

    /**
     * Builder for ServerConfig.
     *
     * All fields are pre-initialised to their defaults, so calling .build()
     * immediately gives you a valid all-defaults config. Override only what
     * you need to change.
     *
     * Every setter validates its input and throws IllegalArgumentException
     * for obviously wrong values (negative port, zero threads, etc.).
     * This catches misconfiguration at startup, not at runtime.
     */
    public static final class Builder {

        // Pre-initialise all fields to defaults
        private int             port               = DEFAULT_PORT;
        private int             bossThreads        = DEFAULT_BOSS_THREADS;
        private int             workerThreads      = DEFAULT_WORKER_THREADS;
        private int             maxConnections     = DEFAULT_MAX_CONNECTIONS;
        private int             cacheCapacity      = DEFAULT_CACHE_CAPACITY;
        private CachePolicyType evictionPolicy     = DEFAULT_POLICY;
        private long            sweepIntervalMs    = DEFAULT_SWEEP_INTERVAL_MS;
        private boolean         persistenceEnabled = DEFAULT_PERSISTENCE_ENABLED;
        private String          snapshotPath       = DEFAULT_SNAPSHOT_PATH;
        private boolean         verbose            = DEFAULT_VERBOSE;

        /** Package-private — use ServerConfig.builder() */
        Builder() {}

        /**
         * Sets the TCP port.
         * @param port Port number, must be 1-65535.
         * @return this Builder.
         */
        public Builder port(int port) {
            // Port 0 is valid — tells the OS to assign any free ephemeral port.
            // Used in tests to avoid port collisions between test classes.
            // After bind, call CacheServer.getPort() for the actual assigned port.
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("Port must be 0-65535, got: " + port);
            }
            this.port = port;
            return this;
        }

        /**
         * Sets the number of Netty boss threads.
         * @param bossThreads Must be >= 1. Almost always 1.
         * @return this Builder.
         */
        public Builder bossThreads(int bossThreads) {
            if (bossThreads < 1) {
                throw new IllegalArgumentException("bossThreads must be >= 1");
            }
            this.bossThreads = bossThreads;
            return this;
        }

        /**
         * Sets the number of Netty worker threads.
         * @param workerThreads Must be >= 1.
         * @return this Builder.
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
         * @param maxConnections Must be >= 1.
         * @return this Builder.
         */
        public Builder maxConnections(int maxConnections) {
            if (maxConnections < 1) {
                throw new IllegalArgumentException("maxConnections must be >= 1");
            }
            this.maxConnections = maxConnections;
            return this;
        }

        public PersistenceConfig toPersistenceConfig() {
            return new PersistenceConfig(
                    persistenceEnabled,
                    snapshotPath,
                    300_000L,
                    100
            );
        }

        /**
         * Sets the maximum cache capacity (number of entries).
         * @param cacheCapacity Must be >= 1.
         * @return this Builder.
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
         * @param evictionPolicy Must not be null.
         * @return this Builder.
         */
        public Builder evictionPolicy(CachePolicyType evictionPolicy) {
            if (evictionPolicy == null) {
                throw new IllegalArgumentException("evictionPolicy cannot be null");
            }
            this.evictionPolicy = evictionPolicy;
            return this;
        }

        /**
         * Sets the TTL sweeper interval.
         * @param sweepIntervalMs Must be > 0.
         * @return this Builder.
         */
        public Builder sweepIntervalMs(long sweepIntervalMs) {
            if (sweepIntervalMs <= 0) {
                throw new IllegalArgumentException("sweepIntervalMs must be > 0");
            }
            this.sweepIntervalMs = sweepIntervalMs;
            return this;
        }

        /**
         * Enables or disables persistence (AOF + snapshots).
         * @param enabled true to enable persistence.
         * @return this Builder.
         */
        public Builder persistenceEnabled(boolean enabled) {
            this.persistenceEnabled = enabled;
            return this;
        }

        /**
         * Sets the snapshot file path.
         * @param snapshotPath Must not be null or blank.
         * @return this Builder.
         */
        public Builder snapshotPath(String snapshotPath) {
            if (snapshotPath == null || snapshotPath.isBlank()) {
                throw new IllegalArgumentException("snapshotPath cannot be blank");
            }
            this.snapshotPath = snapshotPath;
            return this;
        }

        /**
         * Enables or disables verbose logging.
         * @param verbose true to enable per-command logging.
         * @return this Builder.
         */
        public Builder verbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /**
         * Builds and returns an immutable ServerConfig.
         * All fields have been validated by individual setters.
         *
         * @return Fully constructed ServerConfig.
         */
        public ServerConfig build() {
            return new ServerConfig(this);
        }
    }

    // =========================================================================
    // Private static helpers for fromProperties()
    // =========================================================================

    private static int parseInt(Properties props, String key, int defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            System.err.printf(
                    "[ServerConfig] Invalid int for '%s': '%s', using default %d%n",
                    key, value, defaultValue);
            return defaultValue;
        }
    }

    private static long parseLong(Properties props, String key, long defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            System.err.printf(
                    "[ServerConfig] Invalid long for '%s': '%s', using default %d%n",
                    key, value, defaultValue);
            return defaultValue;
        }
    }

    private static boolean parseBoolean(Properties props, String key, boolean defaultValue) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) return defaultValue;
        return Boolean.parseBoolean(value.trim());
    }
}