package com.cache.server;

import com.cache.api.CachePolicyType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Immutable server configuration.
 *
 * <p>Values can come from a properties file, environment variables or
 * command-line flags. {@link CacheServer#main} applies them in that order, so
 * later sources win:
 * <pre>
 * built-in defaults &lt; properties file &lt; environment &lt; command line
 * </pre>
 *
 * <table>
 *   <caption>Settings</caption>
 *   <tr><th>Property</th><th>Environment variable</th><th>Default</th></tr>
 *   <tr><td>server.port</td><td>JCACHE_PORT</td><td>6379</td></tr>
 *   <tr><td>server.boss.threads</td><td>JCACHE_BOSS_THREADS</td><td>1</td></tr>
 *   <tr><td>server.worker.threads</td><td>JCACHE_THREADS</td><td>8</td></tr>
 *   <tr><td>server.max.connections</td><td>JCACHE_MAX_CONNECTIONS</td><td>1000</td></tr>
 *   <tr><td>server.rate.limit</td><td>JCACHE_RATE_LIMIT</td><td>0 (off)</td></tr>
 *   <tr><td>server.rate.limit.burst</td><td>JCACHE_RATE_LIMIT_BURST</td><td>0 (same as rate)</td></tr>
 *   <tr><td>server.metrics.port</td><td>JCACHE_METRICS_PORT</td><td>-1 (off)</td></tr>
 *   <tr><td>server.verbose</td><td>JCACHE_VERBOSE</td><td>false</td></tr>
 *   <tr><td>cache.capacity</td><td>JCACHE_CAPACITY</td><td>10000</td></tr>
 *   <tr><td>cache.policy</td><td>JCACHE_POLICY</td><td>LRU</td></tr>
 *   <tr><td>cache.segments</td><td>JCACHE_SEGMENTS</td><td>16</td></tr>
 *   <tr><td>cache.default.ttl</td><td>JCACHE_DEFAULT_TTL</td><td>0 (no expiry)</td></tr>
 *   <tr><td>cache.sweep.interval.ms</td><td>JCACHE_SWEEP_INTERVAL</td><td>500</td></tr>
 *   <tr><td>persistence.enabled</td><td>JCACHE_PERSISTENCE_ENABLED</td><td>false</td></tr>
 *   <tr><td>persistence.snapshot.path</td><td>JCACHE_SNAPSHOT_PATH</td><td>./jcache-data</td></tr>
 *   <tr><td>persistence.snapshot.interval.ms</td><td>JCACHE_SNAPSHOT_INTERVAL_MS</td><td>300000</td></tr>
 * </table>
 */
public final class ServerConfig {

    /** Default TCP port, the same as Redis. */
    public static final int DEFAULT_PORT = 6379;
    /** Default number of acceptor threads. */
    public static final int DEFAULT_BOSS_THREADS = 1;
    /** Default number of I/O threads. */
    public static final int DEFAULT_WORKER_THREADS = 8;
    /** Default connection limit. */
    public static final int DEFAULT_MAX_CONNECTIONS = 1000;
    /** Default number of entries. */
    public static final int DEFAULT_CACHE_CAPACITY = 10_000;
    /** Default eviction policy. */
    public static final CachePolicyType DEFAULT_POLICY = CachePolicyType.LRU;
    /** Default number of lock segments. */
    public static final int DEFAULT_SEGMENTS = 16;
    /** Default TTL in seconds; 0 means keys do not expire. */
    public static final long DEFAULT_TTL_SECONDS = 0;
    /** Default interval of the TTL sweeper. */
    public static final long DEFAULT_SWEEP_INTERVAL_MS = 500L;
    /** Persistence is off unless enabled. */
    public static final boolean DEFAULT_PERSISTENCE_ENABLED = false;
    /** Default data directory. */
    public static final String DEFAULT_SNAPSHOT_PATH = "./jcache-data";
    /** Default interval between snapshots. */
    public static final long DEFAULT_SNAPSHOT_INTERVAL_MS = 300_000L;
    /** Commands per second per connection; 0 turns rate limiting off. */
    public static final int DEFAULT_RATE_LIMIT = 0;
    /** Burst size per connection; 0 means the same as the rate. */
    public static final int DEFAULT_RATE_LIMIT_BURST = 0;
    /** Port of the Prometheus endpoint; -1 turns it off. */
    public static final int DEFAULT_METRICS_PORT = -1;
    /** Per-command logging is off unless enabled. */
    public static final boolean DEFAULT_VERBOSE = false;

    private final int port;
    private final int bossThreads;
    private final int workerThreads;
    private final int maxConnections;
    private final int rateLimitPerSecond;
    private final int rateLimitBurst;
    private final int cacheCapacity;
    private final CachePolicyType evictionPolicy;
    private final int segments;
    private final long defaultTtlSeconds;
    private final long sweepIntervalMs;
    private final boolean persistenceEnabled;
    private final String snapshotPath;
    private final long snapshotIntervalMs;
    private final int metricsPort;
    private final boolean verbose;

    private ServerConfig(Builder b) {
        this.port = b.port;
        this.bossThreads = b.bossThreads;
        this.workerThreads = b.workerThreads;
        this.maxConnections = b.maxConnections;
        this.rateLimitPerSecond = b.rateLimitPerSecond;
        this.rateLimitBurst = b.rateLimitBurst;
        this.cacheCapacity = b.cacheCapacity;
        this.evictionPolicy = b.evictionPolicy;
        this.segments = b.segments;
        this.defaultTtlSeconds = b.defaultTtlSeconds;
        this.sweepIntervalMs = b.sweepIntervalMs;
        this.persistenceEnabled = b.persistenceEnabled;
        this.snapshotPath = b.snapshotPath;
        this.snapshotIntervalMs = b.snapshotIntervalMs;
        this.metricsPort = b.metricsPort;
        this.verbose = b.verbose;
    }

    /** @return a builder initialised with the defaults */
    public static Builder builder() {
        return new Builder();
    }

    /** @return the default configuration */
    public static ServerConfig defaults() {
        return builder().build();
    }

    /**
     * Reads a properties file on top of the defaults.
     *
     * @param path path of the file
     * @return the configuration
     */
    public static ServerConfig fromProperties(String path) {
        return builder().applyProperties(path).build();
    }

    /**
     * Reads {@code JCACHE_*} environment variables on top of the defaults.
     *
     * @return the configuration
     */
    public static ServerConfig fromEnvironment() {
        return builder().applyEnvironment(System.getenv()).build();
    }

    /** @return a builder holding this configuration's values */
    public Builder toBuilder() {
        return new Builder()
                .port(port)
                .bossThreads(bossThreads)
                .workerThreads(workerThreads)
                .maxConnections(maxConnections)
                .rateLimitPerSecond(rateLimitPerSecond)
                .rateLimitBurst(rateLimitBurst)
                .cacheCapacity(cacheCapacity)
                .evictionPolicy(evictionPolicy)
                .segments(segments)
                .defaultTtlSeconds(defaultTtlSeconds)
                .sweepIntervalMs(sweepIntervalMs)
                .persistenceEnabled(persistenceEnabled)
                .snapshotPath(snapshotPath)
                .snapshotIntervalMs(snapshotIntervalMs)
                .metricsPort(metricsPort)
                .verbose(verbose);
    }

    /** @return the TCP port; 0 means any free port */
    public int getPort() {
        return port;
    }

    /** @return number of acceptor threads */
    public int getBossThreads() {
        return bossThreads;
    }

    /** @return number of I/O threads */
    public int getWorkerThreads() {
        return workerThreads;
    }

    /** @return maximum number of open client connections */
    public int getMaxConnections() {
        return maxConnections;
    }

    /** @return commands per second allowed on each connection; 0 if unlimited */
    public int getRateLimitPerSecond() {
        return rateLimitPerSecond;
    }

    /** @return commands a connection may send at once before the rate applies */
    public int getRateLimitBurst() {
        return rateLimitBurst > 0 ? rateLimitBurst : rateLimitPerSecond;
    }

    /** @return maximum number of cached entries */
    public int getCacheCapacity() {
        return cacheCapacity;
    }

    /** @return the eviction policy */
    public CachePolicyType getEvictionPolicy() {
        return evictionPolicy;
    }

    /** @return number of lock segments */
    public int getSegments() {
        return segments;
    }

    /** @return TTL in seconds applied to writes that do not give one; 0 for none */
    public long getDefaultTtlSeconds() {
        return defaultTtlSeconds;
    }

    /** @return interval of the TTL sweeper */
    public long getSweepIntervalMs() {
        return sweepIntervalMs;
    }

    /** @return whether snapshot and AOF persistence is enabled */
    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    /** @return the data directory used for persistence */
    public String getSnapshotPath() {
        return snapshotPath;
    }

    /** @return interval between snapshots */
    public long getSnapshotIntervalMs() {
        return snapshotIntervalMs;
    }

    /** @return port of the Prometheus endpoint, 0 for any free port, -1 if it is off */
    public int getMetricsPort() {
        return metricsPort;
    }

    /** @return whether every command is logged */
    public boolean isVerbose() {
        return verbose;
    }

    @Override
    public String toString() {
        return String.format("ServerConfig{port=%d, bossThreads=%d, workerThreads=%d, maxConnections=%d, "
                        + "rateLimit=%d, rateLimitBurst=%d, capacity=%d, policy=%s, segments=%d, defaultTtl=%ds, sweepIntervalMs=%d, "
                        + "persistence=%b, snapshotPath='%s', snapshotIntervalMs=%d, metricsPort=%d, verbose=%b}",
                port, bossThreads, workerThreads, maxConnections, rateLimitPerSecond, rateLimitBurst,
                cacheCapacity, evictionPolicy, segments,
                defaultTtlSeconds, sweepIntervalMs, persistenceEnabled, snapshotPath, snapshotIntervalMs, metricsPort, verbose);
    }

    /** Builder for {@link ServerConfig}. Setters validate their argument. */
    public static final class Builder {
        private int port = DEFAULT_PORT;
        private int bossThreads = DEFAULT_BOSS_THREADS;
        private int workerThreads = DEFAULT_WORKER_THREADS;
        private int maxConnections = DEFAULT_MAX_CONNECTIONS;
        private int rateLimitPerSecond = DEFAULT_RATE_LIMIT;
        private int rateLimitBurst = DEFAULT_RATE_LIMIT_BURST;
        private int cacheCapacity = DEFAULT_CACHE_CAPACITY;
        private CachePolicyType evictionPolicy = DEFAULT_POLICY;
        private int segments = DEFAULT_SEGMENTS;
        private long defaultTtlSeconds = DEFAULT_TTL_SECONDS;
        private long sweepIntervalMs = DEFAULT_SWEEP_INTERVAL_MS;
        private boolean persistenceEnabled = DEFAULT_PERSISTENCE_ENABLED;
        private String snapshotPath = DEFAULT_SNAPSHOT_PATH;
        private long snapshotIntervalMs = DEFAULT_SNAPSHOT_INTERVAL_MS;
        private int metricsPort = DEFAULT_METRICS_PORT;
        private boolean verbose = DEFAULT_VERBOSE;

        Builder() {
        }

        /**
         * @param port TCP port, 0 for any free port
         * @return this builder
         */
        public Builder port(int port) {
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("Port must be 0-65535, got: " + port);
            }
            this.port = port;
            return this;
        }

        /**
         * @param bossThreads number of acceptor threads, at least 1
         * @return this builder
         */
        public Builder bossThreads(int bossThreads) {
            this.bossThreads = requirePositive(bossThreads, "bossThreads");
            return this;
        }

        /**
         * @param workerThreads number of I/O threads, at least 1
         * @return this builder
         */
        public Builder workerThreads(int workerThreads) {
            this.workerThreads = requirePositive(workerThreads, "workerThreads");
            return this;
        }

        /**
         * @param maxConnections connection limit, at least 1
         * @return this builder
         */
        public Builder maxConnections(int maxConnections) {
            this.maxConnections = requirePositive(maxConnections, "maxConnections");
            return this;
        }

        /**
         * @param perSecond commands per second per connection, 0 for no limit
         * @return this builder
         */
        public Builder rateLimitPerSecond(int perSecond) {
            this.rateLimitPerSecond = requireNonNegative(perSecond, "rateLimitPerSecond");
            return this;
        }

        /**
         * @param burst commands a connection may send at once, 0 for the same as the rate
         * @return this builder
         */
        public Builder rateLimitBurst(int burst) {
            this.rateLimitBurst = requireNonNegative(burst, "rateLimitBurst");
            return this;
        }

        /**
         * @param cacheCapacity maximum number of entries, at least 1
         * @return this builder
         */
        public Builder cacheCapacity(int cacheCapacity) {
            this.cacheCapacity = requirePositive(cacheCapacity, "cacheCapacity");
            return this;
        }

        /**
         * @param evictionPolicy the eviction policy
         * @return this builder
         */
        public Builder evictionPolicy(CachePolicyType evictionPolicy) {
            if (evictionPolicy == null) {
                throw new IllegalArgumentException("evictionPolicy cannot be null");
            }
            this.evictionPolicy = evictionPolicy;
            return this;
        }

        /**
         * @param policyName LRU, LFU or ARC, in any case
         * @return this builder
         */
        public Builder evictionPolicy(String policyName) {
            return evictionPolicy(parsePolicy(policyName));
        }

        /**
         * @param segments number of lock segments, a power of two
         * @return this builder
         */
        public Builder segments(int segments) {
            if (segments <= 0 || Integer.bitCount(segments) != 1) {
                throw new IllegalArgumentException("segments must be a positive power of 2, got: " + segments);
            }
            this.segments = segments;
            return this;
        }

        /**
         * @param seconds TTL for writes that do not give one, 0 for none
         * @return this builder
         */
        public Builder defaultTtlSeconds(long seconds) {
            if (seconds < 0) {
                throw new IllegalArgumentException("defaultTtlSeconds must be >= 0, got: " + seconds);
            }
            this.defaultTtlSeconds = seconds;
            return this;
        }

        /**
         * @param sweepIntervalMs interval of the TTL sweeper, positive
         * @return this builder
         */
        public Builder sweepIntervalMs(long sweepIntervalMs) {
            if (sweepIntervalMs <= 0) {
                throw new IllegalArgumentException("sweepIntervalMs must be > 0");
            }
            this.sweepIntervalMs = sweepIntervalMs;
            return this;
        }

        /**
         * @param enabled whether to persist the cache to disk
         * @return this builder
         */
        public Builder persistenceEnabled(boolean enabled) {
            this.persistenceEnabled = enabled;
            return this;
        }

        /**
         * @param snapshotPath data directory for persistence
         * @return this builder
         */
        public Builder snapshotPath(String snapshotPath) {
            if (snapshotPath == null || snapshotPath.isBlank()) {
                throw new IllegalArgumentException("snapshotPath cannot be blank");
            }
            this.snapshotPath = snapshotPath;
            return this;
        }

        /**
         * @param snapshotIntervalMs interval between snapshots, 0 to only snapshot on shutdown
         * @return this builder
         */
        public Builder snapshotIntervalMs(long snapshotIntervalMs) {
            if (snapshotIntervalMs < 0) {
                throw new IllegalArgumentException("snapshotIntervalMs must be >= 0");
            }
            this.snapshotIntervalMs = snapshotIntervalMs;
            return this;
        }

        /**
         * @param metricsPort port of the Prometheus endpoint, 0 for any free port, -1 to turn it off
         * @return this builder
         */
        public Builder metricsPort(int metricsPort) {
            if (metricsPort < -1 || metricsPort > 65535) {
                throw new IllegalArgumentException("metricsPort must be -1 (off) or 0-65535, got: " + metricsPort);
            }
            this.metricsPort = metricsPort;
            return this;
        }

        /**
         * @param verbose whether to log every command
         * @return this builder
         */
        public Builder verbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /**
         * Overrides values with those present in a properties file.
         *
         * @param path path of the file
         * @return this builder
         */
        public Builder applyProperties(String path) {
            Properties props = new Properties();
            try (InputStream in = Files.newInputStream(Path.of(path))) {
                props.load(in);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not read config file " + path, e);
            }
            return apply(props::getProperty, "server.port", "server.boss.threads", "server.worker.threads",
                    "server.max.connections", "server.verbose", "cache.capacity", "cache.policy",
                    "cache.segments", "cache.default.ttl", "cache.sweep.interval.ms", "persistence.enabled",
                    "persistence.snapshot.path", "persistence.snapshot.interval.ms", "server.rate.limit",
                    "server.rate.limit.burst", "server.metrics.port");
        }

        /**
         * Overrides values with the {@code JCACHE_*} variables present in {@code env}.
         *
         * @param env environment variables, usually {@link System#getenv()}
         * @return this builder
         */
        public Builder applyEnvironment(Map<String, String> env) {
            return apply(env::get, "JCACHE_PORT", "JCACHE_BOSS_THREADS", "JCACHE_THREADS",
                    "JCACHE_MAX_CONNECTIONS", "JCACHE_VERBOSE", "JCACHE_CAPACITY", "JCACHE_POLICY",
                    "JCACHE_SEGMENTS", "JCACHE_DEFAULT_TTL", "JCACHE_SWEEP_INTERVAL", "JCACHE_PERSISTENCE_ENABLED",
                    "JCACHE_SNAPSHOT_PATH", "JCACHE_SNAPSHOT_INTERVAL_MS", "JCACHE_RATE_LIMIT",
                    "JCACHE_RATE_LIMIT_BURST", "JCACHE_METRICS_PORT");
        }

        /** @return the configuration */
        public ServerConfig build() {
            return new ServerConfig(this);
        }

        /** Applies the settings in the fixed order of {@code names}; missing ones are skipped. */
        private Builder apply(Function<String, String> source, String... names) {
            set(source, names[0], v -> port(Integer.parseInt(v)));
            set(source, names[1], v -> bossThreads(Integer.parseInt(v)));
            set(source, names[2], v -> workerThreads(Integer.parseInt(v)));
            set(source, names[3], v -> maxConnections(Integer.parseInt(v)));
            set(source, names[4], v -> verbose(Boolean.parseBoolean(v)));
            set(source, names[5], v -> cacheCapacity(Integer.parseInt(v)));
            set(source, names[6], v -> evictionPolicy(parsePolicy(v)));
            set(source, names[7], v -> segments(Integer.parseInt(v)));
            set(source, names[8], v -> defaultTtlSeconds(Long.parseLong(v)));
            set(source, names[9], v -> sweepIntervalMs(Long.parseLong(v)));
            set(source, names[10], v -> persistenceEnabled(Boolean.parseBoolean(v)));
            set(source, names[11], this::snapshotPath);
            set(source, names[12], v -> snapshotIntervalMs(Long.parseLong(v)));
            set(source, names[13], v -> rateLimitPerSecond(Integer.parseInt(v)));
            set(source, names[14], v -> rateLimitBurst(Integer.parseInt(v)));
            set(source, names[15], v -> metricsPort(Integer.parseInt(v)));
            return this;
        }

        private static void set(Function<String, String> source, String name, Consumer<String> setter) {
            String raw = source.apply(name);
            if (raw == null || raw.isBlank()) {
                return;
            }
            try {
                setter.accept(raw.trim());
            } catch (IllegalArgumentException e) {
                // NumberFormatException is an IllegalArgumentException too.
                throw new IllegalArgumentException("Invalid value for " + name + ": '" + raw + "'", e);
            }
        }

        private static CachePolicyType parsePolicy(String value) {
            try {
                return CachePolicyType.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown policy, expected LRU, LFU or ARC");
            }
        }

        private static int requireNonNegative(int value, String name) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must be >= 0, got: " + value);
            }
            return value;
        }

        private static int requirePositive(int value, String name) {
            if (value < 1) {
                throw new IllegalArgumentException(name + " must be >= 1, got: " + value);
            }
            return value;
        }
    }
}
