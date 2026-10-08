package com.cache.server.handler;

import com.cache.api.Cache;
import com.cache.persistence.PersistenceManager;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import com.cache.ttl.TTLCache;

import java.util.function.LongSupplier;

/**
 * Runs commands against the cache, independent of the wire protocol. Both
 * {@link CacheServerHandler} and {@link RespCommandHandler} format the results.
 *
 * <p>One instance is created per connection because it holds that
 * connection's rate limiter. The cache, metrics and persistence manager are
 * shared and must be thread-safe.
 */
final class CommandExecutor {

    /** Outcome of a command that changes an existing key. */
    enum KeyResult { ABSENT, UNCHANGED, CHANGED }

    private final Cache<String, String> cache;
    private final TTLCache<String, String> ttlCache;
    private final ServerMetrics metrics;
    private final ServerConfig config;
    private final PersistenceManager persistence;
    private final TokenBucket rateLimit;

    CommandExecutor(Cache<String, String> cache, ServerMetrics metrics, ServerConfig config,
                    PersistenceManager persistence, LongSupplier nanoClock) {
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        if (metrics == null) {
            throw new IllegalArgumentException("metrics cannot be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        this.cache = cache;
        this.ttlCache = cache instanceof TTLCache<String, String> t ? t : null;
        this.metrics = metrics;
        this.config = config;
        this.persistence = persistence;
        this.rateLimit = config.getRateLimitPerSecond() > 0
                ? new TokenBucket(config.getRateLimitPerSecond(), config.getRateLimitBurst(), nanoClock)
                : null;
    }

    ServerConfig config() {
        return config;
    }

    ServerMetrics metrics() {
        return metrics;
    }

    /** @return {@code false} if the connection is over its rate limit; the rejection is recorded */
    boolean tryAcquire() {
        if (rateLimit == null || rateLimit.tryAcquire()) {
            return true;
        }
        metrics.recordRateLimited();
        return false;
    }

    /** @return the value, or {@code null} if the key is absent */
    String get(String key) {
        long start = System.nanoTime();
        String value = cache.get(key);
        metrics.recordGet(value != null, System.nanoTime() - start);
        return value;
    }

    /**
     * Stores a value.
     *
     * @param ttlSeconds TTL in seconds; 0 uses the server's default TTL
     * @throws UnsupportedOperationException if a TTL applies but the cache cannot expire keys
     */
    void put(String key, String value, long ttlSeconds) {
        long ttl = ttlSeconds > 0 ? ttlSeconds : config.getDefaultTtlSeconds();
        if (ttl > 0) {
            requireTtl();
        }
        long start = System.nanoTime();
        mutate(() -> {
            if (ttlCache != null) {
                ttlCache.put(key, value, ttl);
            } else {
                cache.put(key, value);
            }
            if (persistence != null) {
                persistence.logPut(key, value, ttl > 0 ? System.currentTimeMillis() + ttl * 1000 : -1L);
            }
        });
        metrics.recordPut(System.nanoTime() - start);
    }

    /** @return whether the key existed */
    boolean delete(String key) {
        long start = System.nanoTime();
        boolean[] existed = new boolean[1];
        mutate(() -> {
            existed[0] = cache.peek(key) != null;
            cache.evict(key);
            if (persistence != null) {
                persistence.logDelete(key);
            }
        });
        metrics.recordDelete(System.nanoTime() - start);
        return existed[0];
    }

    boolean exists(String key) {
        return cache.peek(key) != null;
    }

    /**
     * Sets a new TTL on an existing key; 0 deletes it.
     *
     * @return {@code false} if the key does not exist
     */
    boolean expire(String key, long ttlSeconds) {
        requireTtl();
        boolean[] found = new boolean[1];
        mutate(() -> {
            String value = ttlCache.peek(key);
            found[0] = ttlCache.expire(key, ttlSeconds);
            if (found[0] && persistence != null) {
                if (ttlSeconds == 0) {
                    persistence.logDelete(key);
                } else {
                    persistence.logPut(key, value, System.currentTimeMillis() + ttlSeconds * 1000);
                }
            }
        });
        return found[0];
    }

    /** @return seconds left, {@code -1} if the key never expires, {@code -2} if it is absent */
    long ttl(String key) {
        requireTtl();
        if (!ttlCache.containsKey(key)) {
            return -2;
        }
        return ttlCache.getRemainingTTL(key);
    }

    /** Removes a key's TTL. */
    KeyResult persist(String key) {
        requireTtl();
        KeyResult[] result = {KeyResult.ABSENT};
        mutate(() -> {
            String value = ttlCache.peek(key);
            if (value == null) {
                return;
            }
            if (ttlCache.persist(key)) {
                result[0] = KeyResult.CHANGED;
                if (persistence != null) {
                    persistence.logPut(key, value, -1L);
                }
            } else {
                result[0] = KeyResult.UNCHANGED;
            }
        });
        return result[0];
    }

    void flush() {
        mutate(() -> {
            cache.clear();
            if (persistence != null) {
                persistence.logClear();
            }
        });
    }

    int size() {
        return cache.size();
    }

    /** @return the STATS line, {@code name:value} pairs separated by spaces */
    String stats() {
        return metrics.toStatsString(cache.getStats().evictions(), cache.size());
    }

    private void requireTtl() {
        if (ttlCache == null) {
            throw new UnsupportedOperationException("TTL is not supported by this cache");
        }
    }

    private void mutate(Runnable mutation) {
        if (persistence != null) {
            persistence.atomically(mutation);
        } else {
            mutation.run();
        }
    }
}
