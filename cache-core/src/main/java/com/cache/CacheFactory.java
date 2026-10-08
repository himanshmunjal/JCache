package com.cache;

import com.cache.api.Cache;
import com.cache.api.CachePolicyType;
import com.cache.api.CacheStats;
import com.cache.policy.ARCCache;
import com.cache.policy.LFUCache;
import com.cache.policy.LRUCache;
import com.cache.stats.CacheMetricsCollector;
import com.cache.ttl.TTLCache;

import java.time.Duration;

/**
 * Entry point for building caches.
 *
 * <p>The static methods cover the common cases:
 * <pre>{@code
 * Cache<String, User> users = CacheFactory.arc(10_000);
 * }</pre>
 * and the builder composes the optional decorators:
 * <pre>{@code
 * Cache<String, Session> sessions = CacheFactory
 *         .<String, Session>builder(CachePolicyType.LRU, 50_000)
 *         .withTTL(Duration.ofMinutes(30))
 *         .withMetrics("sessions")
 *         .build();
 * }</pre>
 *
 * <p>Decorators are applied as metrics, then TTL, then the policy cache.
 * Keeping metrics outermost means a read of an expired key is recorded as a
 * miss, which is what the caller observed.
 *
 * <p>None of the returned caches are thread-safe; see {@code com.cache.concurrent}.
 */
public final class CacheFactory {

    private CacheFactory() {
        throw new AssertionError("CacheFactory has only static methods");
    }

    /**
     * Creates an LRU cache.
     *
     * @param capacity maximum number of entries
     * @param <K>      key type
     * @param <V>      value type
     * @return a new cache
     */
    public static <K, V> Cache<K, V> lru(int capacity) {
        return withPolicy(CachePolicyType.LRU, capacity);
    }

    /**
     * Creates an LFU cache.
     *
     * @param capacity maximum number of entries
     * @param <K>      key type
     * @param <V>      value type
     * @return a new cache
     */
    public static <K, V> Cache<K, V> lfu(int capacity) {
        return withPolicy(CachePolicyType.LFU, capacity);
    }

    /**
     * Creates an ARC cache. A good default when the access pattern is unknown.
     *
     * @param capacity maximum number of entries
     * @param <K>      key type
     * @param <V>      value type
     * @return a new cache
     */
    public static <K, V> Cache<K, V> arc(int capacity) {
        return withPolicy(CachePolicyType.ARC, capacity);
    }

    /**
     * Creates a cache for a policy chosen at runtime, for example from configuration.
     *
     * @param policy   the eviction policy
     * @param capacity maximum number of entries
     * @param <K>      key type
     * @param <V>      value type
     * @return a new cache
     */
    public static <K, V> Cache<K, V> withPolicy(CachePolicyType policy, int capacity) {
        if (policy == null) {
            throw new IllegalArgumentException("Policy cannot be null");
        }
        validateCapacity(capacity);
        return switch (policy) {
            case LRU -> new LRUCache<>(capacity);
            case LFU -> new LFUCache<>(capacity);
            case ARC -> new ARCCache<>(capacity);
        };
    }

    /**
     * Adds expiry to an existing cache.
     *
     * @param delegate   the cache to wrap
     * @param defaultTtl TTL for entries stored with {@link Cache#put}; {@link Duration#ZERO} for none
     * @param <K>        key type
     * @param <V>        value type
     * @return the wrapping cache; call {@link TTLCache#shutdown()} when done with it
     */
    public static <K, V> TTLCache<K, V> withTTL(Cache<K, V> delegate, Duration defaultTtl) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate cannot be null");
        }
        validateTtl(defaultTtl);
        return new TTLCache<>(delegate, TTLCache.DEFAULT_SWEEP_INTERVAL_MS, defaultTtl);
    }

    /**
     * Adds metrics collection to an existing cache.
     *
     * @param delegate  the cache to wrap
     * @param cacheName name used in metrics output
     * @param <K>       key type
     * @param <V>       value type
     * @return the wrapping cache
     */
    public static <K, V> Cache<K, V> withMetrics(Cache<K, V> delegate, String cacheName) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate cannot be null");
        }
        if (cacheName == null) {
            throw new IllegalArgumentException("Cache name cannot be null");
        }
        return new MetricsCollectingCache<>(delegate, new CacheMetricsCollector(cacheName));
    }

    /**
     * Starts a builder for a cache with optional TTL and metrics.
     *
     * @param policy   the eviction policy
     * @param capacity maximum number of entries
     * @param <K>      key type
     * @param <V>      value type
     * @return a new builder
     */
    public static <K, V> Builder<K, V> builder(CachePolicyType policy, int capacity) {
        if (policy == null) {
            throw new IllegalArgumentException("Policy cannot be null");
        }
        validateCapacity(capacity);
        return new Builder<>(policy, capacity);
    }

    /**
     * Fluent builder returned by {@link CacheFactory#builder}.
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static final class Builder<K, V> {

        private final CachePolicyType policy;
        private final int capacity;
        private Duration ttl;
        private long sweepIntervalMs = TTLCache.DEFAULT_SWEEP_INTERVAL_MS;
        private String metricsName;

        Builder(CachePolicyType policy, int capacity) {
            this.policy = policy;
            this.capacity = capacity;
        }

        /**
         * Adds expiry with the given default TTL.
         *
         * @param ttl default TTL; {@link Duration#ZERO} adds expiry support without a default
         * @return this builder
         */
        public Builder<K, V> withTTL(Duration ttl) {
            validateTtl(ttl);
            this.ttl = ttl;
            return this;
        }

        /**
         * Sets how often expired entries are swept. Only used together with {@link #withTTL}.
         *
         * @param sweepIntervalMs interval in milliseconds
         * @return this builder
         */
        public Builder<K, V> withSweepInterval(long sweepIntervalMs) {
            if (sweepIntervalMs <= 0) {
                throw new IllegalArgumentException("Sweep interval must be positive");
            }
            this.sweepIntervalMs = sweepIntervalMs;
            return this;
        }

        /**
         * Adds metrics collection.
         *
         * @param cacheName name used in metrics output
         * @return this builder
         */
        public Builder<K, V> withMetrics(String cacheName) {
            if (cacheName == null || cacheName.isBlank()) {
                throw new IllegalArgumentException("Metrics cache name cannot be null or blank");
            }
            this.metricsName = cacheName;
            return this;
        }

        /**
         * Builds the cache.
         *
         * @return the configured cache
         */
        public Cache<K, V> build() {
            Cache<K, V> cache = withPolicy(policy, capacity);
            if (ttl != null) {
                cache = new TTLCache<>(cache, sweepIntervalMs, ttl);
            }
            if (metricsName != null) {
                cache = new MetricsCollectingCache<>(cache, new CacheMetricsCollector(metricsName));
            }
            return cache;
        }
    }

    /**
     * Decorator that records every operation in a {@link CacheMetricsCollector}.
     *
     * @param <K> key type
     * @param <V> value type
     */
    public static final class MetricsCollectingCache<K, V> implements Cache<K, V> {

        private final Cache<K, V> delegate;
        private final CacheMetricsCollector metrics;

        /**
         * Wraps {@code delegate}.
         *
         * @param delegate the cache to wrap
         * @param metrics  where to record operations
         */
        public MetricsCollectingCache(Cache<K, V> delegate, CacheMetricsCollector metrics) {
            if (delegate == null) {
                throw new IllegalArgumentException("Delegate cannot be null");
            }
            if (metrics == null) {
                throw new IllegalArgumentException("Metrics cannot be null");
            }
            this.delegate = delegate;
            this.metrics = metrics;
        }

        @Override
        public V get(K key) {
            long start = metrics.startTimer();
            V value = delegate.get(key);
            if (value != null) {
                metrics.recordHit(start);
            } else {
                metrics.recordMiss(start);
            }
            return value;
        }

        @Override
        public V peek(K key) {
            return delegate.peek(key);
        }

        @Override
        public void put(K key, V value) {
            metrics.recordPut();
            long before = delegate.getStats().evictions();
            delegate.put(key, value);
            for (long i = delegate.getStats().evictions() - before; i > 0; i--) {
                metrics.recordEviction();
            }
        }

        @Override
        public void evict(K key) {
            metrics.recordManualEviction();
            delegate.evict(key);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        /** Returns the collector's counters rather than the delegate's. */
        @Override
        public CacheStats getStats() {
            return metrics.toCacheStats();
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        /**
         * Returns all collected metrics, including latency percentiles.
         *
         * @return the current snapshot
         */
        public CacheMetricsCollector.MetricsSnapshot getMetricsSnapshot() {
            return metrics.snapshot();
        }

        /**
         * Returns the underlying collector, for example to reset it.
         *
         * @return the collector
         */
        public CacheMetricsCollector getMetricsCollector() {
            return metrics;
        }
    }

    private static void validateCapacity(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Cache capacity must be greater than 0, got: " + capacity);
        }
    }

    private static void validateTtl(Duration ttl) {
        if (ttl == null) {
            throw new IllegalArgumentException("TTL cannot be null");
        }
        if (ttl.isNegative()) {
            throw new IllegalArgumentException("TTL cannot be negative");
        }
    }
}
