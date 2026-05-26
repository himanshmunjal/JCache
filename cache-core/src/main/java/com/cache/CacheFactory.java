package com.cache;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.api.EvictionPolicy;
import com.cache.api.CachePolicyType;
import com.cache.policy.ARCCache;
import com.cache.policy.LFUCache;
import com.cache.policy.LRUCache;
import com.cache.stats.CacheMetricsCollector;
import com.cache.ttl.TTLCache;

import java.time.Duration;

/**
 * CacheFactory — the single entry point for creating any cache configuration.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHY THIS EXISTS (Strategy Pattern)
 * ─────────────────────────────────────────────────────────────────────────────
 * Without a factory, callers must know the concrete types to construct caches:
 *
 *   // WITHOUT factory — caller is coupled to implementation
 *   Cache<K,V> c = new TTLCache<>(
 *       new MetricsCollectingCache<>(
 *           new LRUCache<>(100), collector
 *       ), sweepInterval
 *   );
 *
 * With a factory, that entire construction is encapsulated:
 *
 *   // WITH factory — clean, readable, policy-swappable
 *   Cache<K,V> c = CacheFactory.lru(100)
 *                              .withTTL(Duration.ofMinutes(5))
 *                              .withMetrics("my-cache")
 *                              .build();
 *
 * The factory owns all wiring decisions. Callers get a Cache<K,V> and never
 * know whether it's wrapped in TTL, metrics, or anything else.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TWO USAGE STYLES
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Style 1 — Quick static methods (80% of use cases):
 *   Cache<String,String> c = CacheFactory.lru(100);
 *   Cache<String,String> c = CacheFactory.lfu(100);
 *   Cache<String,String> c = CacheFactory.arc(100);
 *
 * Style 2 — Builder for composed configurations (20% of use cases):
 *   Cache<String,String> c = CacheFactory
 *       .builder(EvictionPolicy.LRU, 500)
 *       .withTTL(Duration.ofSeconds(60))
 *       .withMetrics("session-cache")
 *       .build();
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WRAPPER ORDER (matters for correctness)
 * ─────────────────────────────────────────────────────────────────────────────
 * When both TTL and metrics are applied, the order is:
 *
 *   MetricsWrapper → TTLCache → PolicyCache (LRU/LFU/ARC)
 *
 * This means metrics are recorded AFTER TTL eviction.
 * A get() on an expired key records a MISS in metrics — correct behaviour,
 * because the caller got nothing back regardless of what's in the policy cache.
 *
 * If metrics wrapped the policy cache directly (inside TTL), a get() on an
 * expired key would record a HIT (it exists in the policy cache) followed by
 * a return of null — misleading metrics.
 */
public final class CacheFactory {

    // Private constructor — this is a utility class, never instantiated.
    private CacheFactory() {
        throw new UnsupportedOperationException("CacheFactory is a utility class");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Quick static methods — Style 1
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Creates a plain LRU cache with the given capacity.
     * No TTL, no metrics. Use this for simple, in-process caching.
     *
     * @param capacity Maximum number of entries. Must be > 0.
     * @param <K>      Key type.
     * @param <V>      Value type.
     * @return A Cache backed by LRU eviction policy.
     */
    public static <K, V> Cache<K, V> lru(int capacity) {
        validateCapacity(capacity);
        return new LRUCache<>(capacity);
    }

    /**
     * Creates a plain LFU cache with the given capacity.
     * Evicts the least frequently used key. Better hit rate than LRU
     * on workloads with stable hot keys (Zipfian distribution).
     *
     * @param capacity Maximum number of entries. Must be > 0.
     */
    public static <K, V> Cache<K, V> lfu(int capacity) {
        validateCapacity(capacity);
        return new LFUCache<>(capacity);
    }

    /**
     * Creates a plain ARC cache with the given capacity.
     * Adaptive — balances recency (LRU) and frequency (LFU) automatically.
     * Best default choice when access pattern is unknown.
     *
     * @param capacity Maximum number of entries. Must be > 0.
     */
    public static <K, V> Cache<K, V> arc(int capacity) {
        validateCapacity(capacity);
        return new ARCCache<>(capacity);
    }

    /**
     * Creates a cache from an EvictionPolicy enum value.
     * Useful when the policy is determined at runtime (e.g., from config file).
     *
     * @param policy   Which eviction policy to use.
     * @param capacity Maximum number of entries. Must be > 0.
     */
    public static <K, V> Cache<K, V> withPolicy(CachePolicyType policy, int capacity) {
        validateCapacity(capacity);
        return switch (policy) {
            case LRU -> new LRUCache<>(capacity);
            case LFU -> new LFUCache<>(capacity);
            case ARC -> new ARCCache<>(capacity);
        };
    }

    /**
     * Wraps any existing cache with TTL support.
     * The wrapper intercepts get() to check expiry and runs a background
     * sweeper to clean expired keys.
     *
     * @param delegate The cache to wrap. Cannot be null.
     * @param ttl      Default TTL duration. Keys without explicit TTL use this.
     *                 Pass Duration.ZERO for no default TTL.
     * @param <K>      Key type.
     * @param <V>      Value type.
     * @return A TTLCache wrapping the delegate.
     */
    public static <K, V> TTLCache<K, V> withTTL(Cache<K, V> delegate, Duration ttl) {
        if (delegate == null) throw new IllegalArgumentException("Delegate cannot be null");
        if (ttl == null)      throw new IllegalArgumentException("TTL duration cannot be null");
        if (ttl.isNegative()) throw new IllegalArgumentException("TTL duration cannot be negative");
        return new TTLCache<>(delegate);
    }

    /**
     * Wraps any existing cache with metrics collection.
     * All get/put/evict calls are intercepted and counted.
     *
     * @param delegate  The cache to wrap. Cannot be null.
     * @param cacheName Name for this cache instance in metrics output.
     * @param <K>       Key type.
     * @param <V>       Value type.
     * @return A MetricsCollectingCache wrapping the delegate.
     */
    public static <K, V> Cache<K, V> withMetrics(Cache<K, V> delegate, String cacheName) {
        if (delegate == null)  throw new IllegalArgumentException("Delegate cannot be null");
        if (cacheName == null) throw new IllegalArgumentException("Cache name cannot be null");
        return new MetricsCollectingCache<>(delegate, new CacheMetricsCollector(cacheName));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Builder — Style 2
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns a builder for composing TTL, metrics, and other wrappers
     * onto a base policy cache.
     *
     * Example:
     *   Cache<String,User> cache = CacheFactory
     *       .builder(EvictionPolicy.ARC, 1000)
     *       .withTTL(Duration.ofMinutes(30))
     *       .withMetrics("user-cache")
     *       .build();
     *
     * @param policy   Eviction policy for the base cache.
     * @param capacity Capacity for the base cache. Must be > 0.
     */
    public static <K, V> Builder<K, V> builder(CachePolicyType policy, int capacity) {
        validateCapacity(capacity);
        return new Builder<>(policy, capacity);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Builder class
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Fluent builder for composing cache wrappers.
     *
     * Construction order (innermost to outermost):
     *   1. Policy cache  (LRU / LFU / ARC)      — always present
     *   2. TTLCache      (optional)              — wraps policy cache
     *   3. Metrics cache (optional)              — wraps TTLCache or policy cache
     *
     * @param <K> Key type.
     * @param <V> Value type.
     */
    public static final class Builder<K, V> {

        private final CachePolicyType policy;
        private final int capacity;

        // Optional wrapper configuration
        private Duration ttl         = null;   // null means no TTL wrapper
        private String   metricsName = null;   // null means no metrics wrapper
        private long     sweepIntervalMs = 500L; // default sweep interval for TTL

        /**
         * Package-private — created only via CacheFactory.builder().
         */
        Builder(CachePolicyType policy, int capacity) {
            this.policy   = policy;
            this.capacity = capacity;
        }

        /**
         * Add TTL expiry support with the given default duration.
         * Keys put via put(k, v) use this TTL.
         * Keys put via put(k, v, customTTL) use their own TTL.
         *
         * @param ttl Default TTL. Must be non-null and non-negative.
         *            Pass Duration.ZERO to add TTL infrastructure but no default expiry.
         */
        public Builder<K, V> withTTL(Duration ttl) {
            if (ttl == null)      throw new IllegalArgumentException("TTL cannot be null");
            if (ttl.isNegative()) throw new IllegalArgumentException("TTL cannot be negative");
            this.ttl = ttl;
            return this;
        }

        /**
         * Customise the TTL sweeper interval.
         * Only meaningful when withTTL() is also called.
         * Default is 500ms.
         *
         * @param sweepIntervalMs How often the sweeper runs. Must be > 0.
         */
        public Builder<K, V> withSweepInterval(long sweepIntervalMs) {
            if (sweepIntervalMs <= 0) {
                throw new IllegalArgumentException("Sweep interval must be positive");
            }
            this.sweepIntervalMs = sweepIntervalMs;
            return this;
        }

        /**
         * Add metrics collection with the given cache name.
         * The name appears in all metrics output and snapshots.
         *
         * @param cacheName Human-readable name, e.g. "product-catalog".
         */
        public Builder<K, V> withMetrics(String cacheName) {
            if (cacheName == null || cacheName.isBlank()) {
                throw new IllegalArgumentException("Metrics cache name cannot be null or blank");
            }
            this.metricsName = cacheName;
            return this;
        }

        /**
         * Constructs the final Cache<K,V> with all configured wrappers applied.
         *
         * Wrapper order (innermost to outermost):
         *   PolicyCache → [TTLCache] → [MetricsCache]
         *
         * @return A fully composed Cache<K,V> ready for use.
         */
        public Cache<K, V> build() {
            // Step 1: create the base policy cache.
            Cache<K, V> cache = switch (policy) {
                case LRU -> new LRUCache<>(capacity);
                case LFU -> new LFUCache<>(capacity);
                case ARC -> new ARCCache<>(capacity);
            };

            // Step 2: wrap with TTL if configured.
            if (ttl != null) {
                cache = new TTLCache<>(cache, sweepIntervalMs);
            }

            // Step 3: wrap with metrics if configured.
            // Metrics go OUTSIDE TTL so expired-key misses are counted correctly.
            if (metricsName != null) {
                cache = new MetricsCollectingCache<>(
                        cache,
                        new CacheMetricsCollector(metricsName)
                );
            }

            return cache;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // MetricsCollectingCache — inner decorator for metrics
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * A Cache<K,V> decorator that intercepts all operations and records
     * them in a CacheMetricsCollector.
     *
     * This is an inner class of CacheFactory because it's an implementation
     * detail of the factory's withMetrics() wiring. External code shouldn't
     * need to reference this class directly — they interact with Cache<K,V>.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     */
    public static final class MetricsCollectingCache<K, V> implements Cache<K, V> {

        private final Cache<K, V>             delegate;
        private final CacheMetricsCollector   metrics;

        /**
         * @param delegate The underlying cache to delegate all operations to.
         * @param metrics  The collector to record operations in.
         */
        public MetricsCollectingCache(Cache<K, V> delegate, CacheMetricsCollector metrics) {
            if (delegate == null) throw new IllegalArgumentException("Delegate cannot be null");
            if (metrics == null)  throw new IllegalArgumentException("Metrics cannot be null");
            this.delegate = delegate;
            this.metrics  = metrics;
        }

        /**
         * Intercepts get() to record hit/miss and measure latency.
         * The timer starts before the delegate call and stops after.
         */
        @Override
        public void clear() {
            delegate.clear();
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

        /**
         * Intercepts put() to record the operation.
         * Evictions triggered by this put() are recorded by the policy cache
         * itself (or its stats tracking) — we just count the put here.
         */
        @Override
        public void put(K key, V value) {
            metrics.recordPut();
            delegate.put(key, value);
        }

        /**
         * Intercepts evict() to record a manual eviction.
         */
        @Override
        public void evict(K key) {
            metrics.recordManualEviction();
            delegate.evict(key);
        }

        /** Delegates size() without recording — not a user-facing operation. */
        @Override
        public int size() {
            return delegate.size();
        }

        /**
         * Returns stats from the CacheMetricsCollector, not from the delegate.
         * The collector has richer data (latency percentiles, etc.) than the
         * delegate's built-in CacheStats.
         */
        @Override
        public CacheStats getstats() {
            return metrics.toCacheStats();
        }

        /**
         * Provides access to the full MetricsSnapshot.
         * Call this instead of getStats() when you want latency percentiles.
         *
         * @return Full MetricsSnapshot from the collector.
         */
        public CacheMetricsCollector.MetricsSnapshot getMetricsSnapshot() {
            return metrics.snapshot();
        }

        /**
         * Provides direct access to the underlying CacheMetricsCollector.
         * Useful for calling reset() or getting live counters.
         *
         * @return The CacheMetricsCollector backing this cache.
         */
        public CacheMetricsCollector getMetricsCollector() {
            return metrics;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Validates that a capacity value is legal (strictly positive).
     * Called at the start of every factory method to fail fast with a clear message.
     *
     * @param capacity The capacity to validate.
     * @throws IllegalArgumentException if capacity <= 0.
     */
    private static void validateCapacity(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException(
                    "Cache capacity must be greater than 0, got: " + capacity
            );
        }
    }
}