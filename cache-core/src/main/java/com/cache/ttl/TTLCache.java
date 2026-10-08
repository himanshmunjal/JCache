package com.cache.ttl;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.persistence.SnapshotCapable;
import com.cache.persistence.SnapshotLoader;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Decorator that adds per-key expiry to any {@link Cache}.
 *
 * <p>Expiry works the same way as in Redis: a read checks the deadline and
 * drops the key if it has passed, and a background sweeper periodically
 * removes expired keys that nobody reads. The sweeper also discards expiry
 * metadata for keys the delegate has evicted on its own.
 *
 * <p>Thread safety comes from lock striping: operations on the same key are
 * serialised, operations on different keys only contend if they hash to the
 * same stripe. The delegate must itself be thread-safe if the cache is shared
 * (for example a {@link com.cache.concurrent.SegmentedCache}).
 *
 * <p>TTLs are given in seconds. A TTL of 0 means "never expires".
 *
 * @param <K> key type
 * @param <V> value type
 */
public class TTLCache<K, V> implements Cache<K, V>, SnapshotCapable, AutoCloseable {

    private static final Logger log = Logger.getLogger(TTLCache.class.getName());

    /** Default interval between sweeper runs. */
    public static final long DEFAULT_SWEEP_INTERVAL_MS = 500L;

    private static final long NO_EXPIRY = Long.MAX_VALUE;
    private static final int STRIPES = 64;

    private final Cache<K, V> delegate;
    private final ConcurrentHashMap<K, Long> deadlines = new ConcurrentHashMap<>();
    private final Object[] locks = new Object[STRIPES];
    private final long defaultTtlMillis;
    private final AtomicLong expiredReads = new AtomicLong();
    private final AtomicLong sweptKeys = new AtomicLong();
    private final ScheduledExecutorService sweeper;

    /**
     * Wraps {@code delegate} with no default TTL and the default sweep interval.
     *
     * @param delegate the cache that stores the entries
     */
    public TTLCache(Cache<K, V> delegate) {
        this(delegate, DEFAULT_SWEEP_INTERVAL_MS);
    }

    /**
     * Wraps {@code delegate} with no default TTL.
     *
     * @param delegate        the cache that stores the entries
     * @param sweepIntervalMs how often the sweeper runs, in milliseconds
     */
    public TTLCache(Cache<K, V> delegate, long sweepIntervalMs) {
        this(delegate, sweepIntervalMs, Duration.ZERO);
    }

    /**
     * Wraps {@code delegate}.
     *
     * @param delegate        the cache that stores the entries
     * @param sweepIntervalMs how often the sweeper runs, in milliseconds
     * @param defaultTtl      TTL applied by {@link #put(Object, Object)};
     *                        {@link Duration#ZERO} means entries do not expire
     */
    public TTLCache(Cache<K, V> delegate, long sweepIntervalMs, Duration defaultTtl) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate cache cannot be null");
        }
        if (sweepIntervalMs <= 0) {
            throw new IllegalArgumentException("Sweep interval must be positive, got: " + sweepIntervalMs);
        }
        if (defaultTtl == null || defaultTtl.isNegative()) {
            throw new IllegalArgumentException("Default TTL must be zero or positive");
        }
        this.delegate = delegate;
        this.defaultTtlMillis = defaultTtl.toMillis();
        for (int i = 0; i < STRIPES; i++) {
            locks[i] = new Object();
        }
        this.sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ttl-cache-sweeper");
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleWithFixedDelay(this::sweep, sweepIntervalMs, sweepIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public V get(K key) {
        synchronized (lockFor(key)) {
            if (isExpired(key)) {
                remove(key);
                expiredReads.incrementAndGet();
                return null;
            }
            V value = delegate.get(key);
            if (value == null) {
                // The delegate may have evicted the key for capacity reasons.
                deadlines.remove(key);
            }
            return value;
        }
    }

    /**
     * Stores a value using the default TTL given at construction.
     */
    @Override
    public void put(K key, V value) {
        putWithTtlMillis(key, value, defaultTtlMillis);
    }

    /**
     * Stores a value that expires after {@code ttlSeconds}. Replaces any
     * previous TTL for the key.
     *
     * @param key        the key
     * @param value      the value
     * @param ttlSeconds time to live in seconds; 0 means never expire
     */
    public void put(K key, V value, long ttlSeconds) {
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("TTL cannot be negative, got: " + ttlSeconds);
        }
        putWithTtlMillis(key, value, TimeUnit.SECONDS.toMillis(ttlSeconds));
    }

    /**
     * Sets a new TTL on an existing key. A TTL of 0 deletes the key
     * immediately, matching Redis {@code EXPIRE key 0}.
     *
     * @param key        the key
     * @param ttlSeconds new time to live in seconds
     * @return {@code false} if the key does not exist
     */
    public boolean expire(K key, long ttlSeconds) {
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("TTL cannot be negative, got: " + ttlSeconds);
        }
        synchronized (lockFor(key)) {
            if (!isLive(key)) {
                return false;
            }
            if (ttlSeconds == 0) {
                remove(key);
            } else {
                deadlines.put(key, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ttlSeconds));
            }
            return true;
        }
    }

    /**
     * Removes the TTL from a key so it no longer expires.
     *
     * @param key the key
     * @return {@code true} if a TTL was removed; {@code false} if the key does
     *         not exist or had no TTL
     */
    public boolean persist(K key) {
        synchronized (lockFor(key)) {
            if (!isLive(key) || deadlineOf(key) == NO_EXPIRY) {
                return false;
            }
            deadlines.put(key, NO_EXPIRY);
            return true;
        }
    }

    /**
     * Returns the seconds left before {@code key} expires, rounded up.
     *
     * @param key the key
     * @return remaining seconds, {@code -1} if the key never expires, or
     *         {@code 0} if it is absent or already expired
     */
    public long getRemainingTTL(K key) {
        if (!containsKey(key)) {
            return 0;
        }
        long deadline = deadlineOf(key);
        if (deadline == NO_EXPIRY) {
            return -1;
        }
        long remainingMs = deadline - System.currentTimeMillis();
        return remainingMs <= 0 ? 0 : (remainingMs + 999) / 1000;
    }

    @Override
    public void evict(K key) {
        synchronized (lockFor(key)) {
            remove(key);
        }
    }

    @Override
    public V peek(K key) {
        return isExpired(key) ? null : delegate.peek(key);
    }

    @Override
    public boolean containsKey(K key) {
        return isLive(key);
    }

    /**
     * Returns the number of entries held by the delegate. Expired entries that
     * have not been swept yet are included.
     */
    @Override
    public int size() {
        return delegate.size();
    }

    /**
     * Returns the delegate's statistics, with reads of expired keys counted as misses.
     */
    @Override
    public CacheStats getStats() {
        CacheStats inner = delegate.getStats();
        return CacheStats.of(inner.hits(), inner.misses() + expiredReads.get(), inner.evictions());
    }

    @Override
    public void clear() {
        deadlines.clear();
        delegate.clear();
    }

    /**
     * Returns how many keys the background sweeper has removed.
     *
     * @return the number of swept keys
     */
    public long getSweepEvictionCount() {
        return sweptKeys.get();
    }

    /**
     * Stops the sweeper thread. Lazy expiry on reads keeps working afterwards.
     */
    public void shutdown() {
        sweeper.shutdown();
        try {
            if (!sweeper.awaitTermination(2, TimeUnit.SECONDS)) {
                sweeper.shutdownNow();
            }
        } catch (InterruptedException e) {
            sweeper.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** Same as {@link #shutdown()}. */
    @Override
    public void close() {
        shutdown();
    }

    /**
     * Lists every live key with its absolute expiry time ({@code -1} for no
     * expiry). Only valid when the keys are strings, which is the case for
     * the server's cache.
     */
    @Override
    public Map<String, Long> getSnapshotEntries() {
        long now = System.currentTimeMillis();
        Map<String, Long> entries = new HashMap<>();
        deadlines.forEach((key, deadline) -> {
            if ((deadline == NO_EXPIRY || deadline > now) && delegate.containsKey(key)) {
                entries.put((String) key, deadline == NO_EXPIRY ? -1L : deadline);
            }
        });
        return Collections.unmodifiableMap(entries);
    }

    /**
     * Returns a view of a string cache that the persistence layer can both
     * snapshot and restore into, with TTLs given in milliseconds.
     *
     * <p>This is a separate adapter rather than {@code TTLCache} implementing
     * {@link SnapshotLoader.TtlAware} directly: for {@code TTLCache<String, String>}
     * the inherited {@code put(String, String, long)} would clash with
     * {@link #put(Object, Object, long)}, which takes seconds.
     *
     * @param cache the cache to adapt
     * @return a view that writes through to {@code cache}
     */
    public static Cache<String, String> asPersistable(TTLCache<String, String> cache) {
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        return new PersistenceView(cache);
    }

    private void putWithTtlMillis(K key, V value, long ttlMillis) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        long deadline = ttlMillis == 0 ? NO_EXPIRY : System.currentTimeMillis() + ttlMillis;
        synchronized (lockFor(key)) {
            deadlines.put(key, deadline);
            delegate.put(key, value);
        }
    }

    private boolean isLive(K key) {
        return !isExpired(key) && delegate.containsKey(key);
    }

    private boolean isExpired(K key) {
        long deadline = deadlineOf(key);
        return deadline != NO_EXPIRY && System.currentTimeMillis() > deadline;
    }

    private long deadlineOf(K key) {
        Long deadline = deadlines.get(key);
        return deadline == null ? NO_EXPIRY : deadline;
    }

    /** Caller must hold the key's lock. */
    private void remove(K key) {
        deadlines.remove(key);
        delegate.evict(key);
    }

    private Object lockFor(Object key) {
        int h = key.hashCode();
        return locks[(h ^ (h >>> 16)) & (STRIPES - 1)];
    }

    private void sweep() {
        try {
            long now = System.currentTimeMillis();
            boolean pruneStale = deadlines.size() > delegate.size();
            for (Map.Entry<K, Long> entry : deadlines.entrySet()) {
                K key = entry.getKey();
                boolean expired = entry.getValue() != NO_EXPIRY && now > entry.getValue();
                if (!expired && !pruneStale) {
                    continue;
                }
                synchronized (lockFor(key)) {
                    if (isExpired(key)) {
                        remove(key);
                        sweptKeys.incrementAndGet();
                    } else if (pruneStale && !delegate.containsKey(key)) {
                        deadlines.remove(key);
                    }
                }
            }
        } catch (RuntimeException e) {
            // An exception escaping a scheduled task would cancel all future sweeps.
            log.log(Level.WARNING, "TTL sweep failed", e);
        }
    }

    private static final class PersistenceView
            implements Cache<String, String>, SnapshotCapable, SnapshotLoader.TtlAware {

        private final TTLCache<String, String> target;

        PersistenceView(TTLCache<String, String> target) {
            this.target = target;
        }

        @Override
        public String get(String key) {
            return target.get(key);
        }

        @Override
        public void put(String key, String value) {
            target.putWithTtlMillis(key, value, 0);
        }

        @Override
        public void put(String key, String value, long ttlMillis) {
            target.putWithTtlMillis(key, value, Math.max(1, ttlMillis));
        }

        @Override
        public void evict(String key) {
            target.evict(key);
        }

        @Override
        public String peek(String key) {
            return target.peek(key);
        }

        @Override
        public boolean containsKey(String key) {
            return target.containsKey(key);
        }

        @Override
        public int size() {
            return target.size();
        }

        @Override
        public CacheStats getStats() {
            return target.getStats();
        }

        @Override
        public void clear() {
            target.clear();
        }

        @Override
        public Map<String, Long> getSnapshotEntries() {
            return target.getSnapshotEntries();
        }
    }
}
