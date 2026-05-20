package com.cache.ttl;

import com.cache.api.Cache;
import com.cache.api.CacheStats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TTLCache is a decorator that adds Time-To-Live (TTL) expiry behaviour
 * on top of any existing Cache<K,V> implementation.
 *
 * Design pattern: Decorator
 * This class wraps LRUCache, LFUCache, or ARCCache without modifying them.
 * It intercepts get() and put() calls to enforce expiry rules.
 *
 * Two-mechanism expiry strategy (same as Redis):
 *
 *   1. LAZY EVICTION  — checked on every get().
 *      When you request a key, TTLCache checks if it has expired BEFORE
 *      returning it. If expired, it deletes it and returns null.
 *      Cost: O(1) per get, zero background overhead.
 *      Problem: expired keys sit in memory if nobody reads them.
 *
 *   2. ACTIVE SWEEPER — a background ScheduledExecutorService thread
 *      that wakes up every `sweepIntervalMs` milliseconds and scans
 *      the expiry map, deleting all keys whose deadline has passed.
 *      Cost: proportional to number of keys with TTL.
 *      Benefit: memory is reclaimed even for keys never read again.
 *
 * Together, these two mechanisms ensure:
 *   - No expired value is ever returned to a caller (lazy eviction).
 *   - Memory is eventually reclaimed for all expired keys (sweeper).
 *
 * Thread safety:
 *   - expiryMap is a ConcurrentHashMap — safe for concurrent reads/writes.
 *   - Underlying cache is expected to handle its own thread safety
 *     (CoarseGrainedCache or SegmentedCache wrappers).
 *   - The sweeper thread uses the same remove path as lazy eviction,
 *     so double-removal is safe (cache.evict on a missing key is a no-op).
 *
 * TTL=0 behaviour:
 *   A TTL of 0 means "no expiry" — the key lives until evicted by policy.
 *   This is consistent with Redis semantics (PERSIST command).
 *   A negative TTL throws IllegalArgumentException.
 *
 * @param <K> Key type. Must implement equals() and hashCode() correctly.
 * @param <V> Value type.
 */
public class TTLCache<K, V> implements Cache<K, V> {

    // Constants

    /**
     * Sentinel value stored in expiryMap to indicate "this key never expires".
     * Using Long.MAX_VALUE means expiry checks (now > deadline) always fail.
     */
    private static final long NO_EXPIRY = Long.MAX_VALUE;

    /**
     * Default sweep interval in milliseconds.
     * Sweeper wakes up every 500ms and cleans expired keys.
     * Tune this lower for short TTLs, higher for long-lived caches.
     */
    private static final long DEFAULT_SWEEP_INTERVAL_MS = 500L;

    // State

    /**
     * The underlying cache that actually stores K→V mappings and
     * handles eviction policy (LRU/LFU/ARC). TTLCache delegates all
     * structural operations to this.
     */
    private final Cache<K, V> delegate;

    /**
     * Maps each key to its expiry timestamp in milliseconds (System.currentTimeMillis()).
     * Keys with NO_EXPIRY as their value never expire.
     *
     * Why ConcurrentHashMap here even if delegate is already thread-safe?
     * Because expiryMap and delegate are two separate data structures that
     * must stay in sync. We need expiryMap itself to be thread-safe
     * independently, regardless of the delegate's locking strategy.
     */
    private final ConcurrentHashMap<K, Long> expiryMap;

    /**
     * Counts how many keys were removed by the background sweeper.
     * Useful for debugging and metrics — tells you if TTLs are firing.
     */
    private final AtomicLong sweepEvictions;

    /**
     * Single-threaded scheduled executor for the background sweeper.
     * Single thread is sufficient — sweeping is lightweight and we don't
     * want multiple threads racing to delete the same expired keys.
     */
    private final ScheduledExecutorService sweeper;

    /**
     * How often the background sweeper runs, in milliseconds.
     */
    private final long sweepIntervalMs;

    // Constructors

    /**
     * Creates a TTLCache wrapping the given delegate, using the default
     * sweep interval of 500ms.
     *
     * @param delegate The underlying cache (LRUCache, LFUCache, ARCCache, etc.)
     */
    public TTLCache(Cache<K, V> delegate) {
        this(delegate, DEFAULT_SWEEP_INTERVAL_MS);
    }

    /**
     * Creates a TTLCache with a custom sweep interval.
     * Use a shorter interval for caches with very short TTLs (< 1 second).
     * Use a longer interval for caches with long TTLs (> 1 minute) to
     * reduce background CPU usage.
     *
     * @param delegate       The underlying cache.
     * @param sweepIntervalMs How often the sweeper runs, in milliseconds.
     *                        Must be > 0.
     */
    public TTLCache(Cache<K, V> delegate, long sweepIntervalMs) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate cache cannot be null");
        }
        if (sweepIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "Sweep interval must be positive, got: " + sweepIntervalMs
            );
        }

        this.delegate       = delegate;
        this.sweepIntervalMs = sweepIntervalMs;
        this.expiryMap      = new ConcurrentHashMap<>();
        this.sweepEvictions = new AtomicLong(0);

        // Create and start the background sweeper.
        // We use a daemon thread so it doesn't prevent JVM shutdown
        // if the caller forgets to call shutdown().
        this.sweeper = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable, "ttl-cache-sweeper");
            t.setDaemon(true);  // won't block JVM shutdown
            return t;
        });

        // Schedule the sweeper to run at a fixed rate.
        // scheduleAtFixedRate: next run starts sweepIntervalMs after previous START.
        // scheduleWithFixedDelay: next run starts sweepIntervalMs after previous END.
        // We use AtFixedRate so the cleanup cadence is predictable under load.
        this.sweeper.scheduleAtFixedRate(
                this::sweepExpiredKeys,
                sweepIntervalMs,   // initial delay — don't sweep immediately on startup
                sweepIntervalMs,   // period between sweeps
                TimeUnit.MILLISECONDS
        );
    }

    // Cache<K,V> interface implementation

    /**
     * Returns the value for the given key, or null if:
     *   - The key doesn't exist in the cache, OR
     *   - The key exists but has expired (lazy eviction fires here).
     *
     * Lazy eviction: we check the expiry timestamp BEFORE returning the value.
     * If expired, we delete the key from both the delegate and expiryMap,
     * then return null as if the key was never there.
     *
     * @param key The key to look up.
     * @return The value, or null if absent or expired.
     */
    @Override
    public V get(K key) {
        // Step 1: Check if this key has an expiry entry and if it has passed.
        // isExpired() is a fast O(1) ConcurrentHashMap lookup.
        if (isExpired(key)) {
            // Key is expired — remove from both structures and return null.
            // This is lazy eviction: we only clean up when someone asks.
            deleteKey(key);
            return null;
        }

        // Step 2: Key is either not expired or has no TTL — delegate to underlying cache.
        return delegate.get(key);
    }

    /**
     * Stores a key-value pair with no TTL (lives until evicted by policy).
     * Equivalent to Redis SET key value with no EX/PX option.
     *
     * @param key   The key. Cannot be null.
     * @param value The value. Cannot be null.
     */
    @Override
    public void put(K key, V value) {
        // TTL=0 means no expiry in our convention.
        put(key, value, 0);
    }

    /**
     * Stores a key-value pair with a TTL in seconds.
     *
     * TTL semantics:
     *   ttlSeconds = 0  → no expiry (key lives until evicted by policy)
     *   ttlSeconds > 0  → key expires after ttlSeconds seconds
     *   ttlSeconds < 0  → throws IllegalArgumentException
     *
     * If the key already exists with a different TTL, the new TTL completely
     * replaces the old one. This matches Redis PUT/SET semantics.
     *
     * @param key        The key. Cannot be null.
     * @param value      The value. Cannot be null.
     * @param ttlSeconds Time-to-live in seconds. 0 means no expiry.
     */
    public void put(K key, V value, long ttlSeconds) {
        if (key == null)   throw new IllegalArgumentException("Key cannot be null");
        if (value == null) throw new IllegalArgumentException("Value cannot be null");
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException(
                    "TTL cannot be negative, got: " + ttlSeconds
            );
        }

        // Step 1: Record the expiry deadline BEFORE putting into the delegate.
        // Why before? If we put into delegate first and the sweeper runs in
        // between, it might not find an expiry entry and leave a zombie key.
        if (ttlSeconds == 0) {
            // No TTL — sentinel value means "never expires".
            expiryMap.put(key, NO_EXPIRY);
        } else {
            // Compute absolute deadline: current time + TTL in milliseconds.
            long expiryTimestamp = System.currentTimeMillis() + (ttlSeconds * 1000L);
            expiryMap.put(key, expiryTimestamp);
        }

        // Step 2: Delegate the actual storage and eviction logic.
        delegate.put(key, value);
    }

    /**
     * Explicitly removes a key from the cache, regardless of its TTL.
     * Removes from both the delegate and the expiryMap.
     *
     * @param key The key to remove.
     */
    @Override
    public void evict(K key) {
        deleteKey(key);
    }

    /**
     * Returns the number of keys currently in the cache.
     * Note: this may include keys that are expired but not yet swept.
     * The delegate's size() is the authoritative count of stored entries,
     * but some may return null on get() due to lazy eviction.
     *
     * If you need an exact "live key" count, you'd need to scan expiryMap
     * and filter out expired entries — expensive, so we don't do it here.
     *
     * @return Approximate number of entries in the cache.
     */
    @Override
    public int size() {
        return delegate.size();
    }

    /**
     * Returns the cache statistics (hits, misses, evictions) from
     * the underlying delegate. TTL-based evictions are counted separately
     * in sweepEvictions — see getSweepEvictionCount().
     *
     * @return CacheStats from the delegate.
     */
    @Override
    public CacheStats getstats() {
        return delegate.getstats();
    }

    // TTL-specific public API

    /**
     * Returns how many seconds remain before this key expires.
     * Returns -1 if the key has no TTL (permanent key).
     * Returns 0 if the key has already expired (or doesn't exist).
     *
     * Useful for debugging: "why did this key disappear?"
     *
     * @param key The key to check.
     * @return Remaining TTL in seconds, -1 for no expiry, 0 if expired/absent.
     */
    public long getRemainingTTL(K key) {
        Long deadline = expiryMap.get(key);
        if (deadline == null)         return 0;   // key doesn't exist
        if (deadline == NO_EXPIRY)    return -1;  // permanent key
        long remainingMs = deadline - System.currentTimeMillis();
        return remainingMs <= 0 ? 0 : remainingMs / 1000L;
    }

    /**
     * Returns the total number of keys removed by the background sweeper
     * since this TTLCache was created. Does not include lazy evictions.
     *
     * @return Count of sweep-based evictions.
     */
    public long getSweepEvictionCount() {
        return sweepEvictions.get();
    }

    /**
     * Shuts down the background sweeper thread gracefully.
     * Call this when you're done with the cache (e.g., in @AfterAll in tests,
     * or in a server shutdown hook) to avoid thread leaks.
     *
     * After shutdown, lazy eviction on get() still works, but no background
     * sweeping occurs.
     */
    public void shutdown() {
        sweeper.shutdown();
        try {
            // Give in-flight sweep tasks up to 2 seconds to complete.
            if (!sweeper.awaitTermination(2, TimeUnit.SECONDS)) {
                sweeper.shutdownNow();
            }
        } catch (InterruptedException e) {
            sweeper.shutdownNow();
            Thread.currentThread().interrupt(); // restore interrupted flag
        }
    }

    // Private helpers

    /**
     * Checks whether the given key has expired based on its deadline
     * in expiryMap.
     *
     * A key is expired if:
     *   - It exists in expiryMap, AND
     *   - Its deadline is not NO_EXPIRY, AND
     *   - The current time is past its deadline.
     *
     * A key that doesn't exist in expiryMap is NOT considered expired here —
     * it will just return null from delegate.get() naturally.
     *
     * @param key The key to check.
     * @return true if the key exists and its TTL has elapsed.
     */
    private boolean isExpired(K key) {
        Long deadline = expiryMap.get(key);
        if (deadline == null)      return false;  // no expiry entry
        if (deadline == NO_EXPIRY) return false;  // permanent key
        return System.currentTimeMillis() > deadline;
    }

    /**
     * Removes a key from both the delegate cache and the expiry map.
     * Safe to call multiple times — both structures handle missing keys gracefully.
     *
     * @param key The key to remove.
     */
    private void deleteKey(K key) {
        expiryMap.remove(key);
        delegate.evict(key);
    }

    /**
     * The background sweeper task.
     * Runs every `sweepIntervalMs` milliseconds on the sweeper thread.
     *
     * Strategy: scan all entries in expiryMap and delete any that have expired.
     * ConcurrentHashMap iteration is weakly consistent — keys added during
     * iteration may or may not be seen. That's fine: they'll be caught in
     * the next sweep cycle or by lazy eviction.
     *
     * We catch Throwable (not just Exception) to prevent the sweeper from
     * dying silently. A RuntimeException inside a ScheduledExecutorService
     * task suppresses future executions if uncaught.
     */
    private void sweepExpiredKeys() {
        try {
            long now = System.currentTimeMillis();

            for (Map.Entry<K, Long> entry : expiryMap.entrySet()) {
                long deadline = entry.getValue();

                // Skip permanent keys and non-expired keys.
                if (deadline == NO_EXPIRY || now <= deadline) {
                    continue;
                }

                // Key has expired — remove it.
                // We call deleteKey() which removes from both structures.
                // If lazy eviction already removed it, deleteKey() is a safe no-op.
                deleteKey(entry.getKey());
                sweepEvictions.incrementAndGet();
            }
        } catch (Throwable t) {
            // Log but don't rethrow — rethrowing kills future scheduled executions.
            System.err.println("[TTLCache] Sweeper encountered an error: " + t.getMessage());
        }
    }
}