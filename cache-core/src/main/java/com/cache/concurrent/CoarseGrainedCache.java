package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CoarseGrainedCache — Thread-safe cache using a single ReadWriteLock.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHAT IS COARSE-GRAINED LOCKING?
 * ─────────────────────────────────────────────────────────────────────────────
 * "Coarse-grained" means ONE lock guards the ENTIRE data structure.
 * Every thread that wants to read OR write must acquire this one lock first.
 *
 * Analogy: a library with a single front-door key.
 * - Readers share the key (multiple readers inside at once).
 * - A writer needs exclusive access — everyone else waits outside.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHY ReadWriteLock INSTEAD OF synchronized?
 * ─────────────────────────────────────────────────────────────────────────────
 * ReentrantReadWriteLock has two modes:
 *
 *   READ LOCK  — multiple threads can hold it simultaneously.
 *                Safe because readers don't modify shared state.
 *                cache.get() → readLock (many threads can call this at once)
 *
 *   WRITE LOCK — only ONE thread can hold it, and only when no readers hold it.
 *                cache.put(), cache.evict() → writeLock (exclusive access)
 *
 * If we used `synchronized` on every method, ALL threads would block each other
 * even on read-only operations. ReadWriteLock gives us free parallelism for reads.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * PERFORMANCE CHARACTERISTICS
 * ─────────────────────────────────────────────────────────────────────────────
 * Best case : read-heavy workloads — many threads read simultaneously.
 * Worst case: write-heavy workloads — every write blocks all other threads.
 * Contention: HIGH under mixed load (every write serialises everything).
 *
 * This is your CORRECTNESS BASELINE. Before optimising with segment locking
 * or lock-free approaches, this simple implementation gives you confidence
 * that your cache is correct under concurrent access. If a test fails with
 * CoarseGrainedCache, the bug is in the delegate, not the locking strategy.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * DESIGN PATTERN: Decorator
 * ─────────────────────────────────────────────────────────────────────────────
 * Same pattern as TTLCache — wraps any Cache<K,V> and adds thread safety.
 * The delegate (LRUCache, LFUCache, ARCCache) is NOT thread-safe on its own.
 * CoarseGrainedCache makes it safe without modifying the delegate at all.
 *
 * @param <K> Key type. Must implement equals() and hashCode() correctly.
 * @param <V> Value type.
 */
public class CoarseGrainedCache<K, V> implements Cache<K, V> {

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * The underlying cache that stores K→V and applies eviction policy.
     * This delegate is NOT thread-safe — CoarseGrainedCache provides all safety.
     */
    private final Cache<K, V> delegate;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The single lock guarding the entire cache.
     *
     * ReentrantReadWriteLock properties:
     *   - "Reentrant": a thread that holds the lock can acquire it again
     *     without deadlocking (useful for recursive calls).
     *   - "ReadWrite": read lock is shared; write lock is exclusive.
     *   - Fair mode (true): threads acquire locks in FIFO order.
     *     Prevents writer starvation — a continuous stream of readers
     *     can't indefinitely block a waiting writer.
     *     Trade-off: slightly lower throughput than unfair mode.
     */
//    private final ReadWriteLock lock = new ReentrantReadWriteLock(true /* fair */);

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Wraps the given cache with coarse-grained locking.
     *
     * @param delegate Any Cache<K,V> implementation. Must not be null.
     *                 Must NOT already be thread-safe (wrapping a thread-safe
     *                 cache works but adds unnecessary lock overhead).
     */
    public CoarseGrainedCache(Cache<K, V> delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate cache cannot be null");
        }
        this.delegate = delegate;
    }

    // -------------------------------------------------------------------------
    // Cache<K,V> interface — all operations are lock-guarded
    // -------------------------------------------------------------------------

    /**
     * Returns the value for the given key, or null if not present.
     *
     * Uses READ LOCK — multiple threads can call get() simultaneously
     * as long as no thread is currently writing.
     *
     * Lock ordering:
     *   1. Acquire readLock
     *   2. Delegate to underlying cache
     *   3. Release readLock in finally block
     *
     * The finally block is MANDATORY. If delegate.get() throws a RuntimeException,
     * the lock must still be released or the cache deadlocks permanently.
     *
     * @param key The key to look up.
     * @return The associated value, or null if absent.
     */
    @Override
    public V get(K key) {
        lock.lock();
        try {
            return delegate.get(key);
        } finally {
            lock.unlock(); // ALWAYS releases, even on exception
        }
    }

    /**
     * Stores a key-value pair, potentially evicting an existing key
     * if the cache is at capacity.
     *
     * Uses WRITE LOCK — exclusive access. All other readers and writers
     * block until this put() completes.
     *
     * Why not readLock for put()? Because put() modifies the linked list
     * and the HashMap in the delegate. Allowing concurrent reads during
     * a write would cause ConcurrentModificationException or data corruption.
     *
     * @param key   The key. Cannot be null (delegate will enforce).
     * @param value The value. Cannot be null (delegate will enforce).
     */
    @Override
    public void put(K key, V value) {
        lock.lock();
        try {
            delegate.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the key and its value from the cache.
     *
     * Uses WRITE LOCK — eviction modifies the underlying data structure.
     *
     * @param key The key to remove. No-op if the key doesn't exist.
     */
    @Override
    public void evict(K key) {
        lock.lock();
        try {
            delegate.evict(key);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current number of entries in the cache.
     *
     * Uses READ LOCK — size() reads shared state (the delegate's internal
     * counter) without modifying it. Safe to allow concurrent reads.
     *
     * Note: the returned value may be stale by the time the caller uses it,
     * because another thread may put/evict immediately after size() returns.
     * This is acceptable — size() gives a point-in-time snapshot.
     *
     * @return Current number of cached entries.
     */
    @Override
    public int size() {
        lock.lock();
        try {
            return delegate.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();

        try {
            delegate.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns cache statistics (hits, misses, evictions) from the delegate.
     *
     * Uses READ LOCK — getStats() reads metrics without modifying the cache.
     * Since CacheStats uses AtomicLong internally, the stats themselves are
     * thread-safe, but we still acquire the read lock for consistency:
     * we want the stats snapshot to reflect a coherent moment in time.
     *
     * @return Immutable snapshot of current cache statistics.
     */
    @Override
    public CacheStats getstats() {
        lock.lock();
        try {
            return delegate.getstats();
        } finally {
            lock.unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Diagnostic helpers (not on Cache interface — coarse-grained specific)
    // -------------------------------------------------------------------------

    /**
     * Returns true if any thread currently holds the write lock.
     * Useful in tests to verify locking behaviour.
     *
     * Casting to ReentrantReadWriteLock gives access to diagnostic methods
     * not on the ReadWriteLock interface.
     */
//    public boolean isWriteLocked() {
//        return ((ReentrantReadWriteLock) lock).isWriteLocked();
//    }
//
//    /**
//     * Returns the number of threads currently waiting to acquire any lock.
//     * Useful for detecting contention in benchmark analysis.
//     */
//    public int getQueueLength() {
//        return ((ReentrantReadWriteLock) lock).getQueueLength();
//    }
}