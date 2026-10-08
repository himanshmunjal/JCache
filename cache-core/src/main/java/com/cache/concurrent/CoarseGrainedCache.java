package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Makes any cache thread-safe by guarding every call with one lock.
 *
 * <p>A read-write lock would not help here: in LRU, LFU and ARC even a
 * {@code get} reorders internal lists, so every operation needs exclusive
 * access. This is the simplest correct wrapper and serves as the baseline for
 * {@link SegmentedCache} and {@link LockFreeCache}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CoarseGrainedCache<K, V> implements Cache<K, V> {

    private final Cache<K, V> delegate;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Wraps {@code delegate}.
     *
     * @param delegate a cache that is not thread-safe on its own
     */
    public CoarseGrainedCache(Cache<K, V> delegate) {
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate cache cannot be null");
        }
        this.delegate = delegate;
    }

    @Override
    public V get(K key) {
        lock.lock();
        try {
            return delegate.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V peek(K key) {
        lock.lock();
        try {
            return delegate.peek(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        lock.lock();
        try {
            delegate.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void evict(K key) {
        lock.lock();
        try {
            delegate.evict(key);
        } finally {
            lock.unlock();
        }
    }

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
    public CacheStats getStats() {
        lock.lock();
        try {
            return delegate.getStats();
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
}
