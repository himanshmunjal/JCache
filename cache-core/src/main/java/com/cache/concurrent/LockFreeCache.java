package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe cache with an approximate LRU policy and no locks on the read
 * path.
 *
 * <p>Exact LRU needs a list that is reordered on every read, which means a
 * lock. This cache instead stamps each entry with the time of its last access
 * and, when it is over capacity, scans for the oldest stamp. Reads are a map
 * lookup plus a volatile write; eviction is O(n). That trade-off suits
 * read-heavy workloads where evictions are comparatively rare.
 *
 * <p>Under concurrent inserts the size can briefly exceed the capacity by up
 * to the number of writing threads; every writer trims back to the capacity
 * before returning.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LockFreeCache<K, V> implements Cache<K, V> {

    private static final class Entry<K, V> {
        final K key;
        final V value;
        volatile long lastAccess;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
            this.lastAccess = System.nanoTime();
        }
    }

    private final int capacity;
    private final ConcurrentHashMap<K, Entry<K, V>> map;
    // ConcurrentHashMap.size() walks every bin, so the count is tracked separately.
    private final AtomicInteger size = new AtomicInteger();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();

    /**
     * Creates an empty cache.
     *
     * @param capacity maximum number of entries, must be positive
     */
    public LockFreeCache(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("Capacity must be >= 1, got: " + capacity);
        }
        this.capacity = capacity;
        this.map = new ConcurrentHashMap<>((int) (capacity / 0.75f) + 1);
    }

    @Override
    public V get(K key) {
        Entry<K, V> entry = map.get(key);
        if (entry == null) {
            misses.increment();
            return null;
        }
        entry.lastAccess = System.nanoTime();
        hits.increment();
        return entry.value;
    }

    @Override
    public V peek(K key) {
        Entry<K, V> entry = map.get(key);
        return entry == null ? null : entry.value;
    }

    @Override
    public void put(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (!map.containsKey(key) && size.get() >= capacity) {
            evictOldest();
        }
        if (map.put(key, new Entry<>(key, value)) == null) {
            size.incrementAndGet();
        }
        while (size.get() > capacity) {
            evictOldest();
        }
    }

    @Override
    public void evict(K key) {
        if (map.remove(key) != null) {
            size.decrementAndGet();
        }
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public CacheStats getStats() {
        return CacheStats.of(hits.sum(), misses.sum(), evictions.sum());
    }

    @Override
    public void clear() {
        for (K key : map.keySet()) {
            if (map.remove(key) != null) {
                size.decrementAndGet();
            }
        }
    }

    private void evictOldest() {
        Entry<K, V> oldest = null;
        for (Entry<K, V> entry : map.values()) {
            if (oldest == null || entry.lastAccess < oldest.lastAccess) {
                oldest = entry;
            }
        }
        // Remove only if the entry was not replaced since the scan.
        if (oldest != null && map.remove(oldest.key, oldest)) {
            size.decrementAndGet();
            evictions.increment();
        }
    }
}
