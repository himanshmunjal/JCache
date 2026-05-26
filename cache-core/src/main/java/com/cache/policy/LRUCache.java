package com.cache.policy;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.api.EvictionPolicy;
import com.cache.core.DoublyLinkedList;
import com.cache.core.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LRU (Least Recently Used) cache implementation.
 *
 * Algorithm:
 *  - HashMap gives O(1) key → node lookup.
 *  - DoublyLinkedList maintains access order (front = most recent).
 *  - On get:  move node to front → O(1)
 *  - On put:  if exists, update + move to front; if new, add to front.
 *             If over capacity, evict the tail node.
 *  - On evict: remove from both map and list → O(1).
 *
 * Not thread-safe. Use ConcurrentLRUCache (Day 12) for concurrent access.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class LRUCache<K, V> implements Cache<K, V>, EvictionPolicy<K> {

    private final int capacity;
    private final Map<K, Node<K, V>>   map;
    private final DoublyLinkedList<K, V> list;

    // Stats counters — AtomicLong now so we don't have to change the field
    // type when we add thread safety later
    private final AtomicLong hits      = new AtomicLong(0);
    private final AtomicLong misses    = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    public LRUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be > 0, got: " + capacity);
        }
        this.capacity = capacity;
        this.map      = new HashMap<>(capacity);
        this.list     = new DoublyLinkedList<>();
    }

    // Cache interface

    /**
     * Returns the value for key, or null if absent/expired.
     * Moves the accessed node to the front of the list (marks it most-recent).
     * O(1).
     */
    @Override
    public V get(K key) {
        Node<K, V> node = map.get(key);

        if (node == null) {
            misses.incrementAndGet();
            return null;
        }

        // Lazy TTL eviction — treat expired nodes as misses
        if (node.isExpired()) {
            evict(key);
            misses.incrementAndGet();
            return null;
        }

        hits.incrementAndGet();
        onAccess(key);           // EvictionPolicy hook — moves node to front
        return node.value;
    }

    /**
     * Inserts or updates a key-value pair with no TTL.
     * If the cache is at capacity, the least-recently-used entry is evicted first.
     * O(1).
     */
    @Override
    public void put(K key, V value) {
        put(key, value, -1);
    }

    /**
     * Inserts or updates a key-value pair with a TTL in milliseconds.
     * Pass ttlMillis = -1 for no expiry.
     * O(1).
     */
    public void put(K key, V value, long ttlMillis) {
        if (map.containsKey(key)) {
            // Update existing node in place — cheaper than remove + re-add
            Node<K, V> node = map.get(key);
            node.value      = value;
            node.expiryTime = (ttlMillis == -1) ? -1
                    : System.currentTimeMillis() + ttlMillis;
            onAccess(key);   // Treat update as an access — moves to front
            return;
        }

        // Evict LRU entry before inserting if at capacity
        if (map.size() >= capacity) {
            evictLRU();
        }

        Node<K, V> newNode = (ttlMillis == -1) ? new Node<>(key, value)
                : new Node<>(key, value, ttlMillis);
        map.put(key, newNode);
        list.addToFront(newNode);
        onInsert(key);       // EvictionPolicy hook
    }

    /**
     * Explicitly removes a key from the cache.
     * No-op if the key does not exist.
     * O(1).
     */
    @Override
    public void evict(K key) {
        Node<K, V> node = map.remove(key);
        if (node != null) {
            list.remove(node);
            evictions.incrementAndGet();
        }
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public CacheStats getstats() {
        long h = hits.get();
        long m = misses.get();
        long total = h + m;
        double hitRate = (total == 0) ? 0.0 : (double) h / total;
        return new CacheStats(h, m, evictions.get(), hitRate);
    }

    // EvictionPolicy interface

    /** Called on every successful get — moves node to front. */
    @Override
    public void onAccess(K key) {
        Node<K, V> node = map.get(key);
        if (node != null) {
            list.moveToFront(node);
        }
    }

    @Override
    public void clear() {
        map.clear();
        list.clear();
    }

    /** Called on every new insert — node is already at front, nothing to do. */
    @Override
    public void onInsert(K key) {
        // addToFront already handled in put()
    }

    /** Not part of the EvictionPolicy interface — returns the evicted key. */
    @Override
    public K evict() {
        Node<K, V> lru = list.peekLast();
        if (lru == null) return null;
        evict(lru.key);
        return lru.key;
    }

    // Internal

    private void evictLRU() {
        Node<K, V> lru = list.removeLast();
        if (lru != null) {
            map.remove(lru.key);
            evictions.incrementAndGet();
        }
    }

    //  Debug

    /** Returns the current order of keys from most-recent to least-recent. */
    @Override
    public String toString() {
        return "LRUCache(capacity=" + capacity + ", size=" + size() + ") " + list;
    }
}