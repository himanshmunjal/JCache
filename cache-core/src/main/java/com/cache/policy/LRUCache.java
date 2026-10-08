package com.cache.policy;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.api.EvictionPolicy;
import com.cache.core.DoublyLinkedList;
import com.cache.core.Node;

import java.util.HashMap;
import java.util.Map;

/**
 * Least-recently-used cache. A hash map gives O(1) lookup and a doubly linked
 * list keeps entries in access order, so every operation is O(1).
 *
 * <p>Entries may optionally carry a TTL via {@link #put(Object, Object, long)};
 * expired entries are dropped lazily when they are next read.
 *
 * <p>Not thread-safe.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LRUCache<K, V> implements Cache<K, V>, EvictionPolicy<K> {

    private final int capacity;
    private final Map<K, Node<K, V>> map;
    private final DoublyLinkedList<K, V> list = new DoublyLinkedList<>();

    private long hits;
    private long misses;
    private long evictions;

    /**
     * Creates an empty cache.
     *
     * @param capacity maximum number of entries, must be positive
     */
    public LRUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be > 0, got: " + capacity);
        }
        this.capacity = capacity;
        this.map = new HashMap<>(capacity);
    }

    @Override
    public V get(K key) {
        requireKey(key);
        Node<K, V> node = map.get(key);
        if (node == null) {
            misses++;
            return null;
        }
        if (node.isExpired()) {
            evict(key);
            misses++;
            return null;
        }
        hits++;
        list.moveToFront(node);
        return node.value;
    }

    @Override
    public void put(K key, V value) {
        put(key, value, Node.NO_EXPIRY);
    }

    /**
     * Stores a value that expires after {@code ttlMillis}.
     *
     * @param key       the key
     * @param value     the value
     * @param ttlMillis time to live in milliseconds, or {@link Node#NO_EXPIRY}
     */
    public void put(K key, V value, long ttlMillis) {
        requireKey(key);
        requireValue(value);

        Node<K, V> existing = map.get(key);
        if (existing != null) {
            existing.value = value;
            existing.expireAfter(ttlMillis);
            list.moveToFront(existing);
            return;
        }

        if (map.size() >= capacity) {
            evict();
        }
        Node<K, V> node = new Node<>(key, value);
        node.expireAfter(ttlMillis);
        map.put(key, node);
        list.addToFront(node);
    }

    @Override
    public void evict(K key) {
        Node<K, V> node = map.remove(key);
        if (node != null) {
            list.remove(node);
        }
    }

    @Override
    public V peek(K key) {
        Node<K, V> node = map.get(key);
        return node == null || node.isExpired() ? null : node.value;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public CacheStats getStats() {
        return CacheStats.of(hits, misses, evictions);
    }

    @Override
    public void clear() {
        map.clear();
        list.clear();
    }

    @Override
    public void onAccess(K key) {
        Node<K, V> node = map.get(key);
        if (node != null) {
            list.moveToFront(node);
        }
    }

    @Override
    public void onInsert(K key) {
        // put() already links new nodes at the front.
    }

    @Override
    public K evict() {
        Node<K, V> lru = list.removeLast();
        if (lru == null) {
            return null;
        }
        map.remove(lru.key);
        evictions++;
        return lru.key;
    }

    @Override
    public String toString() {
        return "LRUCache(capacity=" + capacity + ", size=" + size() + ") " + list;
    }

    static void requireKey(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }
    }

    static void requireValue(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }
}
