package com.cache.policy;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.api.EvictionPolicy;
import com.cache.core.DoublyLinkedList;
import com.cache.core.Node;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.cache.policy.LRUCache.requireKey;
import static com.cache.policy.LRUCache.requireValue;

/**
 * Least-frequently-used cache with O(1) operations, following Shah, Mitra and
 * Matani, "An O(1) algorithm for implementing the LFU cache eviction scheme"
 * (2010).
 *
 * <p>Entries are grouped into one list per access count, and the lowest
 * non-empty count is tracked in {@code minFreq}. Eviction removes the least
 * recently used entry from that bucket, so ties are broken by recency.
 *
 * <p>Plain LFU never forgets: a key that was hot an hour ago keeps a high
 * count and cannot be evicted after it goes cold. To avoid that, every
 * {@value #DECAY_INTERVAL_OPS} operations all counts are halved. The decay pass
 * is O(n) but amortises to O(1) per operation.
 *
 * <p>Not thread-safe.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class LFUCache<K, V> implements Cache<K, V>, EvictionPolicy<K> {

    static final int DECAY_INTERVAL_OPS = 10_000;

    private final int capacity;
    private final Map<K, Node<K, V>> map;
    private final Map<Integer, DoublyLinkedList<K, V>> buckets = new HashMap<>();
    private int minFreq;
    private int opsSinceDecay;

    private long hits;
    private long misses;
    private long evictions;

    /**
     * Creates an empty cache.
     *
     * @param capacity maximum number of entries, must be positive
     */
    public LFUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be > 0, got: " + capacity);
        }
        this.capacity = capacity;
        this.map = new HashMap<>(capacity);
    }

    @Override
    public V get(K key) {
        requireKey(key);
        tickDecayClock();

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
        incrementFrequency(node);
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
        tickDecayClock();

        Node<K, V> existing = map.get(key);
        if (existing != null) {
            existing.value = value;
            existing.expireAfter(ttlMillis);
            incrementFrequency(existing);
            return;
        }

        if (map.size() >= capacity) {
            evict();
        }
        Node<K, V> node = new Node<>(key, value);
        node.expireAfter(ttlMillis);
        node.frequency = 1;
        bucket(1).addToFront(node);
        map.put(key, node);
        minFreq = 1;
    }

    @Override
    public void evict(K key) {
        Node<K, V> node = map.remove(key);
        if (node != null) {
            unlink(node);
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
        buckets.clear();
        minFreq = 0;
    }

    @Override
    public void onAccess(K key) {
        Node<K, V> node = map.get(key);
        if (node != null) {
            incrementFrequency(node);
        }
    }

    @Override
    public void onInsert(K key) {
        // put() already places new nodes in the frequency-1 bucket.
    }

    @Override
    public K evict() {
        if (map.isEmpty()) {
            return null;
        }
        DoublyLinkedList<K, V> lowest = buckets.get(minFreq);
        if (lowest == null) {
            // minFreq can go stale after an explicit evict(key); the number of
            // distinct frequencies is small, so finding the new minimum is cheap.
            minFreq = Collections.min(buckets.keySet());
            lowest = buckets.get(minFreq);
        }
        Node<K, V> victim = lowest.peekLast();
        map.remove(victim.key);
        unlink(victim);
        evictions++;
        return victim.key;
    }

    /**
     * Returns the access count currently recorded for {@code key}.
     *
     * @param key the key
     * @return the frequency, or 0 if the key is absent
     */
    public int frequencyOf(K key) {
        Node<K, V> node = map.get(key);
        return node == null ? 0 : node.frequency;
    }

    @Override
    public String toString() {
        return "LFUCache(capacity=" + capacity + ", size=" + size() + ") " + buckets;
    }

    private void incrementFrequency(Node<K, V> node) {
        int oldFreq = node.frequency;
        unlink(node);
        if (oldFreq == minFreq && !buckets.containsKey(oldFreq)) {
            minFreq = oldFreq + 1;
        }
        node.frequency = oldFreq + 1;
        bucket(node.frequency).addToFront(node);
    }

    /** Removes the node from its frequency bucket and drops the bucket if it empties. */
    private void unlink(Node<K, V> node) {
        DoublyLinkedList<K, V> list = buckets.get(node.frequency);
        list.remove(node);
        if (list.isEmpty()) {
            buckets.remove(node.frequency);
        }
    }

    private DoublyLinkedList<K, V> bucket(int frequency) {
        return buckets.computeIfAbsent(frequency, f -> new DoublyLinkedList<>());
    }

    private void tickDecayClock() {
        if (++opsSinceDecay >= DECAY_INTERVAL_OPS) {
            opsSinceDecay = 0;
            decayFrequencies();
        }
    }

    /**
     * Halves every count (keeping a floor of 1) and rebuilds the buckets.
     * Ordering inside a bucket is not preserved, which only affects how ties
     * are broken right after a decay.
     */
    private void decayFrequencies() {
        if (map.isEmpty()) {
            return;
        }
        buckets.clear();
        int newMin = Integer.MAX_VALUE;
        for (Node<K, V> node : map.values()) {
            node.frequency = Math.max(1, node.frequency / 2);
            node.prev = null;
            node.next = null;
            bucket(node.frequency).addToFront(node);
            newMin = Math.min(newMin, node.frequency);
        }
        minFreq = newMin;
    }
}
