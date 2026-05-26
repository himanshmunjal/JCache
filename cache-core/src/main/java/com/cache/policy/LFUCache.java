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
 * LFU (Least Frequently Used) cache implementation.
 * Algorithm:
 *  - HashMap gives O(1) key → node lookup.
 *  - Each frequency has its own DoublyLinkedList.
 *  - freqMap maps:
 *      frequency -> DLL of nodes with that frequency
 *  Example:
 *      freq=1 : A <-> B
 *      freq=2 : C
 *      freq=5 : D
 *  - On get:
 *      Increase node frequency.
 *      Move node from old frequency list to new frequency list.
 *  - On put:
 *      If key exists:
 *          update value + increase frequency.
 *      If new key:
 *          insert into frequency=1 list.
 *      If capacity exceeded:
 *          evict least frequently used node.
 *  - Eviction:
 *      Remove from minFreq list.
 *      If multiple nodes share same frequency,
 *      remove least recently used among them.
 * Time Complexity:
 *  - get()  -> O(1)
 *  - put()  -> O(1)
 *  - evict()-> O(1)
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class LFUCache<K, V> implements Cache<K, V>, EvictionPolicy<K> {

    private final int capacity;
    //Main lookup table key -> node
    private final Map<K, Node<K, V>> map;
    // Frequency table:frequency -> DLL of nodes
    private final Map<Integer, DoublyLinkedList<K, V>> freqMap;

    /**
     * Tracks the minimum frequency currently present in cache.
     * Example:
     *  freq=1 : A B
     *  freq=2 : C
     * minFreq = 1
     */
    private int minFreq = 1;

    // Stats
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    // Constructor
    public LFUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException(
                    "Capacity must be > 0, got: " + capacity
            );
        }

        this.capacity = capacity;
        this.map = new HashMap<>(capacity);
        this.freqMap = new HashMap<>();
    }

    @Override
    public void clear() {
        map.clear();
        freqMap.clear();
        minFreq = 0;
    }

    // Cache Interface
    /**
     * Returns value for key.
     * On successful access:
     *  - increase frequency
     *  - move node to new frequency list
     * O(1).
     */
    @Override
    public V get(K key) {
        Node<K, V> node = map.get(key);

        // Cache miss
        if (node == null) {
            misses.incrementAndGet();
            return null;
        }

        // Lazy TTL eviction
        if (node.isExpired()) {
            evict(key);
            misses.incrementAndGet();
            return null;
        }

        hits.incrementAndGet();

        // Increase frequency
        updateFrequency(node);

        return node.value;
    }

    /**
     * Inserts key-value pair with no TTL.
     */
    @Override
    public void put(K key, V value) {
        put(key, value, -1);
    }

    /**
     * Inserts key-value pair with optional TTL.
     * O(1).
     */
    public void put(K key, V value, long ttlMillis) {

        // Capacity edge case
        if (capacity == 0) return;

        // Update existing node
        if (map.containsKey(key)) {

            Node<K, V> node = map.get(key);
            node.value = value;
            node.expiryTime =
                    (ttlMillis == -1)
                            ? -1
                            : System.currentTimeMillis() + ttlMillis;
            updateFrequency(node);
            return;
        }

        // Evict if full─
        if (map.size() >= capacity) {
            evictLFU();
        }

        // Create new node
        Node<K, V> newNode =
                (ttlMillis == -1)
                        ? new Node<>(key, value)
                        : new Node<>(key, value, ttlMillis);

        // New nodes always start at frequency=1
        newNode.frequency = 1;

        // Reset min frequency
        minFreq = 1;

        // Create freq=1 list if absent
        freqMap.putIfAbsent(1, new DoublyLinkedList<>());

        // Add node to freq=1 list
        freqMap.get(1).addToFront(newNode);

        // Add to main map
        map.put(key, newNode);
    }

    /**
     * Removes a key explicitly from cache.
     * O(1).
     */
    @Override
    public void evict(K key) {

        Node<K, V> node = map.remove(key);

        if (node == null) return;

        DoublyLinkedList<K, V> list =
                freqMap.get(node.frequency);

        list.remove(node);

        evictions.incrementAndGet();

        // Cleanup empty frequency list
        if (list.isEmpty() && node.frequency == minFreq) {
            minFreq++;
        }
    }

    /**
     * Returns current cache size.
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     * Returns cache statistics.
     */
    @Override
    public CacheStats getstats() {

        long h = hits.get();
        long m = misses.get();

        long total = h + m;

        double hitRate =
                (total == 0)
                        ? 0.0
                        : (double) h / total;

        return new CacheStats(
                h,
                m,
                evictions.get(),
                hitRate
        );
    }

    // EvictionPolicy Interface

    /**
     * Called on access.
     *
     * In LFU:
     *  access = increase frequency.
     */
    @Override
    public void onAccess(K key) {

        Node<K, V> node = map.get(key);

        if (node != null) {
            updateFrequency(node);
        }
    }

    @Override
    public void onInsert(K key) {
        // Already handled in put()
    }

    /**
     * Evicts LFU node and returns its key.
     */
    @Override
    public K evict() {
        DoublyLinkedList<K, V> minList =
                freqMap.get(minFreq);
        if (minList == null) return null;
        Node<K, V> lfu = minList.removeLast();
        if (lfu == null) return null;
        map.remove(lfu.key);
        evictions.incrementAndGet();
        return lfu.key;
    }

    // Internal Frequency Logic
    /**
     * Core LFU operation.
     * Moves node:
     *      freq=N   -> freq=N+1
     * Steps:
     *  1. Remove from old frequency list
     *  2. Increase frequency
     *  3. Add to new frequency list
     * O(1).
     */
    private void updateFrequency(Node<K, V> node) {
        int oldFreq = node.frequency;

        // Get old frequency list
        DoublyLinkedList<K, V> oldList =
                freqMap.get(oldFreq);

        // Remove node from old list
        oldList.remove(node);

        // If old min frequency becomes empty, increase min frequency
        if (oldFreq == minFreq && oldList.isEmpty()) {
            minFreq++;
        }

        // Increase node frequency
        node.frequency++;

        // Create new frequency list if needed
        freqMap.putIfAbsent(
                node.frequency,
                new DoublyLinkedList<>()
        );

        // Add node to front of new frequency list
        freqMap
                .get(node.frequency)
                .addToFront(node);
    }

    /**
     * Removes least frequently used node.
     * If multiple nodes have same frequency,
     * remove least recently used among them.
     * O(1).
     */
    private void evictLFU() {
        DoublyLinkedList<K, V> minList = freqMap.get(minFreq);

        Node<K, V> node = minList.removeLast();

        if (node != null) {
            map.remove(node.key);
            evictions.incrementAndGet();
        }
    }

    @Override
    public String toString() {
        return "LFUCache(capacity=" + capacity + ", size=" + size() + ") "+ freqMap;
    }
}