package com.cache.policy;

import com.cache.api.Cache;
import com.cache.api.EvictionPolicy;
import com.cache.api.CacheStats;

import com.cache.core.Node;
import com.cache.core.DoublyLinkedList;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class FIFOCache<K,V> implements Cache<K,V>, EvictionPolicy<K> {
    private final int capacity;
    private final Map<K, Node<K, V>>   map;
    private final DoublyLinkedList<K, V> list;

    private final AtomicLong hits      = new AtomicLong(0);
    private final AtomicLong misses    = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    public FIFOCache(int capacity){
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be > 0, got: " + capacity);
        }
        this.capacity = capacity;
        this.list = new DoublyLinkedList<>();
        this.map = new HashMap<>(capacity);
    }

    @Override
    public V get(K key){
        Node<K,V> node = map.get(key);

        if(node==null){
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

        Node<K,V> node = map.get(key);
        if (node != null) {
            node.value = value;
            node.expiryTime = (ttlMillis == -1) ? -1: System.currentTimeMillis() + ttlMillis;
            return;
        }

        if (map.size() >= capacity) {
            evictFIFO();
        }

        Node<K,V> newNode = (ttlMillis == -1)
                ? new Node<>(key, value)
                : new Node<>(key, value, ttlMillis);
        map.put(key, newNode);
        list.addToFront(newNode);
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

    @Override
    public void onAccess(K key) { }

    @Override
    public void onInsert(K key) {
    }

    @Override
    public K evict() {
        Node<K, V> fifo = list.peekLast();
        if (fifo == null) return null;
        evict(fifo.key);
        return fifo.key;
    }

    // Internal

    private void evictFIFO() {
        Node<K, V> fifo = list.removeFirst();
        if (fifo != null) {
            map.remove(fifo.key);
            evictions.incrementAndGet();
        }
    }
}
