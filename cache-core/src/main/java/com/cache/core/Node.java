package com.cache.core;

/**
 * Doubly linked list node used as the building block for LRU, LFU, and ARC caches.
 * Stores the key alongside the value so that when a node is evicted from the tail,
 * we can remove it from the HashMap in O(1) without a reverse lookup.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class Node<K, V> {

    public K key;
    public V value;

    // Expiry time in milliseconds (System.currentTimeMillis).
    // -1 means no TTL set — node lives forever until evicted by policy.
    public long expiryTime;

    public Node<K, V> prev;
    public Node<K, V> next;
    public int frequency;

    // ── Constructors ──────────────────────────────────────────────────────────

    /** Standard node with no TTL. */
    public Node(K key, V value) {
        this.key        = key;
        this.value      = value;
        this.expiryTime = -1;
        this.prev       = null;
        this.next       = null;
        this.frequency  = 0;
    }

    /** Node with TTL. ttlMillis is the duration, not the absolute timestamp. */
    public Node(K key, V value, long ttlMillis) {
        this(key, value);
        this.expiryTime = System.currentTimeMillis() + ttlMillis;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns true if this node has expired.
     * A node with expiryTime == -1 never expires.
     */
    public boolean isExpired() {
        return expiryTime != -1 && System.currentTimeMillis() > expiryTime;
    }

    @Override
    public String toString() {
        return "Node[key=" + key + ", value=" + value + ", expired=" + isExpired() + "]";
    }
}