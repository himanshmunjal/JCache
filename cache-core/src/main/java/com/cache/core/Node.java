package com.cache.core;

/**
 * Entry in a {@link DoublyLinkedList}. The key is kept alongside the value so
 * that a node taken off the tail can be removed from the owning map without a
 * reverse lookup.
 *
 * <p>Fields are public because the policy classes manipulate them directly on
 * the hot path; the node is never exposed outside the cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class Node<K, V> {

    /** Marker for "never expires". */
    public static final long NO_EXPIRY = -1L;

    /** The entry's key. */
    public K key;
    /** The entry's value. */
    public V value;
    /** Absolute expiry time in epoch millis, or {@link #NO_EXPIRY}. */
    public long expiryTime;
    /** Access count, used by LFU. ARC uses it to record which list the node is on. */
    public int frequency;

    /** Previous node in the list. */
    public Node<K, V> prev;
    /** Next node in the list. */
    public Node<K, V> next;

    /**
     * Creates a node that never expires.
     *
     * @param key   the key
     * @param value the value
     */
    public Node(K key, V value) {
        this.key = key;
        this.value = value;
        this.expiryTime = NO_EXPIRY;
    }

    /**
     * Creates a node that expires {@code ttlMillis} from now.
     *
     * @param key       the key
     * @param value     the value
     * @param ttlMillis time to live in milliseconds
     */
    public Node(K key, V value, long ttlMillis) {
        this(key, value);
        this.expiryTime = System.currentTimeMillis() + ttlMillis;
    }

    /**
     * Sets the expiry relative to now.
     *
     * @param ttlMillis time to live in milliseconds, or {@link #NO_EXPIRY}
     */
    public void expireAfter(long ttlMillis) {
        this.expiryTime = ttlMillis == NO_EXPIRY ? NO_EXPIRY : System.currentTimeMillis() + ttlMillis;
    }

    /**
     * Returns whether the node's expiry time has passed.
     *
     * @return {@code true} if expired
     */
    public boolean isExpired() {
        return expiryTime != NO_EXPIRY && System.currentTimeMillis() > expiryTime;
    }

    @Override
    public String toString() {
        return "Node[key=" + key + ", value=" + value + ", expired=" + isExpired() + "]";
    }
}
