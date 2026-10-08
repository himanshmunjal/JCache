package com.cache.api;

/**
 * A bounded key-value cache.
 *
 * <p>Implementations decide what to evict when they are full. Unless an
 * implementation says otherwise it is not thread-safe; wrap it in one of the
 * classes in {@code com.cache.concurrent} to share it between threads.
 *
 * <p>Null keys and null values are not supported.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Cache<K, V> {

    /**
     * Returns the value mapped to {@code key}, or {@code null} if there is none.
     * A successful lookup counts as an access for the eviction policy.
     *
     * @param key the key to look up
     * @return the cached value, or {@code null}
     */
    V get(K key);

    /**
     * Stores a value, replacing any existing mapping for the key. If the cache
     * is full and the key is new, one entry is evicted first.
     *
     * @param key   the key
     * @param value the value
     */
    void put(K key, V value);

    /**
     * Removes the mapping for {@code key} if present. This is not counted as
     * an eviction in {@link #getStats()}.
     *
     * @param key the key to remove
     */
    void evict(K key);

    /**
     * Returns the value mapped to {@code key} without counting it as an access:
     * eviction order and hit/miss statistics are left unchanged.
     *
     * @param key the key to look up
     * @return the cached value, or {@code null}
     */
    V peek(K key);

    /**
     * Returns {@code true} if the cache holds a live mapping for {@code key}.
     * Like {@link #peek(Object)}, this does not count as an access.
     *
     * @param key the key to check
     * @return whether the key is present
     */
    default boolean containsKey(K key) {
        return peek(key) != null;
    }

    /**
     * Returns the number of entries currently stored.
     *
     * @return the entry count
     */
    int size();

    /**
     * Returns a point-in-time snapshot of hit, miss and eviction counters.
     *
     * @return the current statistics
     */
    CacheStats getStats();

    /** Removes every entry. Statistics are kept. */
    void clear();
}
