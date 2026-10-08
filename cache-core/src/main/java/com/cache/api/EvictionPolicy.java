package com.cache.api;

/**
 * Hooks a policy-based cache exposes so callers can drive or observe eviction.
 *
 * @param <K> key type
 */
public interface EvictionPolicy<K> {

    /**
     * Records an access to {@code key}.
     *
     * @param key the key that was read or updated
     */
    void onAccess(K key);

    /**
     * Records that {@code key} was just inserted.
     *
     * @param key the new key
     */
    void onInsert(K key);

    /**
     * Evicts the entry the policy would choose next.
     *
     * @return the evicted key, or {@code null} if the cache was empty
     */
    K evict();
}
