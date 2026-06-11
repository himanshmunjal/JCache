package com.cache.persistence;

import java.util.Map;

/**
 * SnapshotCapable is a marker interface that cache implementations can
 * optionally implement to support persistence via SnapshotWriter.
 *
 * WHY A SEPARATE INTERFACE?
 *
 * The core Cache<K,V> interface deliberately has no keySet() or entrySet()
 * method — iteration is not part of the cache contract (it's not an O(1)
 * operation and exposing it encourages misuse). SnapshotCapable provides
 * the minimum surface needed for persistence without polluting Cache<K,V>.
 *
 * Implementing classes: LRUCache, LFUCache, ARCCache (optional — only
 * needed if you want persistence support for that policy).
 *
// * @param <K> Key type (typically String for the persistence layer).
// * @param <V> Value type (typically String).
 */
public interface SnapshotCapable {

    /**
     * Returns a snapshot of all live cache entries as a map of
     * key → absolute expiry timestamp in milliseconds.
     *
     * CONTRACT:
     *   - Returns all keys currently in the cache (not expired).
     *   - expiryTimeMs = -1 means the key has no expiry.
     *   - expiryTimeMs = absolute System.currentTimeMillis() value.
     *   - The returned map is a SNAPSHOT — changes to the cache after
     *     this call are not reflected in the returned map.
     *   - The map is unmodifiable (or a defensive copy).
     *
     * PERFORMANCE:
     *   This is an O(N) operation. It should only be called during
     *   snapshot creation, not on the hot path.
     *
     * @return Map of key → expiryTimeMs for all live entries.
     */
    Map<String, Long> getSnapshotEntries();
}