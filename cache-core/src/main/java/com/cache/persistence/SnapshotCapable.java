package com.cache.persistence;

import java.util.Map;

/**
 * Implemented by caches that can list their keys for {@link SnapshotWriter}.
 * {@link com.cache.api.Cache} deliberately has no way to iterate entries, so
 * persistence needs this extra capability.
 */
public interface SnapshotCapable {

    /**
     * Lists every live key with its absolute expiry time in epoch millis, or
     * {@code -1} if the key never expires. The returned map is a copy and is
     * not updated by later changes to the cache.
     *
     * @return key to expiry time
     */
    Map<String, Long> getSnapshotEntries();
}
