package com.cache.api;

/**
 * Immutable snapshot of a cache's counters.
 *
 * @param hits      lookups that found a value
 * @param misses    lookups that found nothing
 * @param evictions entries the policy removed to make room; explicit removals
 *                  and expiry are not counted
 * @param hitRate   {@code hits / (hits + misses)}, or 0 when there were no lookups
 */
public record CacheStats(long hits, long misses, long evictions, double hitRate) {

    /**
     * Builds a snapshot and derives the hit rate from the two counters.
     *
     * @param hits      lookups that found a value
     * @param misses    lookups that found nothing
     * @param evictions entries the policy removed to make room
     * @return the snapshot
     */
    public static CacheStats of(long hits, long misses, long evictions) {
        long lookups = hits + misses;
        return new CacheStats(hits, misses, evictions, lookups == 0 ? 0.0 : (double) hits / lookups);
    }
}
