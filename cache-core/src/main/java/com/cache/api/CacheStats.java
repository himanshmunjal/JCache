package com.cache.api;

public record CacheStats(
    long hits,
    long misses,
    long evictions,
    double hitRate
) {}