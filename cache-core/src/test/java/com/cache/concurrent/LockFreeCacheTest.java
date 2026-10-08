package com.cache.concurrent;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LockFreeCacheTest {

    @Test
    void explicitEvictIsNotCountedAsEviction() {
        LockFreeCache<String, String> cache = new LockFreeCache<>(2);
        cache.put("a", "1");
        cache.put("b", "2");
        cache.evict("a");
        assertEquals(0, cache.getStats().evictions());

        cache.put("c", "3");
        cache.put("d", "4");
        assertEquals(1, cache.getStats().evictions());
        assertEquals(2, cache.size());
    }
}
