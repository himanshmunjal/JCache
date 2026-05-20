package com.cache.policy;

import org.junit.jupiter.api.*;
import com.cache.api.CacheStats;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LFUCache Tests")
public class LFUCacheTest {
    private LFUCache<String, String> cache;

    @BeforeEach
    void setup(){
        cache = new LFUCache<>(3);
    }

    @Test
    @DisplayName("get() returns correct value after put()")
    void testBasic(){
        cache.put("name", "Alice");

        assertEquals("Alice", cache.get("name"));
    }

    @Test
    @DisplayName("Basic frequency tracking")
    void testBasicFrequencyTracking() {
        cache.put("A", "N1");
        cache.get("A");
        cache.get("A");
        cache.put("B", "N2");
        cache.put("C", "N3");
        // Cache full here (assuming capacity = 3)
        cache.put("D", "N4");
        // B should be evicted because it has lowest frequency
        assertNull(cache.get("B"), "B should have been evicted (LFU)");
        // A should still exist because it was accessed most
        assertEquals("N1", cache.get("A"));
        assertEquals("N3", cache.get("C"));
        assertEquals("N4", cache.get("D"));
    }



}
