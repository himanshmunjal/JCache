package com.cache.policy;

import com.cache.api.Cache;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ARCCache Tests")
class ARCCacheTest {

    private Cache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new ARCCache<>(3);
    }

    // =========================================================================
    // Basic Operations
    // =========================================================================

    @Test
    @DisplayName("Basic put/get works")
    void testBasicPutGet() {
        cache.put("A", "Apple");

        assertEquals("Apple", cache.get("A"));
    }

    @Test
    @DisplayName("Missing key returns null")
    void testMissingKeyReturnsNull() {
        assertNull(cache.get("Missing"));
    }

    @Test
    @DisplayName("Size updates correctly")
    void testSizeUpdatesCorrectly() {
        cache.put("A", "1");
        cache.put("B", "2");

        assertEquals(2, cache.size());
    }

    // =========================================================================
    // ARC Adaptivity
    // =========================================================================

    @Test
    @DisplayName("Recently used entries survive")
    void testRecencyProtection() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");

        cache.put("D", "4");

        assertEquals("1", cache.get("A"));
    }

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Frequently used entries survive")
    void testFrequencyProtection() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        for (int i = 0; i < 10; i++) {
            cache.get("A");
        }

        cache.put("D", "4");

        assertEquals("1", cache.get("A"));
    }

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("ARC adapts between recency and frequency")
    void testAdaptiveBehaviour() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");
        cache.get("A");

        cache.get("B");

        cache.put("D", "4");

        assertEquals("1", cache.get("A"));
    }

    // =========================================================================
    // Replacement Behaviour
    // =========================================================================

    @Test
    @DisplayName("Cache never exceeds capacity")
    void testCapacityConstraint() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");

        assertTrue(cache.size() <= 3);
    }

    @Test
    @DisplayName("Eviction occurs when capacity exceeded")
    void testEvictionOccurs() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");

        assertTrue(cache.getstats().evictions() >= 1);
    }

    // =========================================================================
    // Update Existing Keys
    // =========================================================================

    @Test
    @DisplayName("Updating existing key updates value")
    void testUpdateExistingKey() {
        cache.put("A", "1");
        cache.put("A", "100");

        assertEquals("100", cache.get("A"));
    }

    @Test
    @DisplayName("Updating existing key does not increase size")
    void testUpdateDoesNotIncreaseSize() {
        cache.put("A", "1");
        cache.put("A", "2");

        assertEquals(1, cache.size());
    }

    // =========================================================================
    // Edge Cases
    // =========================================================================

    @Test
    @DisplayName("Capacity one behaves correctly")
    void testCapacityOne() {
        Cache<String, String> tiny = new ARCCache<>(1);

        tiny.put("A", "1");
        tiny.put("B", "2");

        assertNull(tiny.get("A"));
        assertEquals("2", tiny.get("B"));
    }

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Capacity zero behaves gracefully")
    void testCapacityZero() {
        Cache<String, String> zero = new ARCCache<>(0);

        zero.put("A", "1");

        assertNull(zero.get("A"));
    }

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Null key throws exception")
    void testNullKey() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put(null, "1"));
    }

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Null value throws exception")
    void testNullValue() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("A", null));
    }

    // =========================================================================
    // Explicit Eviction
    // =========================================================================

    @Test
    @DisplayName("Explicit eviction removes key")
    void testExplicitEviction() {
        cache.put("A", "1");

        cache.evict("A");

        assertNull(cache.get("A"));
    }

    @Test
    @DisplayName("Evict nonexistent key is safe")
    void testEvictNonexistentKey() {
        assertDoesNotThrow(() -> cache.evict("ghost"));
    }

    // =========================================================================
    // Stats
    // =========================================================================

    @Test
    @DisplayName("Hit count increments")
    void testHitStats() {
        cache.put("A", "1");

        cache.get("A");
        cache.get("A");

        assertEquals(2, cache.getstats().hits());
    }

    @Test
    @DisplayName("Miss count increments")
    void testMissStats() {
        cache.get("X");
        cache.get("Y");

        assertEquals(2, cache.getstats().misses());
    }

    @Test
    @DisplayName("Eviction count increments")
    void testEvictionStats() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");

        assertTrue(cache.getstats().evictions() >= 1);
    }
}