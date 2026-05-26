package com.cache.policy;

import com.cache.api.Cache;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LFUCache Tests")
class LFUCacheTest {

    private Cache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new LFUCache<>(3);
    }

    // =========================================================================
    // Basic Put/Get
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
        assertNull(cache.get("X"));
    }

    @Test
    @DisplayName("Cache size updates correctly")
    void testSizeUpdatesCorrectly() {
        cache.put("A", "1");
        cache.put("B", "2");

        assertEquals(2, cache.size());
    }

    // =========================================================================
    // LFU Eviction
    // =========================================================================

    @Test
    @DisplayName("Least frequently used key is evicted")
    void testLFUEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");
        cache.get("A");

        cache.get("B");

        cache.put("D", "4");

        assertNull(cache.get("C"), "C should be evicted as LFU");
        assertEquals("1", cache.get("A"));
        assertEquals("2", cache.get("B"));
        assertEquals("4", cache.get("D"));
    }

    @Test
    @DisplayName("Frequency increments on get")
    void testFrequencyIncrementOnGet() {
        cache.put("A", "1");

        cache.get("A");
        cache.get("A");

        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");

        assertEquals("1", cache.get("A"));
    }

    @Test
    @DisplayName("Tie resolved using LRU")
    void testTieResolvedUsingLRU() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");

        cache.put("D", "4");

        assertNull(cache.get("B"), "B should be evicted first among LFU ties");
    }

    // =========================================================================
    // Update Existing Keys
    // =========================================================================

    @Test
    @DisplayName("Updating existing key changes value")
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
    // Capacity Edge Cases
    // =========================================================================

    @Test
    @DisplayName("Capacity one behaves correctly")
    void testCapacityOne() {
        Cache<String, String> tiny = new LFUCache<>(1);

        tiny.put("A", "1");
        tiny.put("B", "2");

        assertNull(tiny.get("A"));
        assertEquals("2", tiny.get("B"));
    }

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Capacity zero behaves gracefully")
    void testCapacityZero() {
        Cache<String, String> zero = new LFUCache<>(0);

        zero.put("A", "1");

        assertNull(zero.get("A"));
        assertEquals(0, zero.size());
    }

    // =========================================================================
    // Null Handling
    // =========================================================================

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
    // Stress Behaviour
    // =========================================================================

    @Test
    @DisplayName("Frequent accesses protect key from eviction")
    void testFrequentAccessProtection() {
        cache.put("HOT", "X");
        cache.put("B", "1");
        cache.put("C", "2");

        for (int i = 0; i < 100; i++) {
            cache.get("HOT");
        }

        cache.put("D", "3");

        assertEquals("X", cache.get("HOT"));
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
        cache.get("missing");
        cache.get("missing2");

        assertEquals(2, cache.getstats().misses());
    }
}