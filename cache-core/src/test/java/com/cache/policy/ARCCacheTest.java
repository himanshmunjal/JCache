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

        assertTrue(cache.getStats().evictions() >= 1);
    }

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

    @Test
    @DisplayName("Capacity one behaves correctly")
    void testCapacityOne() {
        Cache<String, String> tiny = new ARCCache<>(1);

        tiny.put("A", "1");
        tiny.put("B", "2");

        assertNull(tiny.get("A"));
        assertEquals("2", tiny.get("B"));
    }

    @Test
    @DisplayName("Capacity zero is rejected")
    void testCapacityZero() {
        assertThrows(IllegalArgumentException.class, () -> new ARCCache<>(0));
        assertThrows(IllegalArgumentException.class, () -> new ARCCache<>(-1));
    }

    @Test
    @DisplayName("Null key throws exception")
    void testNullKey() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put(null, "1"));
    }

    @Test
    @DisplayName("Null value throws exception")
    void testNullValue() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("A", null));
    }

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

    @Test
    @DisplayName("Hit count increments")
    void testHitStats() {
        cache.put("A", "1");

        cache.get("A");
        cache.get("A");

        assertEquals(2, cache.getStats().hits());
    }

    @Test
    @DisplayName("Miss count increments")
    void testMissStats() {
        cache.get("X");
        cache.get("Y");

        assertEquals(2, cache.getStats().misses());
    }

    @Test
    @DisplayName("Eviction count increments")
    void testEvictionStats() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");

        assertTrue(cache.getStats().evictions() >= 1);
    }

    @Test
    @DisplayName("Repeated hits on a T2 entry do not corrupt list sizes")
    void testRepeatedHitsKeepSizesConsistent() {
        ARCCache<String, String> arc = new ARCCache<>(3);
        arc.put("A", "1");
        for (int i = 0; i < 100; i++) {
            arc.get("A");
        }
        for (int i = 0; i < 50; i++) {
            arc.put("k" + i, "v");
            assertTrue(arc.size() <= 3);
        }
        assertEquals("1", arc.get("A"));
    }

    @Test
    @DisplayName("A put that hits ghost list B1 grows the recency target")
    void testGhostHitInB1_increasesTarget() {
        ARCCache<String, String> arc = new ARCCache<>(2);
        arc.put("A", "1");
        arc.get("A");
        arc.put("B", "2");
        arc.put("C", "3");
        assertNull(arc.peek("B"));
        assertEquals(0, arc.targetRecencySize());

        arc.put("B", "2");

        assertEquals(1, arc.targetRecencySize());
        assertEquals("2", arc.get("B"));
        assertEquals(2, arc.size());
    }

    @Test
    @DisplayName("Ghost lists stay bounded under a long stream of unique keys")
    void testGhostListsAreBounded() {
        ARCCache<String, String> arc = new ARCCache<>(10);
        for (int i = 0; i < 100_000; i++) {
            arc.put("key-" + i, "v");
        }
        assertEquals(10, arc.size());
        assertTrue(arc.ghostCount() <= 10, "ghosts: " + arc.ghostCount());
    }

    @Test
    @DisplayName("evict() is not counted as an eviction; capacity evictions are")
    void explicitEvictIsNotCountedAsEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.evict("A");
        assertEquals(0, cache.getStats().evictions());

        cache.put("C", "3");
        cache.put("D", "4");
        cache.put("E", "5");
        assertEquals(1, cache.getStats().evictions());
    }
}
