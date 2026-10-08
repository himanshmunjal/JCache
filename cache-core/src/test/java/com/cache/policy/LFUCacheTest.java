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

    @Test
    @DisplayName("Capacity one behaves correctly")
    void testCapacityOne() {
        Cache<String, String> tiny = new LFUCache<>(1);

        tiny.put("A", "1");
        tiny.put("B", "2");

        assertNull(tiny.get("A"));
        assertEquals("2", tiny.get("B"));
    }

    @Test
    @DisplayName("Capacity zero is rejected")
    void testCapacityZero() {
        assertThrows(IllegalArgumentException.class, () -> new LFUCache<>(0));
        assertThrows(IllegalArgumentException.class, () -> new LFUCache<>(-1));
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

        assertTrue(cache.getStats().evictions() >= 1);
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
        cache.get("missing");
        cache.get("missing2");

        assertEquals(2, cache.getStats().misses());
    }

    @Test
    @DisplayName("A key read more than 10,000 times can still be evicted later")
    void testHeavilyReadKey_doesNotBreakEviction() {
        LFUCache<String, String> lfu = new LFUCache<>(3);
        lfu.put("hot", "1");
        for (int i = 0; i < 25_000; i++) {
            lfu.get("hot");
        }
        for (int i = 0; i < 100; i++) {
            lfu.put("k" + i, "v");
        }
        assertEquals(3, lfu.size());
        assertEquals("1", lfu.get("hot"));
    }

    @Test
    @DisplayName("Evicting the only lowest-frequency key keeps eviction working")
    void testExplicitEvictOfMinFrequencyKey() {
        LFUCache<String, String> lfu = new LFUCache<>(2);
        lfu.put("a", "1");
        lfu.put("b", "2");
        lfu.get("b");
        lfu.evict("a");
        assertNotNull(lfu.evict());
        lfu.put("c", "3");
        lfu.put("d", "4");
        lfu.put("e", "5");
        assertEquals(2, lfu.size());
    }

    @Test
    @DisplayName("Ties at the lowest frequency evict the least recently used key")
    void testTieBreakIsLeastRecentlyUsed() {
        LFUCache<String, String> lfu = new LFUCache<>(2);
        lfu.put("old", "1");
        lfu.put("new", "2");
        lfu.put("third", "3");
        assertNull(lfu.peek("old"));
        assertEquals("2", lfu.peek("new"));
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
