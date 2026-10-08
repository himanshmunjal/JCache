package com.cache.policy;

import com.cache.api.CacheStats;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LRUCache Tests")
class LRUCacheTest {
    private LRUCache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new LRUCache<>(3);
    }

    @Test
    @DisplayName("get() returns correct value after put()")
    void testBasicPutAndGet() {
        cache.put("name", "Alice");

        assertEquals("Alice", cache.get("name"));
    }

    @Test
    @DisplayName("get() returns null for key that was never put")
    void testGet_missingKey_returnsNull() {
        assertNull(cache.get("ghost"));
    }

    @Test
    @DisplayName("Multiple keys stored and retrieved independently")
    void testMultipleKeys_storeAndRetrieveCorrectly() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        assertEquals("1", cache.get("a"));
        assertEquals("2", cache.get("b"));
        assertEquals("3", cache.get("c"));
    }

    @Test
    @DisplayName("size() reflects number of unique keys stored")
    void testSize_incrementsWithUniqueKeys() {
        assertEquals(0, cache.size());
        cache.put("x", "1");
        assertEquals(1, cache.size());
        cache.put("y", "2");
        assertEquals(2, cache.size());
    }

    @Test
    @DisplayName("size() never exceeds capacity")
    void testSize_neverExceedsCapacity() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        cache.put("d", "4");
        cache.put("e", "5");

        assertEquals(3, cache.size(), "Size must never exceed capacity");
    }

    @Test
    @DisplayName("Eviction removes the least recently used key (insertion order)")
    void testEviction_removesLRUKey_insertionOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.put("D", "4");

        assertNull(cache.get("A"),  "A should have been evicted (LRU)");
        assertNotNull(cache.get("B"), "B should still be present");
        assertNotNull(cache.get("C"), "C should still be present");
        assertNotNull(cache.get("D"), "D (just inserted) should be present");
    }

    @Test
    @DisplayName("Eviction removes correct key when access order differs from insertion order")
    void testEviction_removesCorrectKey_afterAccessReorder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");

        cache.put("D", "4");

        assertNotNull(cache.get("A"), "A was recently accessed, should survive");
        assertNull(cache.get("B"),    "B is LRU after A was accessed, should be evicted");
        assertNotNull(cache.get("C"), "C should survive");
        assertNotNull(cache.get("D"), "D (just inserted) should be present");
    }

    @Test
    @DisplayName("Sequential evictions always remove the correct LRU key")
    void testEviction_chainOfEvictions_correctOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");
        cache.put("E", "5");
        cache.put("F", "6");

        assertNull(cache.get("A"), "A evicted first");
        assertNull(cache.get("B"), "B evicted second");
        assertNull(cache.get("C"), "C evicted third");
        assertNotNull(cache.get("D"), "D survived");
        assertNotNull(cache.get("E"), "E survived");
        assertNotNull(cache.get("F"), "F survived");
    }

    @Test
    @DisplayName("get() moves key to MRU position, protecting it from eviction")
    void testMoveToFront_onGet_protectsFromEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");

        cache.put("D", "4");

        assertNotNull(cache.get("A"), "A was recently accessed so should NOT be evicted");
        assertNull(cache.get("B"),    "B became LRU after A was accessed");
    }

    @Test
    @DisplayName("Multiple get() calls update recency correctly each time")
    void testMoveToFront_multipleGets_correctFinalOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");
        cache.get("B");

        cache.put("D", "4");

        assertNull(cache.get("C"),    "C is LRU after A and B were accessed");
        assertNotNull(cache.get("A"), "A was accessed, should survive");
        assertNotNull(cache.get("B"), "B was most recently accessed, should survive");
        assertNotNull(cache.get("D"), "D just inserted, should be present");
    }

    @Test
    @DisplayName("get() on missing key does not affect recency of existing keys")
    void testMoveToFront_missedGet_doesNotAffectOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("MISSING");

        cache.put("D", "4");

        assertNull(cache.get("A"),    "A should still be LRU after a missed get");
        assertNotNull(cache.get("B"), "B order unchanged");
        assertNotNull(cache.get("C"), "C order unchanged");
    }

    @Test
    @DisplayName("Duplicate put() updates value and moves key to MRU position")
    void testDuplicateKey_updatesValueAndResetsRecency() {
        cache.put("A", "old");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.put("A", "new");

        cache.put("D", "4");

        assertEquals("new", cache.get("A"), "Value should be updated on duplicate put");
        assertNull(cache.get("B"),          "B should be evicted (LRU after A re-put)");
        assertNotNull(cache.get("C"),       "C should survive");
    }

    @Test
    @DisplayName("Duplicate put() does not increase size")
    void testDuplicateKey_doesNotIncreaseSize() {
        cache.put("A", "1");
        cache.put("A", "2");
        cache.put("A", "3");

        assertEquals(1, cache.size(), "Duplicate puts on same key must not increase size");
    }

    @Test
    @DisplayName("Duplicate puts on multiple keys all update correctly")
    void testDuplicateKey_multipleKeys_allUpdateCorrectly() {
        cache.put("A", "a1");
        cache.put("B", "b1");
        cache.put("A", "a2");
        cache.put("B", "b2");

        assertEquals("a2", cache.get("A"));
        assertEquals("b2", cache.get("B"));
        assertEquals(2, cache.size());
    }

    @Test
    @DisplayName("Capacity=1: every put evicts the previous key")
    void testCapacityOne_evictsOnEveryPut() {
        LRUCache<String, String> tiny = new LRUCache<>(1);

        tiny.put("A", "1");
        assertEquals("1", tiny.get("A"));
        assertEquals(1, tiny.size());

        tiny.put("B", "2");
        assertNull(tiny.get("A"),    "A should be evicted");
        assertEquals("2", tiny.get("B"));
        assertEquals(1, tiny.size());

        tiny.put("C", "3");
        assertNull(tiny.get("B"),    "B should be evicted");
        assertEquals("3", tiny.get("C"));
        assertEquals(1, tiny.size());
    }

    @Test
    @DisplayName("Capacity=1: duplicate put updates value without eviction")
    void testCapacityOne_duplicatePut_noEviction() {
        LRUCache<String, String> tiny = new LRUCache<>(1);

        tiny.put("A", "old");
        tiny.put("A", "new");

        assertEquals("new", tiny.get("A"));
        assertEquals(1, tiny.size());
    }

    @Test
    @DisplayName("Capacity=1: get() then put different key still evicts")
    void testCapacityOne_getFollowedByPut_stillEvicts() {
        LRUCache<String, String> tiny = new LRUCache<>(1);

        tiny.put("A", "1");
        tiny.get("A");
        tiny.put("B", "2");

        assertNull(tiny.get("A"),    "A must be evicted even though it was accessed");
        assertEquals("2", tiny.get("B"));
    }

    @Test
    @DisplayName("Capacity=2: access pattern correctly determines eviction")
    void testCapacityTwo_accessPatternDeterminesEviction() {
        LRUCache<String, String> small = new LRUCache<>(2);

        small.put("A", "1");
        small.put("B", "2");
        small.get("A");
        small.put("C", "3");

        assertNull(small.get("B"),    "B is LRU, should be evicted");
        assertNotNull(small.get("A"), "A was accessed, should survive");
        assertNotNull(small.get("C"), "C just inserted, should be present");
    }

    @Test
    @DisplayName("CacheStats: hit count increments on successful get()")
    void testStats_hitCountIncrements() {
        cache.put("key", "value");

        cache.get("key");
        cache.get("key");
        cache.get("key");

        CacheStats stats = cache.getStats();
        assertEquals(3, stats.hits(), "3 successful gets = 3 hits");
    }

    @Test
    @DisplayName("CacheStats: miss count increments on failed get()")
    void testStats_missCountIncrements() {
        cache.get("nope");
        cache.get("nope2");

        CacheStats stats = cache.getStats();
        assertEquals(2, stats.misses(), "2 failed gets = 2 misses");
    }

    @Test
    @DisplayName("CacheStats: hits + misses equals total get() calls")
    void testStats_hitsAndMissesSumToTotalGets() {
        cache.put("A", "1");
        cache.put("B", "2");

        cache.get("A");
        cache.get("B");
        cache.get("C");
        cache.get("A");
        cache.get("GHOST");

        CacheStats stats = cache.getStats();
        long totalGets = stats.hits() + stats.misses();

        assertEquals(5, totalGets,          "Total gets should be 5");
        assertEquals(3, stats.hits(),    "3 hits (A, B, A)");
        assertEquals(2, stats.misses(),  "2 misses (C, GHOST)");
    }

    @Test
    @DisplayName("CacheStats: eviction count increments on capacity breach")
    void testStats_evictionCountIncrements_onCapacityBreach() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4");
        cache.put("E", "5");

        CacheStats stats = cache.getStats();
        assertEquals(2, stats.evictions(), "2 puts beyond capacity = 2 evictions");
    }

    @Test
    @DisplayName("CacheStats: duplicate put() does not count as eviction")
    void testStats_duplicatePut_doesNotCountAsEviction() {
        cache.put("A", "1");
        cache.put("A", "2");
        cache.put("A", "3");

        CacheStats stats = cache.getStats();
        assertEquals(0, stats.evictions(), "Updates to existing keys are not evictions");
    }

    @Test
    @DisplayName("CacheStats: all counters start at zero on fresh cache")
    void testStats_initialState_allZero() {
        CacheStats stats = cache.getStats();

        assertEquals(0, stats.hits());
        assertEquals(0, stats.misses());
        assertEquals(0, stats.evictions());
    }

    @Test
    @DisplayName("CacheStats: hit rate computed correctly")
    void testStats_hitRate_computedCorrectly() {
        cache.put("A", "1");

        cache.get("A");
        cache.get("A");
        cache.get("B");

        CacheStats stats = cache.getStats();

        double expectedRate = 2.0 / 3.0;
        assertEquals(expectedRate, stats.hitRate(), 0.001,
                "Hit rate should be 2/3 with 2 hits and 1 miss");
    }

    @Test
    @DisplayName("CacheStats: hit rate is 0.0 on fresh cache (no division by zero)")
    void testStats_hitRate_noDivisionByZero() {
        CacheStats stats = cache.getStats();

        assertDoesNotThrow(stats::hitRate, "Hit rate on empty stats must not throw");
        assertEquals(0.0, stats.hitRate(), "Hit rate with no gets should be 0.0");
    }

    @Test
    @DisplayName("evict() removes the key, get() returns null afterward")
    void testExplicitEvict_removesKey() {
        cache.put("key", "value");
        cache.evict("key");

        assertNull(cache.get("key"), "Evicted key should return null");
        assertEquals(0, cache.size(), "Size should decrease after evict");
    }

    @Test
    @DisplayName("evict() frees capacity for new keys")
    void testExplicitEvict_freesCapacity() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.evict("A");

        cache.put("D", "4");

        assertNotNull(cache.get("B"), "B should still be present");
        assertNotNull(cache.get("C"), "C should still be present");
        assertNotNull(cache.get("D"), "D should be present in freed slot");
        assertEquals(3, cache.size());
    }

    @Test
    @DisplayName("evict() on nonexistent key is a safe no-op")
    void testExplicitEvict_nonexistentKey_safeNoOp() {
        cache.put("A", "1");

        assertDoesNotThrow(() -> cache.evict("GHOST"),
                "Evicting a nonexistent key must not throw");

        assertEquals(1, cache.size(), "Size must not change after evicting a nonexistent key");
        assertEquals("1", cache.get("A"), "Existing key must not be affected");
    }

    @Test
    @DisplayName("evict() is not counted as an eviction in CacheStats")
    void testExplicitEvict_notCountedAsEviction() {
        cache.put("A", "1");
        cache.put("B", "2");

        cache.evict("A");
        cache.evict("B");

        assertEquals(0, cache.getStats().evictions(),
                "Explicit removals are not capacity evictions");
    }

    @Test
    @DisplayName("put() with null key throws IllegalArgumentException")
    void testNullKey_put_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put(null, "value"));
    }

    @Test
    @DisplayName("put() with null value throws IllegalArgumentException")
    void testNullValue_put_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("key", null));
    }

    @Test
    @DisplayName("get() with null key throws IllegalArgumentException")
    void testNullKey_get_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> cache.get(null));
    }

    @Test
    @DisplayName("Constructor with capacity <= 0 throws IllegalArgumentException")
    void testConstructor_invalidCapacity_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> new LRUCache<>(0));
        assertThrows(IllegalArgumentException.class, () -> new LRUCache<>(-1));
    }

    @Test
    @DisplayName("get() on full cache never causes eviction")
    void testFullCache_getDoesNotCauseEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A");
        cache.get("B");
        cache.get("C");

        assertEquals(3, cache.size(), "Gets on full cache must not cause eviction");
        assertEquals(0, cache.getStats().evictions());
    }

    @Test
    @DisplayName("Duplicate put() on full cache does not cause eviction")
    void testFullCache_duplicatePut_doesNotCauseEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.put("A", "updated");
        cache.put("B", "updated");

        assertEquals(3, cache.size());
        assertEquals(0, cache.getStats().evictions(),
                "Updates to existing keys must not count as evictions");
        assertEquals("updated", cache.get("A"));
        assertEquals("updated", cache.get("B"));
    }
}
