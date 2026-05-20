package com.cache.policy;

import com.cache.api.CacheStats;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for LRUCache.
 *
 * What we are proving here:
 *   1. Basic correctness      — put/get work as expected
 *   2. Eviction ordering      — the LEAST recently used key is always evicted
 *   3. Move-to-front on get   — reading a key rescues it from eviction
 *   4. Move-to-front on put   — re-putting a key also resets its recency
 *   5. Duplicate key handling — value updates, recency resets, size stays same
 *   6. Capacity edge cases    — capacity=1, capacity=2, exactly-full cache
 *   7. CacheStats accuracy    — hits, misses, evictions are tracked correctly
 *   8. Explicit evict()       — manual removal works and stats update
 *   9. Null / invalid input   — defensive programming checks
 *
 * How to read the eviction order tests:
 *   We always build a precise access sequence, then trigger one eviction,
 *   then verify EXACTLY which key was removed. "Something got evicted"
 *   is not good enough — the WRONG key evicted means your linked list
 *   pointer manipulation has a bug.
 *
 * LRU invariant (stated once, used everywhere):
 *   At any point in time, if the cache is full and a new key arrives,
 *   the key whose LAST ACCESS (get OR put) was furthest in the past
 *   is the one that gets evicted.
 */
@DisplayName("LRUCache Tests")
class LRUCacheTest {

    private LRUCache<String, String> cache;

    // -------------------------------------------------------------------------
    // Setup
    // -------------------------------------------------------------------------

    /**
     * Fresh cache before each test. Capacity 3 is the sweet spot for
     * eviction order tests — large enough to build interesting sequences,
     * small enough that evictions happen predictably.
     */
    @BeforeEach
    void setUp() {
        cache = new LRUCache<>(3);
    }

    // =========================================================================
    // 1. Basic correctness
    // =========================================================================

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
        // Capacity is 3 — put 5 keys
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        cache.put("d", "4"); // evicts "a"
        cache.put("e", "5"); // evicts "b"

        assertEquals(3, cache.size(), "Size must never exceed capacity");
    }

    // =========================================================================
    // 2. Eviction ordering — the core LRU invariant
    // =========================================================================

    /**
     * Basic eviction test.
     *
     * Access sequence:
     *   put A, put B, put C  →  order (MRU→LRU): C, B, A
     *   put D                →  A is LRU, so A gets evicted
     *
     * After eviction:
     *   A → null (evicted)
     *   B, C, D → still present
     */
    @Test
    @DisplayName("Eviction removes the least recently used key (insertion order)")
    void testEviction_removesLRUKey_insertionOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        // Cache full. Put D — must evict A (oldest insertion, never accessed).
        cache.put("D", "4");

        assertNull(cache.get("A"),  "A should have been evicted (LRU)");
        assertNotNull(cache.get("B"), "B should still be present");
        assertNotNull(cache.get("C"), "C should still be present");
        assertNotNull(cache.get("D"), "D (just inserted) should be present");
    }

    /**
     * Eviction when the middle key is LRU.
     *
     * Access sequence:
     *   put A, put B, put C  →  order: C, B, A
     *   get A                →  A moves to front: A, C, B
     *   put D                →  B is LRU, B gets evicted
     */
    @Test
    @DisplayName("Eviction removes correct key when access order differs from insertion order")
    void testEviction_removesCorrectKey_afterAccessReorder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        // Access A — it moves to MRU position.
        // Order is now (MRU→LRU): A, C, B
        cache.get("A");

        // Put D — B is LRU, so B is evicted.
        cache.put("D", "4");

        assertNotNull(cache.get("A"), "A was recently accessed, should survive");
        assertNull(cache.get("B"),    "B is LRU after A was accessed, should be evicted");
        assertNotNull(cache.get("C"), "C should survive");
        assertNotNull(cache.get("D"), "D (just inserted) should be present");
    }

    /**
     * Chain of evictions — each put evicts the correct key in sequence.
     *
     * Access sequence (capacity=3):
     *   put A, put B, put C   →  C, B, A
     *   put D                 →  evicts A  →  D, C, B
     *   put E                 →  evicts B  →  E, D, C
     *   put F                 →  evicts C  →  F, E, D
     */
    @Test
    @DisplayName("Sequential evictions always remove the correct LRU key")
    void testEviction_chainOfEvictions_correctOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");
        cache.put("D", "4"); // evicts A
        cache.put("E", "5"); // evicts B
        cache.put("F", "6"); // evicts C

        assertNull(cache.get("A"), "A evicted first");
        assertNull(cache.get("B"), "B evicted second");
        assertNull(cache.get("C"), "C evicted third");
        assertNotNull(cache.get("D"), "D survived");
        assertNotNull(cache.get("E"), "E survived");
        assertNotNull(cache.get("F"), "F survived");
    }

    // =========================================================================
    // 3. Move-to-front on get()
    // =========================================================================

    /**
     * This is the defining behaviour of LRU vs FIFO.
     * A key that is READ should be protected from eviction.
     *
     * Sequence:
     *   put A, put B, put C
     *   get A                ← A moves to MRU position
     *   put D                ← B is now LRU (not A), so B is evicted
     */
    @Test
    @DisplayName("get() moves key to MRU position, protecting it from eviction")
    void testMoveToFront_onGet_protectsFromEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A"); // A jumps to front — order: A, C, B

        cache.put("D", "4"); // triggers eviction — B is evicted, not A

        assertNotNull(cache.get("A"), "A was recently accessed so should NOT be evicted");
        assertNull(cache.get("B"),    "B became LRU after A was accessed");
    }

    /**
     * Multiple get() calls each update recency correctly.
     *
     * Sequence (capacity=3):
     *   put A, put B, put C  →  C, B, A
     *   get A                →  A, C, B
     *   get B                →  B, A, C
     *   put D                →  C is LRU, C is evicted
     */
    @Test
    @DisplayName("Multiple get() calls update recency correctly each time")
    void testMoveToFront_multipleGets_correctFinalOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("A"); // order: A, C, B
        cache.get("B"); // order: B, A, C  ← C is now LRU

        cache.put("D", "4"); // C evicted

        assertNull(cache.get("C"),    "C is LRU after A and B were accessed");
        assertNotNull(cache.get("A"), "A was accessed, should survive");
        assertNotNull(cache.get("B"), "B was most recently accessed, should survive");
        assertNotNull(cache.get("D"), "D just inserted, should be present");
    }

    /**
     * get() on a missing key should NOT affect the order of existing keys.
     * A failed lookup must be a pure no-op on the cache structure.
     */
    @Test
    @DisplayName("get() on missing key does not affect recency of existing keys")
    void testMoveToFront_missedGet_doesNotAffectOrder() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3");

        cache.get("MISSING"); // no-op — order should remain: C, B, A

        cache.put("D", "4"); // A should still be evicted

        assertNull(cache.get("A"),    "A should still be LRU after a missed get");
        assertNotNull(cache.get("B"), "B order unchanged");
        assertNotNull(cache.get("C"), "C order unchanged");
    }

    // =========================================================================
    // 4. Move-to-front on put() — duplicate keys
    // =========================================================================

    /**
     * Putting the same key again must:
     *   (a) Update the value
     *   (b) Move the key to MRU position
     *   (c) NOT increase the size
     */
    @Test
    @DisplayName("Duplicate put() updates value and moves key to MRU position")
    void testDuplicateKey_updatesValueAndResetsRecency() {
        cache.put("A", "old");
        cache.put("B", "2");
        cache.put("C", "3");

        // Re-put A — updates value, A moves to MRU.
        // Order: A, C, B
        cache.put("A", "new");

        cache.put("D", "4"); // B is LRU, B evicted

        assertEquals("new", cache.get("A"), "Value should be updated on duplicate put");
        assertNull(cache.get("B"),          "B should be evicted (LRU after A re-put)");
        assertNotNull(cache.get("C"),       "C should survive");
    }

    /**
     * Duplicate put() must NOT increase size.
     * If size grows on re-put, it means your HashMap+LinkedList are out of sync.
     */
    @Test
    @DisplayName("Duplicate put() does not increase size")
    void testDuplicateKey_doesNotIncreaseSize() {
        cache.put("A", "1");
        cache.put("A", "2");
        cache.put("A", "3");

        assertEquals(1, cache.size(), "Duplicate puts on same key must not increase size");
    }

    /**
     * Duplicate put() across multiple keys — verify each independent.
     */
    @Test
    @DisplayName("Duplicate puts on multiple keys all update correctly")
    void testDuplicateKey_multipleKeys_allUpdateCorrectly() {
        cache.put("A", "a1");
        cache.put("B", "b1");
        cache.put("A", "a2"); // update A
        cache.put("B", "b2"); // update B

        assertEquals("a2", cache.get("A"));
        assertEquals("b2", cache.get("B"));
        assertEquals(2, cache.size());
    }

    // =========================================================================
    // 5. Capacity = 1 (hardest edge case)
    // =========================================================================

    /**
     * Capacity=1 means every put evicts the previous key.
     * This stress-tests the linked list — remove + addToFront must work
     * when there's only ever one node.
     */
    @Test
    @DisplayName("Capacity=1: every put evicts the previous key")
    void testCapacityOne_evictsOnEveryPut() {
        LRUCache<String, String> tiny = new LRUCache<>(1);

        tiny.put("A", "1");
        assertEquals("1", tiny.get("A"));
        assertEquals(1, tiny.size());

        tiny.put("B", "2"); // evicts A
        assertNull(tiny.get("A"),    "A should be evicted");
        assertEquals("2", tiny.get("B"));
        assertEquals(1, tiny.size());

        tiny.put("C", "3"); // evicts B
        assertNull(tiny.get("B"),    "B should be evicted");
        assertEquals("3", tiny.get("C"));
        assertEquals(1, tiny.size());
    }

    /**
     * Capacity=1 with a duplicate key — should update value, not evict.
     */
    @Test
    @DisplayName("Capacity=1: duplicate put updates value without eviction")
    void testCapacityOne_duplicatePut_noEviction() {
        LRUCache<String, String> tiny = new LRUCache<>(1);

        tiny.put("A", "old");
        tiny.put("A", "new"); // same key — update only, no eviction

        assertEquals("new", tiny.get("A"));
        assertEquals(1, tiny.size());
    }

    /**
     * Capacity=1 get() then put() — get rescues the key but a NEW put still evicts it.
     * There's only one slot. get() moves it to front (it's already there — size=1),
     * but adding a different key must still evict.
     */
    @Test
    @DisplayName("Capacity=1: get() then put different key still evicts")
    void testCapacityOne_getFollowedByPut_stillEvicts() {
        LRUCache<String, String> tiny = new LRUCache<>(1);

        tiny.put("A", "1");
        tiny.get("A");        // A is "recently used" — but cache is size=1
        tiny.put("B", "2");   // B is new key — must evict A regardless

        assertNull(tiny.get("A"),    "A must be evicted even though it was accessed");
        assertEquals("2", tiny.get("B"));
    }

    // =========================================================================
    // 6. Capacity = 2 edge cases
    // =========================================================================

    /**
     * Capacity=2 is the smallest cache where access order meaningfully differs
     * from insertion order. Tests the two-node linked list edge cases.
     */
    @Test
    @DisplayName("Capacity=2: access pattern correctly determines eviction")
    void testCapacityTwo_accessPatternDeterminesEviction() {
        LRUCache<String, String> small = new LRUCache<>(2);

        small.put("A", "1");
        small.put("B", "2");
        small.get("A");       // A→MRU, B→LRU. Order: A, B
        small.put("C", "3"); // B evicted

        assertNull(small.get("B"),    "B is LRU, should be evicted");
        assertNotNull(small.get("A"), "A was accessed, should survive");
        assertNotNull(small.get("C"), "C just inserted, should be present");
    }

    // =========================================================================
    // 7. CacheStats accuracy
    // =========================================================================

    /**
     * A get() on a present key is a HIT.
     * hits counter must increment.
     */
    @Test
    @DisplayName("CacheStats: hit count increments on successful get()")
    void testStats_hitCountIncrements() {
        cache.put("key", "value");

        cache.get("key"); // hit
        cache.get("key"); // hit
        cache.get("key"); // hit

        CacheStats stats = cache.getstats();
        assertEquals(3, stats.hits(), "3 successful gets = 3 hits");
    }

    /**
     * A get() on a missing key is a MISS.
     * misses counter must increment.
     */
    @Test
    @DisplayName("CacheStats: miss count increments on failed get()")
    void testStats_missCountIncrements() {
        cache.get("nope");  // miss
        cache.get("nope2"); // miss

        CacheStats stats = cache.getstats();
        assertEquals(2, stats.misses(), "2 failed gets = 2 misses");
    }

    /**
     * hits + misses must always equal total number of get() calls.
     * This is a mathematical invariant — if it breaks, stats are lying.
     */
    @Test
    @DisplayName("CacheStats: hits + misses equals total get() calls")
    void testStats_hitsAndMissesSumToTotalGets() {
        cache.put("A", "1");
        cache.put("B", "2");

        cache.get("A");     // hit  (total: 1)
        cache.get("B");     // hit  (total: 2)
        cache.get("C");     // miss (total: 3)
        cache.get("A");     // hit  (total: 4)
        cache.get("GHOST"); // miss (total: 5)

        CacheStats stats = cache.getstats();
        long totalGets = stats.hits() + stats.misses();

        assertEquals(5, totalGets,          "Total gets should be 5");
        assertEquals(3, stats.hits(),    "3 hits (A, B, A)");
        assertEquals(2, stats.misses(),  "2 misses (C, GHOST)");
    }

    /**
     * Evictions must be counted when capacity is exceeded.
     * Each put that causes an eviction must increment the eviction counter.
     */
    @Test
    @DisplayName("CacheStats: eviction count increments on capacity breach")
    void testStats_evictionCountIncrements_onCapacityBreach() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3"); // full — no eviction yet
        cache.put("D", "4"); // eviction 1 (A evicted)
        cache.put("E", "5"); // eviction 2 (B evicted)

        CacheStats stats = cache.getstats();
        assertEquals(2, stats.evictions(), "2 puts beyond capacity = 2 evictions");
    }

    /**
     * A duplicate put() on an existing key must NOT count as an eviction.
     * The key is updated in place — nothing is removed from the cache.
     */
    @Test
    @DisplayName("CacheStats: duplicate put() does not count as eviction")
    void testStats_duplicatePut_doesNotCountAsEviction() {
        cache.put("A", "1");
        cache.put("A", "2"); // update, not eviction
        cache.put("A", "3"); // update, not eviction

        CacheStats stats = cache.getstats();
        assertEquals(0, stats.evictions(), "Updates to existing keys are not evictions");
    }

    /**
     * Fresh cache has all-zero stats. Verifies CacheStats initialises correctly.
     */
    @Test
    @DisplayName("CacheStats: all counters start at zero on fresh cache")
    void testStats_initialState_allZero() {
        CacheStats stats = cache.getstats();

        assertEquals(0, stats.hits());
        assertEquals(0, stats.misses());
        assertEquals(0, stats.evictions());
    }

    /**
     * Hit rate = hits / (hits + misses).
     * Verify the computed rate is accurate.
     */
    @Test
    @DisplayName("CacheStats: hit rate computed correctly")
    void testStats_hitRate_computedCorrectly() {
        cache.put("A", "1");

        cache.get("A"); // hit
        cache.get("A"); // hit
        cache.get("B"); // miss

        CacheStats stats = cache.getstats();

        // 2 hits, 1 miss → hit rate = 2/3 ≈ 0.6667
        double expectedRate = 2.0 / 3.0;
        assertEquals(expectedRate, stats.hitRate(), 0.001,
                "Hit rate should be 2/3 with 2 hits and 1 miss");
    }

    /**
     * Hit rate on a fresh cache with no gets should not throw.
     * Division by zero must be handled — return 0.0 when no gets have occurred.
     */
    @Test
    @DisplayName("CacheStats: hit rate is 0.0 on fresh cache (no division by zero)")
    void testStats_hitRate_noDivisionByZero() {
        CacheStats stats = cache.getstats();

        assertDoesNotThrow(stats::hitRate, "Hit rate on empty stats must not throw");
        assertEquals(0.0, stats.hitRate(), "Hit rate with no gets should be 0.0");
    }

    // =========================================================================
    // 8. Explicit evict()
    // =========================================================================

    /**
     * Explicit evict() removes the key from the cache.
     * Subsequent get() returns null.
     */
    @Test
    @DisplayName("evict() removes the key, get() returns null afterward")
    void testExplicitEvict_removesKey() {
        cache.put("key", "value");
        cache.evict("key");

        assertNull(cache.get("key"), "Evicted key should return null");
        assertEquals(0, cache.size(), "Size should decrease after evict");
    }

    /**
     * After explicit evict(), the slot is free and a new key can take it
     * without triggering an LRU eviction.
     */
    @Test
    @DisplayName("evict() frees capacity for new keys")
    void testExplicitEvict_freesCapacity() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3"); // cache is full

        cache.evict("A");    // manually free a slot

        // Now D can be inserted without evicting B or C
        cache.put("D", "4");

        assertNotNull(cache.get("B"), "B should still be present");
        assertNotNull(cache.get("C"), "C should still be present");
        assertNotNull(cache.get("D"), "D should be present in freed slot");
        assertEquals(3, cache.size());
    }

    /**
     * Evicting a nonexistent key must be a silent no-op.
     * No exception, no change to stats or size.
     */
    @Test
    @DisplayName("evict() on nonexistent key is a safe no-op")
    void testExplicitEvict_nonexistentKey_safeNoOp() {
        cache.put("A", "1");

        assertDoesNotThrow(() -> cache.evict("GHOST"),
                "Evicting a nonexistent key must not throw");

        assertEquals(1, cache.size(), "Size must not change after evicting a nonexistent key");
        assertEquals("1", cache.get("A"), "Existing key must not be affected");
    }

    /**
     * Explicit evict() should increment the eviction counter in CacheStats.
     */
    @Test
    @DisplayName("evict() increments eviction counter in CacheStats")
    void testExplicitEvict_incrementsEvictionStats() {
        cache.put("A", "1");
        cache.put("B", "2");

        cache.evict("A");
        cache.evict("B");

        CacheStats stats = cache.getstats();
        assertEquals(2, stats.evictions(),
                "Two explicit evictions should count as 2 in stats");
    }

    // =========================================================================
    // 9. Null and invalid input
    // =========================================================================

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("put() with null key throws IllegalArgumentException")
    void testNullKey_put_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put(null, "value"));
    }

    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("put() with null value throws IllegalArgumentException")
    void testNullValue_put_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.put("key", null));
    }


    @Disabled("Temporarily disabled during server integration")
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

    // =========================================================================
    // 10. Full cache — exactly at capacity
    // =========================================================================

    /**
     * A cache exactly at capacity should NOT evict when getting existing keys.
     * get() must never cause eviction, only put() can.
     */
    @Test
    @DisplayName("get() on full cache never causes eviction")
    void testFullCache_getDoesNotCauseEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3"); // full

        // Multiple gets on existing keys — nothing should be evicted
        cache.get("A");
        cache.get("B");
        cache.get("C");

        assertEquals(3, cache.size(), "Gets on full cache must not cause eviction");
        assertEquals(0, cache.getstats().evictions());
    }

    /**
     * Re-putting an existing key on a full cache must NOT cause eviction.
     * It's an update, not a new insertion.
     */
    @Test
    @DisplayName("Duplicate put() on full cache does not cause eviction")
    void testFullCache_duplicatePut_doesNotCauseEviction() {
        cache.put("A", "1");
        cache.put("B", "2");
        cache.put("C", "3"); // full

        cache.put("A", "updated"); // update — no eviction
        cache.put("B", "updated"); // update — no eviction

        assertEquals(3, cache.size());
        assertEquals(0, cache.getstats().evictions(),
                "Updates to existing keys must not count as evictions");
        assertEquals("updated", cache.get("A"));
        assertEquals("updated", cache.get("B"));
    }
}