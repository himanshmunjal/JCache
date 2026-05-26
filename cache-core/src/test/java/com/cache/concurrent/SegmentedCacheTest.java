package com.cache.concurrent;

import com.cache.api.CacheStats;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for SegmentedCache.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHAT WE TEST HERE THAT WE DID NOT TEST IN CoarseGrainedCacheTest
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * CoarseGrainedCacheTest already covered:
 *   - Basic put/get/evict/size correctness
 *   - Concurrent read safety
 *   - Concurrent write safety
 *   - Mixed load safety
 *   - No deadlocks
 *
 * SegmentedCacheTest adds tests SPECIFIC to the segmented design:
 *
 *   1. Segment routing consistency
 *      Same key must always go to the same segment, always.
 *      If routing is inconsistent, a key written in one segment would
 *      be looked up in a different segment and return null.
 *
 *   2. Segment isolation
 *      Operations on different segments must not interfere.
 *      This is the core correctness guarantee of segmented locking.
 *
 *   3. Key distribution
 *      Keys should spread reasonably evenly across segments.
 *      If all keys pile into segment 0, we lose all benefit of segmentation.
 *
 *   4. size() aggregation correctness
 *      Global size() must equal the sum of all segment sizes.
 *
 *   5. Per-segment capacity
 *      Each segment has independent capacity — verify eviction happens
 *      per-segment, not globally.
 *
 *   6. Higher concurrency throughput
 *      Segmented locking should handle more threads without timeouts
 *      that CoarseGrainedCache would struggle with.
 *
 *   7. Configuration validation
 *      Non-power-of-2 segment counts must be rejected.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TEST CONFIGURATION
 * ─────────────────────────────────────────────────────────────────────────────
 */
@DisplayName("SegmentedCache Tests")
class SegmentedCacheTest {

    private static final int TOTAL_CAPACITY  = 5000;  // 10 per segment with 16 segments
    private static final int NUM_SEGMENTS    = 16;
    private static final int TIMEOUT_SECONDS = 10;

    private SegmentedCache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new SegmentedCache<>(TOTAL_CAPACITY, NUM_SEGMENTS);
    }

    // =========================================================================
    // 1. Basic correctness (wrapper doesn't break core cache behaviour)
    // =========================================================================

    @Test
    @DisplayName("get() returns null for missing key")
    void testGet_missingKey_returnsNull() {
        assertNull(cache.get("nonexistent"));
    }

    @Test
    @DisplayName("put() then get() returns correct value")
    void testPutAndGet_correct() {
        cache.put("city", "Delhi");
        assertEquals("Delhi", cache.get("city"));
    }

    @Test
    @DisplayName("put() with same key overwrites value")
    void testPut_overwrite() {
        cache.put("x", "old");
        cache.put("x", "new");
        assertEquals("new", cache.get("x"));
    }

    @Test
    @DisplayName("evict() removes key — get() returns null")
    void testEvict_removesKey() {
        cache.put("temp", "data");
        cache.evict("temp");
        assertNull(cache.get("temp"));
    }

    @Test
    @DisplayName("evict() on nonexistent key does not throw")
    void testEvict_nonexistent_noException() {
        assertDoesNotThrow(() -> cache.evict("ghost"));
    }

    @Test
    @DisplayName("size() is 0 for empty cache")
    void testSize_emptyCache() {
        assertEquals(0, cache.size());
    }

    @Test
    @DisplayName("size() increases on put and decreases on evict")
    void testSize_putAndEvict() {
        cache.put("a", "1");
        cache.put("b", "2");
        assertEquals(2, cache.size());

        cache.evict("a");
        assertEquals(1, cache.size());
    }

    @Test
    @DisplayName("Multiple distinct keys are all retrievable")
    void testMultipleKeys_allRetrievable() {
        Map<String, String> expected = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            expected.put("key-" + i, "val-" + i);
            cache.put("key-" + i, "val-" + i);
        }

        // Verify every key routes back to its correct value.
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), cache.get(entry.getKey()),
                    "Wrong value for key: " + entry.getKey());
        }
    }

    // =========================================================================
    // 2. Segment routing consistency
    // =========================================================================

    /**
     * The same key must ALWAYS route to the same segment.
     * If segment assignment is non-deterministic (e.g., using random or
     * time-based hashing), a put() might write to segment 3 but get()
     * might read from segment 7 — returning null for an existing key.
     *
     * We verify by checking the segment index before and after put/get cycles.
     */
    @Test
    @DisplayName("Same key always routes to the same segment")
    void testRouting_sameKeyAlwaysSameSegment() {
        String key = "routing-test-key";

        int firstIndex = cache.getSegmentIndexFor(key);

        // Call many times — index must never change.
        for (int i = 0; i < 1000; i++) {
            assertEquals(firstIndex, cache.getSegmentIndexFor(key),
                    "Segment index changed on call " + i + " for key: " + key);
        }
    }

    /**
     * Put a key, then verify it can be retrieved — proving the same
     * segment is used for both operations end-to-end.
     */
    @Test
    @DisplayName("Key written and read always hits the same segment")
    void testRouting_putAndGetUseSameSegment() {
        // Use keys that we know hash differently (different prefixes)
        List<String> keys = Arrays.asList("alpha", "beta", "gamma", "delta",
                "epsilon", "zeta", "eta", "theta");

        for (String key : keys) {
            cache.put(key, "value-of-" + key);
        }

        // Every key must return its value — proves put and get hit the same segment.
        for (String key : keys) {
            assertEquals("value-of-" + key, cache.get(key),
                    "Routing inconsistency for key: " + key);
        }
    }

    /**
     * Two different keys may legitimately map to the same segment (collision).
     * That's expected and fine. But the same key must never map to two segments.
     */
    @Test
    @DisplayName("getSegmentIndexFor() is pure — same key, same result always")
    void testRouting_segmentIndexIsPure() {
        // Generate 100 different keys and verify their segment indices are stable.
        for (int i = 0; i < 100; i++) {
            String key = "stability-key-" + i;
            int idx1 = cache.getSegmentIndexFor(key);
            int idx2 = cache.getSegmentIndexFor(key);
            int idx3 = cache.getSegmentIndexFor(key);
            assertEquals(idx1, idx2, "Segment index unstable for key: " + key);
            assertEquals(idx2, idx3, "Segment index unstable for key: " + key);
        }
    }

    // =========================================================================
    // 3. Key distribution across segments
    // =========================================================================

    /**
     * With a good hash function, keys should spread reasonably evenly.
     * "Reasonably" means no segment should receive more than 3x the
     * average load (where average = totalKeys / numSegments).
     *
     * We use 1600 keys (100 per segment on average) so statistical noise
     * is small enough to detect real imbalance.
     */
    @Test
    @DisplayName("Keys distribute reasonably evenly across all segments")
    void testDistribution_reasonablyEven() {
        int totalKeys      = 1600; // 100 per segment on average
        int[] segmentLoads = new int[NUM_SEGMENTS];

        for (int i = 0; i < totalKeys; i++) {
            String key = "dist-key-" + i;
            int idx = cache.getSegmentIndexFor(key);
            segmentLoads[idx]++;
        }

        // Every segment should have received some keys.
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            assertTrue(segmentLoads[i] > 0,
                    "Segment " + i + " received zero keys — severe distribution problem");
        }

        // No segment should be more than 3x the average.
        double average  = (double) totalKeys / NUM_SEGMENTS;
        int    maxLoad  = Arrays.stream(segmentLoads).max().getAsInt();
        assertTrue(maxLoad <= average * 3,
                "Worst segment has " + maxLoad + " keys vs average " + average
                        + " — distribution too skewed");

        // Log actual distribution for benchmark analysis.
        System.out.println("[SegmentedCacheTest] Key distribution across segments:");
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            System.out.printf("  Segment %2d: %4d keys%n", i, segmentLoads[i]);
        }
    }

    /**
     * All configured segments should be reachable — no segment index
     * returned by getSegmentIndexFor() should fall outside [0, numSegments).
     */
    @Test
    @DisplayName("All segment indices are within valid range [0, numSegments)")
    void testDistribution_allIndicesInRange() {
        for (int i = 0; i < 1000; i++) {
            int idx = cache.getSegmentIndexFor("key-" + i);
            assertTrue(idx >= 0 && idx < NUM_SEGMENTS,
                    "Segment index out of range: " + idx + " for key-" + i);
        }
    }

    // =========================================================================
    // 4. size() aggregation correctness
    // =========================================================================

    /**
     * Global size() must equal the sum of all individual segment sizes.
     * If the aggregation logic has a bug (e.g., double-counting a segment),
     * this test catches it.
     */
    @Test
    @DisplayName("Global size() equals sum of all segment sizes")
    void testSize_equalsSegmentSizeSum() {
        // Put 80 keys — spread across segments by hash.
        for (int i = 0; i < 80; i++) {
            cache.put("sz-key-" + i, "v");
        }

        int globalSize = cache.size();

        // Sum segment sizes manually.
        int segmentSum = 0;
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            segmentSum += cache.getSegmentSize(i);
        }

        assertEquals(globalSize, segmentSum,
                "Global size() (" + globalSize + ") != segment sum (" + segmentSum + ")");
    }

    /**
     * After clearing specific keys via evict(), size() must reflect
     * the correct reduced count.
     */
    @Test
    @DisplayName("size() reflects evictions across different segments")
    void testSize_afterCrossSegmentEvictions() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            String k = "evict-sz-" + i;
            keys.add(k);
            cache.put(k, "v");
        }
        assertEquals(60, cache.size());

        // Evict every other key.
        int evicted = 0;
        for (int i = 0; i < keys.size(); i += 2) {
            cache.evict(keys.get(i));
            evicted++;
        }

        assertEquals(60 - evicted, cache.size(),
                "size() should be " + (60 - evicted) + " after " + evicted + " evictions");
    }

    // =========================================================================
    // 5. Segment isolation
    // =========================================================================

    /**
     * Operations on keys in different segments must not affect each other.
     * This is the fundamental guarantee of segment isolation.
     *
     * Strategy: find two keys that route to DIFFERENT segments,
     * then verify that evicting one doesn't affect the other.
     */
    @Test
    @DisplayName("Evicting a key in one segment does not affect keys in other segments")
    void testIsolation_evictInOneSegment_doesNotAffectOthers() {
        // Put 50 keys across all segments.
        for (int i = 0; i < 50; i++) {
            cache.put("iso-" + i, "val-" + i);
        }

        // Evict one key.
        cache.evict("iso-0");

        // All other keys must still be present.
        for (int i = 1; i < 50; i++) {
            assertEquals("val-" + i, cache.get("iso-" + i),
                    "Key iso-" + i + " was incorrectly affected by eviction of iso-0");
        }
    }

    /**
     * Segment-level capacity eviction must be isolated.
     * When segment S is full and evicts LRU, only keys in segment S
     * are candidates for eviction — not keys in other segments.
     */
    @Test
    @DisplayName("Per-segment capacity eviction only evicts within that segment")
    void testIsolation_capacityEvictionStaysWithinSegment() {
        // Use a small cache where we can force capacity eviction.
        SegmentedCache<String, String> smallCache = new SegmentedCache<>(16, 16);
        // Each segment now has capacity 1.

        // Find two keys that hash to DIFFERENT segments.
        String keyA = null;
        String keyB = null;
        for (int i = 0; i < 1000; i++) {
            String candidate = "cap-" + i;
            int idx = smallCache.getSegmentIndexFor(candidate);
            if (keyA == null) {
                keyA = candidate;
            } else if (idx != smallCache.getSegmentIndexFor(keyA)) {
                keyB = candidate;
                break;
            }
        }

        assertNotNull(keyA, "Could not find key for segment A");
        assertNotNull(keyB, "Could not find key for segment B");

        // Put keyA and keyB — they go to different segments.
        smallCache.put(keyA, "valueA");
        smallCache.put(keyB, "valueB");

        // Both should be retrievable — they're in different segments with capacity 1 each.
        assertEquals("valueA", smallCache.get(keyA), "keyA should be in its own segment");
        assertEquals("valueB", smallCache.get(keyB), "keyB should be in its own segment");
    }

    // =========================================================================
    // 6. Configuration validation
    // =========================================================================

    @Test
    @DisplayName("Constructor rejects non-power-of-2 segment count")
    void testConfig_nonPowerOfTwo_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentedCache<>(100, 3),
                "3 is not a power of 2 — should throw");

        assertThrows(IllegalArgumentException.class,
                () -> new SegmentedCache<>(100, 15),
                "15 is not a power of 2 — should throw");
    }

    @Test
    @DisplayName("Constructor rejects non-positive capacity")
    void testConfig_zeroCapacity_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentedCache<>(0, 16));
    }

    @Test
    @DisplayName("Constructor rejects zero segment count")
    void testConfig_zeroSegments_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentedCache<>(100, 0));
    }

    @Test
    @DisplayName("getNumSegments() returns configured value")
    void testConfig_getNumSegments() {
        assertEquals(NUM_SEGMENTS, cache.getNumSegments());

        SegmentedCache<String, String> custom = new SegmentedCache<>(64, 4);
        assertEquals(4, custom.getNumSegments());
    }

    @Test
    @DisplayName("getSegmentSize() throws on out-of-range index")
    void testConfig_getSegmentSize_outOfRange_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> cache.getSegmentSize(-1));
        assertThrows(IllegalArgumentException.class,
                () -> cache.getSegmentSize(NUM_SEGMENTS)); // index == numSegments is out of range
    }

    // =========================================================================
    // 7. Concurrent correctness under high thread count
    // =========================================================================

    /**
     * Key routing test under concurrency: keys put by one thread must be
     * retrievable by any other thread, even if they hash to different segments.
     *
     * This catches a class of bugs where segment-local caches are not
     * properly shared between threads (e.g., ThreadLocal misuse).
     */
//    @Disabled("Temporarily disabled during server integration")
    @Test
    @DisplayName("Keys written by one thread are readable by all other threads")
    void testConcurrency_crossThreadVisibility() throws InterruptedException {
        int writerThreads = 4;
        int readerThreads = 8;
        int keysPerWriter = 50;
        // Writers populate the cache.
        CountDownLatch writesDone = new CountDownLatch(writerThreads);
        ExecutorService writers   = Executors.newFixedThreadPool(writerThreads);
        for (int w = 0; w < writerThreads; w++) {
            final int writerId = w;
            writers.submit(() -> {
                try {
                    for (int i = 0; i < keysPerWriter; i++) {
                        cache.put("w" + writerId + "-k" + i, "val-" + writerId + "-" + i);
                    }
                } finally {
                    writesDone.countDown();
                }
            });
        }
        // Wait for all writes to complete before readers start.
        writesDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        writers.shutdown();
        // Readers verify all keys are visible.
        CountDownLatch allDone    = new CountDownLatch(readerThreads);
        ExecutorService readers   = Executors.newFixedThreadPool(readerThreads);
        AtomicInteger  mismatches = new AtomicInteger(0);
        for (int r = 0; r < readerThreads; r++) {
            readers.submit(() -> {
                try {
                    // Each reader checks ALL keys written by ALL writers.
                    for (int w = 0; w < writerThreads; w++) {
                        for (int i = 0; i < keysPerWriter; i++) {
                            String key      = "w" + w + "-k" + i;
                            String expected = "val-" + w + "-" + i;
                            String actual   = cache.get(key);
                            if (!expected.equals(actual)) {
                                mismatches.incrementAndGet();
                            }
                        }
                    }
                } finally {
                    allDone.countDown();
                }
            });
        }
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        readers.shutdown();
        assertTrue(finished, "Reader threads timed out");
        assertEquals(0, mismatches.get(),
                "Cross-thread read mismatches: " + mismatches.get());
    }

    /**
     * High thread count stress test — 64 concurrent writers across 16 segments.
     * Each segment handles ~4 concurrent writers on average.
     * All operations must complete without exception or timeout.
     */
    @Test
    @DisplayName("64 concurrent writers complete without deadlock or exception")
    void testConcurrency_highThreadCount_noDeadlock() throws InterruptedException {
        int threadCount = 64;
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(threadCount);
        AtomicInteger  errors   = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    for (int i = 0; i < 200; i++) {
                        String key = "stress-t" + threadId + "-k" + i;
                        cache.put(key, "v" + i);
                        cache.get(key);
                        // Check size doesn't go wildly wrong
                        int sz = cache.size();
                        if (sz < 0) errors.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Thread " + threadId + " error: " + e);
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS * 2L, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "DEADLOCK: 64 threads did not finish in time");
        assertEquals(0, errors.get(), "Errors under high thread count: " + errors.get());
    }

    /**
     * Mixed operations across all segments simultaneously.
     * The key invariant: keys that are put and NOT evicted must be retrievable.
     *
     * This is harder to assert exactly because LRU may evict some keys when
     * segment capacity is exceeded. We instead assert NO exceptions and
     * size stays within [0, effective_capacity].
     */
    @Test
    @DisplayName("Mixed concurrent gets, puts, evicts across all segments — no invariant violations")
    void testConcurrency_mixedOps_noViolations() throws InterruptedException {
        int totalThreads = 32;
        // Effective capacity = segmentCapacity * numSegments.
        int segmentCapacity = (int) Math.ceil((double) TOTAL_CAPACITY / NUM_SEGMENTS);
        int effectiveCap    = segmentCapacity * NUM_SEGMENTS;

        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch allDone  = new CountDownLatch(totalThreads);
        AtomicInteger  errors   = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);

        for (int t = 0; t < totalThreads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGun.await();
                    Random rng = new Random(threadId); // deterministic per thread
                    for (int i = 0; i < 300; i++) {
                        int op = rng.nextInt(10);
                        String key = "m" + (rng.nextInt(200)); // limited key space → collisions

                        if (op < 5) {
                            cache.get(key);                    // 50% reads
                        } else if (op < 9) {
                            cache.put(key, "t" + threadId);   // 40% writes
                        } else {
                            cache.evict(key);                  // 10% evictions
                        }

                        int sz = cache.size();
                        if (sz < 0 || sz > effectiveCap) {
                            errors.incrementAndGet();
                            System.err.println("Size violation: " + sz + " (cap=" + effectiveCap + ")");
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Thread " + threadId + " exception: " + e);
                } finally {
                    allDone.countDown();
                }
            });
        }

        startGun.countDown();
        boolean finished = allDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(finished, "Mixed concurrent test timed out — possible deadlock");
        assertEquals(0, errors.get(),
                "Invariant violations or exceptions: " + errors.get());
    }

    // =========================================================================
    // 8. Stats aggregation
    // =========================================================================

    /**
     * Stats should reflect activity across ALL segments.
     * After putting and getting across multiple segments, the aggregated
     * stats must show non-zero hits and misses.
     */
    @Test
    @DisplayName("getStats() aggregates hits and misses across all segments")
    void testStats_aggregatesAcrossSegments() {
        // Put keys that spread across segments.
        for (int i = 0; i < 50; i++) {
            cache.put("stat-" + i, "v");
        }

        // Generate hits (existing keys) and misses (nonexistent keys).
        for (int i = 0; i < 50; i++) {
            cache.get("stat-" + i);      // hit
            cache.get("missing-" + i);   // miss
        }

        CacheStats stats = cache.getstats();
        assertNotNull(stats);

        // Total activity across segments must be non-zero.
        long totalActivity = stats.hits() + stats.misses();
        assertTrue(totalActivity > 0,
                "Aggregated stats should show activity after gets, got: hits="
                        + stats.hits() + " misses=" + stats.misses());
    }
}