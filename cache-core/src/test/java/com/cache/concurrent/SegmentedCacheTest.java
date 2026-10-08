package com.cache.concurrent;

import com.cache.api.CacheStats;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SegmentedCache Tests")
class SegmentedCacheTest {
    private static final int TOTAL_CAPACITY  = 5000;
    private static final int NUM_SEGMENTS    = 16;
    private static final int TIMEOUT_SECONDS = 10;

    private SegmentedCache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new SegmentedCache<>(TOTAL_CAPACITY, NUM_SEGMENTS);
    }

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
    @DisplayName("evict() removes key: get() returns null")
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

        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), cache.get(entry.getKey()),
                    "Wrong value for key: " + entry.getKey());
        }
    }

    @Test
    @DisplayName("Same key always routes to the same segment")
    void testRouting_sameKeyAlwaysSameSegment() {
        String key = "routing-test-key";

        int firstIndex = cache.getSegmentIndexFor(key);

        for (int i = 0; i < 1000; i++) {
            assertEquals(firstIndex, cache.getSegmentIndexFor(key),
                    "Segment index changed on call " + i + " for key: " + key);
        }
    }

    @Test
    @DisplayName("Key written and read always hits the same segment")
    void testRouting_putAndGetUseSameSegment() {
        List<String> keys = Arrays.asList("alpha", "beta", "gamma", "delta",
                "epsilon", "zeta", "eta", "theta");

        for (String key : keys) {
            cache.put(key, "value-of-" + key);
        }

        for (String key : keys) {
            assertEquals("value-of-" + key, cache.get(key),
                    "Routing inconsistency for key: " + key);
        }
    }

    @Test
    @DisplayName("getSegmentIndexFor() is pure: same key, same result always")
    void testRouting_segmentIndexIsPure() {
        for (int i = 0; i < 100; i++) {
            String key = "stability-key-" + i;
            int idx1 = cache.getSegmentIndexFor(key);
            int idx2 = cache.getSegmentIndexFor(key);
            int idx3 = cache.getSegmentIndexFor(key);
            assertEquals(idx1, idx2, "Segment index unstable for key: " + key);
            assertEquals(idx2, idx3, "Segment index unstable for key: " + key);
        }
    }

    @Test
    @DisplayName("Keys distribute reasonably evenly across all segments")
    void testDistribution_reasonablyEven() {
        int totalKeys      = 1600;
        int[] segmentLoads = new int[NUM_SEGMENTS];

        for (int i = 0; i < totalKeys; i++) {
            String key = "dist-key-" + i;
            int idx = cache.getSegmentIndexFor(key);
            segmentLoads[idx]++;
        }

        for (int i = 0; i < NUM_SEGMENTS; i++) {
            assertTrue(segmentLoads[i] > 0,
                    "Segment " + i + " received zero keys: severe distribution problem");
        }

        double average  = (double) totalKeys / NUM_SEGMENTS;
        int    maxLoad  = Arrays.stream(segmentLoads).max().getAsInt();
        assertTrue(maxLoad <= average * 3,
                "Worst segment has " + maxLoad + " keys vs average " + average
                        + ": distribution too skewed");
    }

    @Test
    @DisplayName("All segment indices are within valid range [0, numSegments)")
    void testDistribution_allIndicesInRange() {
        for (int i = 0; i < 1000; i++) {
            int idx = cache.getSegmentIndexFor("key-" + i);
            assertTrue(idx >= 0 && idx < NUM_SEGMENTS,
                    "Segment index out of range: " + idx + " for key-" + i);
        }
    }

    @Test
    @DisplayName("Global size() equals sum of all segment sizes")
    void testSize_equalsSegmentSizeSum() {
        for (int i = 0; i < 80; i++) {
            cache.put("sz-key-" + i, "v");
        }

        int globalSize = cache.size();

        int segmentSum = 0;
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            segmentSum += cache.getSegmentSize(i);
        }

        assertEquals(globalSize, segmentSum,
                "Global size() (" + globalSize + ") != segment sum (" + segmentSum + ")");
    }

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

        int evicted = 0;
        for (int i = 0; i < keys.size(); i += 2) {
            cache.evict(keys.get(i));
            evicted++;
        }

        assertEquals(60 - evicted, cache.size(),
                "size() should be " + (60 - evicted) + " after " + evicted + " evictions");
    }

    @Test
    @DisplayName("Evicting a key in one segment does not affect keys in other segments")
    void testIsolation_evictInOneSegment_doesNotAffectOthers() {
        for (int i = 0; i < 50; i++) {
            cache.put("iso-" + i, "val-" + i);
        }

        cache.evict("iso-0");

        for (int i = 1; i < 50; i++) {
            assertEquals("val-" + i, cache.get("iso-" + i),
                    "Key iso-" + i + " was incorrectly affected by eviction of iso-0");
        }
    }

    @Test
    @DisplayName("Per-segment capacity eviction only evicts within that segment")
    void testIsolation_capacityEvictionStaysWithinSegment() {
        SegmentedCache<String, String> smallCache = new SegmentedCache<>(16, 16);

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

        smallCache.put(keyA, "valueA");
        smallCache.put(keyB, "valueB");

        assertEquals("valueA", smallCache.get(keyA), "keyA should be in its own segment");
        assertEquals("valueB", smallCache.get(keyB), "keyB should be in its own segment");
    }

    @Test
    @DisplayName("Constructor rejects non-power-of-2 segment count")
    void testConfig_nonPowerOfTwo_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentedCache<>(100, 3),
                "3 is not a power of 2: should throw");

        assertThrows(IllegalArgumentException.class,
                () -> new SegmentedCache<>(100, 15),
                "15 is not a power of 2: should throw");
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
                () -> cache.getSegmentSize(NUM_SEGMENTS));
    }

    @Test
    @DisplayName("Keys written by one thread are readable by all other threads")
    void testConcurrency_crossThreadVisibility() throws InterruptedException {
        int writerThreads = 4;
        int readerThreads = 8;
        int keysPerWriter = 50;

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

        writesDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        writers.shutdown();

        CountDownLatch allDone    = new CountDownLatch(readerThreads);
        ExecutorService readers   = Executors.newFixedThreadPool(readerThreads);
        AtomicInteger  mismatches = new AtomicInteger(0);
        for (int r = 0; r < readerThreads; r++) {
            readers.submit(() -> {
                try {
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

    @Test
    @DisplayName("Mixed concurrent gets, puts, evicts across all segments: no invariant violations")
    void testConcurrency_mixedOps_noViolations() throws InterruptedException {
        int totalThreads = 32;

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
                    Random rng = new Random(threadId);
                    for (int i = 0; i < 300; i++) {
                        int op = rng.nextInt(10);
                        String key = "m" + (rng.nextInt(200));

                        if (op < 5) {
                            cache.get(key);
                        } else if (op < 9) {
                            cache.put(key, "t" + threadId);
                        } else {
                            cache.evict(key);
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

        assertTrue(finished, "Mixed concurrent test timed out: possible deadlock");
        assertEquals(0, errors.get(),
                "Invariant violations or exceptions: " + errors.get());
    }

    @Test
    @DisplayName("getStats() aggregates hits and misses across all segments")
    void testStats_aggregatesAcrossSegments() {
        for (int i = 0; i < 50; i++) {
            cache.put("stat-" + i, "v");
        }

        for (int i = 0; i < 50; i++) {
            cache.get("stat-" + i);
            cache.get("missing-" + i);
        }

        CacheStats stats = cache.getStats();
        assertNotNull(stats);

        long totalActivity = stats.hits() + stats.misses();
        assertTrue(totalActivity > 0,
                "Aggregated stats should show activity after gets, got: hits="
                        + stats.hits() + " misses=" + stats.misses());
    }

    @Test
    @DisplayName("Total size never exceeds the requested capacity")
    void testCapacityIsExact() {
        SegmentedCache<String, String> segmented = new SegmentedCache<>(100, 16);
        for (int i = 0; i < 10_000; i++) {
            segmented.put("key-" + i, "v");
        }
        assertEquals(100, segmented.size());
    }

    @Test
    @DisplayName("Capacity smaller than the segment count uses fewer segments")
    void testSmallCapacity_usesFewerSegments() {
        SegmentedCache<String, String> segmented = new SegmentedCache<>(5, 16);
        assertEquals(4, segmented.getNumSegments());
        for (int i = 0; i < 100; i++) {
            segmented.put("key-" + i, "v");
        }
        assertTrue(segmented.size() <= 5);
    }
}
