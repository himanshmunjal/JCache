package com.cache;

import com.cache.api.Cache;
import com.cache.api.CachePolicyType;
import com.cache.api.CacheStats;
import com.cache.concurrent.CoarseGrainedCache;
import com.cache.policy.ARCCache;
import com.cache.policy.LFUCache;
import com.cache.policy.LRUCache;
import com.cache.stats.CacheMetricsCollector;
import com.cache.ttl.TTLCache;
import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CacheFactory Tests")
class CacheFactoryTest {
    @Test
    @DisplayName("lru() returns a working LRU cache")
    void testLRU_returnsWorkingCache() {
        Cache<String, String> cache = CacheFactory.lru(10);

        cache.put("key", "value");
        assertEquals("value", cache.get("key"));
        assertEquals(1, cache.size());
    }

    @Test
    @DisplayName("lru() returns LRUCache instance")
    void testLRU_returnsCorrectType() {
        Cache<String, String> cache = CacheFactory.lru(10);

        assertInstanceOf(LRUCache.class, cache,
                "lru() should return an LRUCache");
    }

    @Test
    @DisplayName("lfu() returns a working LFU cache")
    void testLFU_returnsWorkingCache() {
        Cache<String, String> cache = CacheFactory.lfu(10);

        cache.put("k", "v");
        assertEquals("v", cache.get("k"));
    }

    @Test
    @DisplayName("lfu() returns LFUCache instance")
    void testLFU_returnsCorrectType() {
        Cache<String, String> cache = CacheFactory.lfu(10);

        assertInstanceOf(LFUCache.class, cache,
                "lfu() should return an LFUCache");
    }

    @Test
    @DisplayName("arc() returns a working ARC cache")
    void testARC_returnsWorkingCache() {
        Cache<String, String> cache = CacheFactory.arc(10);

        cache.put("k", "v");
        assertEquals("v", cache.get("k"));
    }

    @Test
    @DisplayName("arc() returns ARCCache instance")
    void testARC_returnsCorrectType() {
        Cache<String, String> cache = CacheFactory.arc(10);

        assertInstanceOf(ARCCache.class, cache,
                "arc() should return an ARCCache");
    }

    @Test
    @DisplayName("withPolicy(LRU) returns LRUCache")
    void testWithPolicy_LRU_returnsLRUCache() {
        Cache<String, String> cache = CacheFactory.withPolicy(CachePolicyType.LRU, 10);

        assertInstanceOf(LRUCache.class, cache);
    }

    @Test
    @DisplayName("withPolicy(LFU) returns LFUCache")
    void testWithPolicy_LFU_returnsLFUCache() {
        Cache<String, String> cache = CacheFactory.withPolicy(CachePolicyType.LFU, 10);

        assertInstanceOf(LFUCache.class, cache);
    }

    @Test
    @DisplayName("withPolicy(ARC) returns ARCCache")
    void testWithPolicy_ARC_returnsARCCache() {
        Cache<String, String> cache = CacheFactory.withPolicy(CachePolicyType.ARC, 10);

        assertInstanceOf(ARCCache.class, cache);
    }

    @Test
    @DisplayName("withPolicy() with all enum values returns working caches")
    void testWithPolicy_allValues_allWork() {
        for (CachePolicyType policy : CachePolicyType.values()) {
            Cache<String, String> cache = CacheFactory.withPolicy(policy, 5);

            assertNotNull(cache, "Factory must return non-null for policy: " + policy);
            cache.put("test", "value");
            assertEquals("value", cache.get("test"),
                    "Cache from withPolicy(" + policy + ") must be functional");
        }
    }

    @Test
    @DisplayName("withTTL() wraps delegate in TTLCache")
    void testWithTTL_returnsTTLCache() {
        Cache<String, String> base = CacheFactory.lru(10);
        Cache<String, String> withTTL = CacheFactory.withTTL(base, Duration.ofSeconds(60));

        assertInstanceOf(TTLCache.class, withTTL,
                "withTTL() should return a TTLCache");
    }

    @Test
    @DisplayName("withTTL() wrapped cache stores and retrieves correctly")
    void testWithTTL_delegatesGetPutCorrectly() {
        Cache<String, String> cache = CacheFactory.withTTL(
                CacheFactory.lru(10), Duration.ofSeconds(60)
        );

        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
    }

    @Test
    @DisplayName("withTTL() with Duration.ZERO creates cache with no default expiry")
    void testWithTTL_zeroDuration_noExpiry() {
        Cache<String, String> cache = CacheFactory.withTTL(
                CacheFactory.lru(10), Duration.ZERO
        );

        cache.put("permanent", "value");
        assertEquals("value", cache.get("permanent"),
                "Key should not expire when Duration.ZERO is used");
    }

    @Test
    @DisplayName("withTTL() with null delegate throws IllegalArgumentException")
    void testWithTTL_nullDelegate_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.withTTL(null, Duration.ofSeconds(60)),
                "Null delegate should throw IllegalArgumentException"
        );
    }

    @Test
    @DisplayName("withTTL() with null duration throws IllegalArgumentException")
    void testWithTTL_nullDuration_throwsException() {
        Cache<String, String> base = CacheFactory.lru(10);
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.withTTL(base, null),
                "Null duration should throw IllegalArgumentException"
        );
    }

    @Test
    @DisplayName("withTTL() with negative duration throws IllegalArgumentException")
    void testWithTTL_negativeDuration_throwsException() {
        Cache<String, String> base = CacheFactory.lru(10);
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.withTTL(base, Duration.ofSeconds(-1)),
                "Negative duration should throw IllegalArgumentException"
        );
    }

    @Test
    @DisplayName("withMetrics() wraps delegate in MetricsCollectingCache")
    void testWithMetrics_returnsMetricsCollectingCache() {
        Cache<String, String> base   = CacheFactory.lru(10);
        Cache<String, String> cached = CacheFactory.withMetrics(base, "test-cache");

        assertInstanceOf(CacheFactory.MetricsCollectingCache.class, cached,
                "withMetrics() should return a MetricsCollectingCache");
    }

    @Test
    @DisplayName("withMetrics() wrapped cache stores and retrieves correctly")
    void testWithMetrics_delegatesGetPutCorrectly() {
        Cache<String, String> cache = CacheFactory.withMetrics(
                CacheFactory.lru(10), "my-cache"
        );

        cache.put("a", "1");
        cache.put("b", "2");

        assertEquals("1", cache.get("a"));
        assertEquals("2", cache.get("b"));
    }

    @Test
    @DisplayName("withMetrics() records hits correctly")
    void testWithMetrics_recordsHits() {
        Cache<String, String> cache = CacheFactory.withMetrics(
                CacheFactory.lru(10), "hit-test"
        );

        cache.put("key", "value");
        cache.get("key");
        cache.get("key");
        cache.get("key");

        CacheStats stats = cache.getStats();
        assertEquals(3, stats.hits(), "3 successful gets should record 3 hits");
    }

    @Test
    @DisplayName("withMetrics() records misses correctly")
    void testWithMetrics_recordsMisses() {
        Cache<String, String> cache = CacheFactory.withMetrics(
                CacheFactory.lru(10), "miss-test"
        );

        cache.get("nope");
        cache.get("nope2");

        CacheStats stats = cache.getStats();
        assertEquals(2, stats.misses(), "2 failed gets should record 2 misses");
    }

    @Test
    @DisplayName("withMetrics() records manual evictions separately from evictions")
    void testWithMetrics_recordsManualEvictions() {
        CacheFactory.MetricsCollectingCache<String, String> cache = new CacheFactory.MetricsCollectingCache<>(
                CacheFactory.lru(10), new CacheMetricsCollector("evict-test")
        );

        cache.put("a", "1");
        cache.put("b", "2");
        cache.evict("a");
        cache.evict("b");

        assertEquals(0, cache.getStats().evictions(), "Explicit removals are not evictions");
        assertEquals(2, cache.getMetricsCollector().getManualEvictions());
    }

    @Test
    @DisplayName("withMetrics() records evictions made by the policy")
    void testWithMetrics_recordsPolicyEvictions() {
        CacheFactory.MetricsCollectingCache<String, String> cache = new CacheFactory.MetricsCollectingCache<>(
                CacheFactory.lru(2), new CacheMetricsCollector("capacity-test")
        );

        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        cache.put("d", "4");
        cache.put("d", "5");

        assertEquals(2, cache.getStats().evictions());
        assertEquals(2, cache.getMetricsCollector().getPolicyEvictions());
    }

    @Test
    @DisplayName("withMetrics() getMetricsSnapshot() returns full snapshot")
    void testWithMetrics_snapshotContainsCorrectData() {
        Cache<String, String> cache = CacheFactory.withMetrics(
                CacheFactory.lru(10), "snapshot-test"
        );

        cache.put("x", "1");
        cache.get("x");
        cache.get("miss");

        CacheFactory.MetricsCollectingCache<String, String> metricsCache =
                (CacheFactory.MetricsCollectingCache<String, String>) cache;

        CacheMetricsCollector.MetricsSnapshot snap = metricsCache.getMetricsSnapshot();

        assertEquals("snapshot-test", snap.getCacheName());
        assertEquals(1, snap.getHits());
        assertEquals(1, snap.getMisses());
        assertEquals(0.5, snap.getHitRate(), 0.001,
                "1 hit and 1 miss = 50% hit rate");
    }

    @Test
    @DisplayName("withMetrics() with null delegate throws IllegalArgumentException")
    void testWithMetrics_nullDelegate_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.withMetrics(null, "name"),
                "Null delegate should throw"
        );
    }

    @Test
    @DisplayName("withMetrics() with null name throws IllegalArgumentException")
    void testWithMetrics_nullName_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.withMetrics(CacheFactory.lru(10), null),
                "Null cache name should throw"
        );
    }

    @Test
    @DisplayName("Builder with policy only returns bare policy cache")
    void testBuilder_policyOnly_returnsBareCache() {
        Cache<String, String> cache = CacheFactory
                .<String, String>builder(CachePolicyType.LRU, 10)
                .build();

        assertInstanceOf(LRUCache.class, cache,
                "Builder with no wrappers should return bare policy cache");
    }

    @Test
    @DisplayName("Builder with TTL wraps in TTLCache")
    void testBuilder_withTTL_wrapsInTTLCache() {
        Cache<String, String> cache = CacheFactory
                .<String, String>builder(CachePolicyType.LRU, 10)
                .withTTL(Duration.ofSeconds(30))
                .build();

        assertInstanceOf(TTLCache.class, cache,
                "Builder with TTL should return TTLCache");
    }

    @Test
    @DisplayName("Builder with metrics wraps in MetricsCollectingCache")
    void testBuilder_withMetrics_wrapsInMetricsCache() {
        Cache<String, String> cache = CacheFactory
                .<String, String>builder(CachePolicyType.LRU, 10)
                .withMetrics("builder-test")
                .build();

        assertInstanceOf(CacheFactory.MetricsCollectingCache.class, cache,
                "Builder with metrics should return MetricsCollectingCache");
    }

    @Test
    @DisplayName("Builder with TTL + metrics: outermost wrapper is MetricsCollectingCache")
    void testBuilder_TTLAndMetrics_correctWrapperOrder() {
        Cache<String, String> cache = CacheFactory
                .<String, String>builder(CachePolicyType.ARC, 100)
                .withTTL(Duration.ofSeconds(60))
                .withMetrics("composed-cache")
                .build();

        assertInstanceOf(CacheFactory.MetricsCollectingCache.class, cache,
                "With both TTL and metrics, MetricsCollectingCache must be outermost");
    }

    @Test
    @DisplayName("Builder-composed cache stores and retrieves correctly end-to-end")
    void testBuilder_composedCache_functionalEndToEnd() {
        Cache<String, String> cache = CacheFactory
                .<String, String>builder(CachePolicyType.LFU, 5)
                .withTTL(Duration.ofSeconds(30))
                .withMetrics("e2e-test")
                .build();

        cache.put("name", "Alice");
        cache.put("city", "Delhi");

        assertEquals("Alice", cache.get("name"));
        assertEquals("Delhi", cache.get("city"));
        assertNull(cache.get("country"), "Missing key should return null");
        assertEquals(2, cache.size());
    }

    @Test
    @DisplayName("Builder with all three policies produces correct types")
    void testBuilder_allPolicies_produceCaches() {
        for (CachePolicyType policy : CachePolicyType.values()) {
            Cache<String, String> cache = CacheFactory
                    .<String, String>builder(policy, 10)
                    .build();

            assertNotNull(cache, "Builder must produce non-null for: " + policy);
            cache.put("k", "v");
            assertEquals("v", cache.get("k"),
                    "Cache from builder(" + policy + ") must function correctly");
        }
    }

    @Test
    @DisplayName("Builder withMetrics: hit/miss stats flow through correctly")
    void testBuilder_metricsStats_flowThroughCorrectly() {
        Cache<String, String> cache = CacheFactory
                .<String, String>builder(CachePolicyType.LRU, 10)
                .withMetrics("stats-flow-test")
                .build();

        cache.put("a", "1");
        cache.put("b", "2");

        cache.get("a");
        cache.get("b");
        cache.get("absent");

        CacheStats stats = cache.getStats();

        assertEquals(2, stats.hits(),   "2 hits expected");
        assertEquals(1, stats.misses(), "1 miss expected");
    }

    @Test
    @DisplayName("Builder withSweepInterval configures TTL sweeper interval")
    void testBuilder_withSweepInterval_doesNotThrow() {
        assertDoesNotThrow(() -> {
            Cache<String, String> cache = CacheFactory
                    .<String, String>builder(CachePolicyType.LRU, 10)
                    .withTTL(Duration.ofSeconds(5))
                    .withSweepInterval(100)
                    .build();

            cache.put("key", "value");
            assertEquals("value", cache.get("key"));
        });
    }

    @Test
    @DisplayName("Builder withSweepInterval without withTTL still builds correctly")
    void testBuilder_sweepIntervalWithoutTTL_stillBuilds() {
        assertDoesNotThrow(() -> {
            Cache<String, String> cache = CacheFactory
                    .<String, String>builder(CachePolicyType.LRU, 10)
                    .withSweepInterval(200)
                    .build();

            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
        });
    }

    @Test
    @DisplayName("lru() with capacity=0 throws IllegalArgumentException")
    void testLRU_zeroCapacity_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.lru(0));
    }

    @Test
    @DisplayName("lru() with negative capacity throws IllegalArgumentException")
    void testLRU_negativeCapacity_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.lru(-5));
    }

    @Test
    @DisplayName("lfu() with zero capacity throws IllegalArgumentException")
    void testLFU_zeroCapacity_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.lfu(0));
    }

    @Test
    @DisplayName("arc() with zero capacity throws IllegalArgumentException")
    void testARC_zeroCapacity_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.arc(0));
    }

    @Test
    @DisplayName("withPolicy() with zero capacity throws IllegalArgumentException")
    void testWithPolicy_zeroCapacity_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.withPolicy(CachePolicyType.LRU, 0));
    }

    @Test
    @DisplayName("Builder with zero capacity throws IllegalArgumentException")
    void testBuilder_zeroCapacity_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.builder(CachePolicyType.LRU, 0));
    }

    @Test
    @DisplayName("Builder withMetrics with null name throws IllegalArgumentException")
    void testBuilder_withMetrics_nullName_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory
                        .<String, String>builder(CachePolicyType.LRU, 10)
                        .withMetrics(null)
                        .build()
        );
    }

    @Test
    @DisplayName("Builder withTTL with null duration throws IllegalArgumentException")
    void testBuilder_withTTL_nullDuration_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> CacheFactory
                        .<String, String>builder(CachePolicyType.LRU, 10)
                        .withTTL(null)
        );
    }

    @Test
    @DisplayName("CacheFactory constructor throws UnsupportedOperationException")
    void testCacheFactory_notInstantiable() throws Exception {
        var constructor = CacheFactory.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        assertThrows(java.lang.reflect.InvocationTargetException.class,
                constructor::newInstance,
                "CacheFactory must not be instantiable"
        );
    }

    @Test
    @DisplayName("MetricsCollectingCache: reset() clears all counters")
    void testMetricsCache_reset_clearsCounters() {
        Cache<String, String> cache = CacheFactory.withMetrics(
                CacheFactory.lru(10), "reset-test"
        );

        cache.put("k", "v");
        cache.get("k");
        cache.get("miss");

        CacheFactory.MetricsCollectingCache<String, String> metricsCache =
                (CacheFactory.MetricsCollectingCache<String, String>) cache;
        metricsCache.getMetricsCollector().reset();

        CacheStats stats = cache.getStats();
        assertEquals(0, stats.hits(),   "Hits should be 0 after reset");
        assertEquals(0, stats.misses(), "Misses should be 0 after reset");

        assertEquals("v", cache.get("k"),
                "Data must still be accessible after stats reset");
    }

    @Test
    @DisplayName("withTTL() applies the default TTL to plain puts")
    void testWithTTL_appliesDefaultTtl() throws InterruptedException {
        TTLCache<String, String> cache = CacheFactory.withTTL(
                new CoarseGrainedCache<>(CacheFactory.lru(10)), Duration.ofMillis(200));
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
            Thread.sleep(400);
            assertNull(cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("builder().withTTL() applies the default TTL to plain puts")
    void testBuilderWithTTL_appliesDefaultTtl() throws InterruptedException {
        Cache<String, String> cache = CacheFactory.<String, String>builder(CachePolicyType.LFU, 10)
                .withTTL(Duration.ofMillis(200))
                .withSweepInterval(50)
                .build();
        cache.put("k", "v");
        Thread.sleep(400);
        assertNull(cache.get("k"));
        ((TTLCache<String, String>) cache).shutdown();
    }
}
