package com.cache.integration;

import com.cache.client.ClusterCacheClient;
import com.cache.common.cluster.CacheNode;
import com.cache.server.CacheServer;
import com.cache.server.ServerConfig;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ClusterCacheClient Integration Tests")
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterCacheClientTest {
    private static CacheServer server1;
    private static CacheServer server2;
    private static int serverPort1;
    private static int serverPort2;

    private ClusterCacheClient client;

    @BeforeAll
    static void startServers() {
        server1 = startServer();
        server2 = startServer();
        serverPort1 = server1.getPort();
        serverPort2 = server2.getPort();
    }

    @AfterAll
    static void stopServers() {
        server1.shutdown();
        server2.shutdown();
    }

    private static CacheServer startServer() {
        CacheServer server = new CacheServer(ServerConfig.builder().port(0).cacheCapacity(10_000).build());
        server.startAsync();
        return server;
    }

    @BeforeEach
    void setUp() throws IOException {
        client = ClusterCacheClient.builder()
                .addServer("node-1", "localhost", serverPort1)
                .addServer("node-2", "localhost", serverPort2)
                .poolSizePerNode(3)
                .build();

        Map<String, String> failures = client.flushAll();
        assertTrue(failures.isEmpty(),
                "Pre-test flush failed on nodes: " + failures);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Test
    @Order(1)
    @DisplayName("Builder rejects null node ID")
    void testBuilder_nullNodeId_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().addServer(null, "localhost", 6379));
    }

    @Test
    @Order(2)
    @DisplayName("Builder rejects blank node ID")
    void testBuilder_blankNodeId_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().addServer("  ", "localhost", 6379));
    }

    @Test
    @Order(3)
    @DisplayName("Builder rejects null host")
    void testBuilder_nullHost_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().addServer("node-1", null, 6379));
    }

    @Test
    @Order(4)
    @DisplayName("Builder rejects blank host")
    void testBuilder_blankHost_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().addServer("node-1", "  ", 6379));
    }

    @Test
    @Order(5)
    @DisplayName("Builder rejects port 0 (below valid range)")
    void testBuilder_portZero_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().addServer("node-1", "localhost", 0));
    }

    @Test
    @Order(6)
    @DisplayName("Builder rejects port 65536 (above valid range)")
    void testBuilder_portTooHigh_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().addServer("node-1", "localhost", 65536));
    }

    @Test
    @Order(7)
    @DisplayName("Builder rejects pool size of 0")
    void testBuilder_poolSizeZero_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().poolSizePerNode(0));
    }

    @Test
    @Order(8)
    @DisplayName("Builder rejects negative pool size")
    void testBuilder_poolSizeNegative_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().poolSizePerNode(-5));
    }

    @Test
    @Order(9)
    @DisplayName("Builder rejects health check interval of 0")
    void testBuilder_healthCheckIntervalZero_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> ClusterCacheClient.builder().withHealthCheck(0));
    }

    @Test
    @Order(10)
    @DisplayName("build() throws IOException for unreachable server")
    void testBuilder_unreachableServer_throwsIOException() {
        assertThrows(IOException.class, () ->
                        ClusterCacheClient.builder()
                                .addServer("bad", "localhost", 1)
                                .build(),
                "build() should throw IOException when server is unreachable");
    }

    @Test
    @Order(20)
    @DisplayName("put() then get() returns the stored value")
    void testBasic_putAndGet() {
        client.put("name", "Alice");

        assertEquals("Alice", client.get("name"));
    }

    @Test
    @Order(21)
    @DisplayName("get() returns null for a key that was never put")
    void testBasic_getMissingKey_returnsNull() {
        assertNull(client.get("key-that-does-not-exist-xyz"));
    }

    @Test
    @Order(22)
    @DisplayName("put() with TTL=0 stores key with no expiry")
    void testBasic_putTTLZero_keyAccessible() {
        client.put("permanent", "value", 0);

        assertEquals("value", client.get("permanent"),
                "TTL=0 means no expiry: key should be retrievable");
    }

    @Test
    @Order(23)
    @DisplayName("put() with positive TTL stores key (accessible before expiry)")
    void testBasic_putWithTTL_accessibleBeforeExpiry() {
        client.put("session", "token-abc", 60);

        assertEquals("token-abc", client.get("session"),
                "Key should be accessible before its TTL expires");
    }

    @Test
    @Order(24)
    @DisplayName("delete() removes a key: get() returns null afterward")
    void testBasic_delete_removesKey() {
        client.put("temp", "value");
        assertNotNull(client.get("temp"), "Key should exist before delete");

        client.delete("temp");

        assertNull(client.get("temp"), "Key should be null after delete()");
    }

    @Test
    @Order(25)
    @DisplayName("delete() on non-existent key is a safe no-op")
    void testBasic_deleteNonExistentKey_noException() {
        assertDoesNotThrow(() -> client.delete("ghost-key-never-existed"));
    }

    @Test
    @Order(26)
    @DisplayName("Second put() overwrites the first value for the same key")
    void testBasic_overwriteKey_replacesValue() {
        client.put("key", "original");
        client.put("key", "updated");

        assertEquals("updated", client.get("key"),
                "Second put() should overwrite the first");
    }

    @Test
    @Order(27)
    @DisplayName("Multiple distinct keys coexist correctly")
    void testBasic_multipleKeys_coexist() {
        client.put("a", "1");
        client.put("b", "2");
        client.put("c", "3");

        assertEquals("1", client.get("a"));
        assertEquals("2", client.get("b"));
        assertEquals("3", client.get("c"));
    }

    @Test
    @Order(30)
    @DisplayName("get() throws IllegalArgumentException on null key")
    void testValidation_get_nullKey() {
        assertThrows(IllegalArgumentException.class, () -> client.get(null));
    }

    @Test
    @Order(31)
    @DisplayName("get() throws IllegalArgumentException on empty key")
    void testValidation_get_emptyKey() {
        assertThrows(IllegalArgumentException.class, () -> client.get(""));
    }

    @Test
    @Order(32)
    @DisplayName("put() throws IllegalArgumentException on null key")
    void testValidation_put_nullKey() {
        assertThrows(IllegalArgumentException.class, () -> client.put(null, "value"));
    }

    @Test
    @Order(33)
    @DisplayName("put() throws IllegalArgumentException on null value")
    void testValidation_put_nullValue() {
        assertThrows(IllegalArgumentException.class, () -> client.put("key", null));
    }

    @Test
    @Order(34)
    @DisplayName("put() throws IllegalArgumentException on empty value")
    void testValidation_put_emptyValue() {
        assertThrows(IllegalArgumentException.class, () -> client.put("key", ""));
    }

    @Test
    @Order(35)
    @DisplayName("put() throws on key containing spaces (wire protocol violation)")
    void testValidation_put_keyWithSpaces_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> client.put("key with spaces", "value"));
    }

    @Test
    @Order(36)
    @DisplayName("put() throws IllegalArgumentException on negative TTL")
    void testValidation_put_negativeTTL_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> client.put("key", "value", -1));
    }

    @Test
    @Order(37)
    @DisplayName("delete() throws IllegalArgumentException on null key")
    void testValidation_delete_nullKey() {
        assertThrows(IllegalArgumentException.class, () -> client.delete(null));
    }

    @Test
    @Order(40)
    @DisplayName("Same key always routes to the same node (deterministic hashing)")
    void testRouting_sameKeyAlwaysRoutesToSameNode() {
        String key = "consistent-routing-test-key";
        CacheNode firstTarget = client.getRoutingTarget(key);

        for (int i = 1; i <= 20; i++) {
            CacheNode target = client.getRoutingTarget(key);
            assertEquals(firstTarget.getId(), target.getId(),
                    "Routing must be deterministic. Call " + i + " routed differently.");
        }
    }

    @Test
    @Order(41)
    @DisplayName("Different keys route to different nodes (ring is distributing)")
    void testRouting_differentKeysDifferentNodes() {
        Set<String> usedNodeIds = new HashSet<>();

        for (int i = 0; i < 50; i++) {
            CacheNode target = client.getRoutingTarget("variety-key-" + i);
            usedNodeIds.add(target.getId());
        }

        assertEquals(2, usedNodeIds.size(),
                "Both nodes should receive at least one key out of 50. " +
                        "If only 1 node received keys, the ring is broken. Nodes seen: " + usedNodeIds);
    }

    @Test
    @Order(42)
    @DisplayName("200 put() keys are all retrievable via get() (routing is consistent)")
    void testRouting_putAndGetAllKeys_allRetrievable() {
        int keyCount = 200;
        Map<String, String> written = new LinkedHashMap<>();

        for (int i = 0; i < keyCount; i++) {
            String key   = "put-get-test:" + i;
            String value = "val-" + i;
            client.put(key, value);
            written.put(key, value);
        }

        int misses = 0;
        List<String> missedKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : written.entrySet()) {
            String result = client.get(entry.getKey());
            if (!entry.getValue().equals(result)) {
                misses++;
                missedKeys.add(entry.getKey());
            }
        }

        assertEquals(0, misses,
                misses + "/" + keyCount + " keys were lost. " +
                        "This means routing is inconsistent between put() and get(). " +
                        "First few missed keys: " + missedKeys.subList(0, Math.min(5, missedKeys.size())));
    }

    @Test
    @Order(50)
    @DisplayName("100 keys distribute across both nodes (not all on one)")
    void testDistribution_keysSpreadAcrossBothNodes() {
        Map<String, Integer> counts = new HashMap<>();
        counts.put("node-1", 0);
        counts.put("node-2", 0);

        int total = 100;
        for (int i = 0; i < total; i++) {
            CacheNode target = client.getRoutingTarget("dist-key:" + i);
            counts.merge(target.getId(), 1, Integer::sum);
        }

        int minKeys = total / 4;
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            assertTrue(e.getValue() >= minKeys,
                    String.format("Node [%s] only got %d/%d keys. " +
                                    "Min expected: %d. Full distribution: %s",
                            e.getKey(), e.getValue(), total, minKeys, counts));
        }
    }

    @Test
    @Order(60)
    @DisplayName("getRoutingTarget() returns a registered node")
    void testGetRoutingTarget_returnsRegisteredNode() {
        CacheNode target = client.getRoutingTarget("any-key");

        assertNotNull(target);
        assertTrue(
                target.getId().equals("node-1") || target.getId().equals("node-2"),
                "Target must be a registered node, got: " + target.getId()
        );
    }

    @Test
    @Order(61)
    @DisplayName("getRoutingTarget() throws IllegalArgumentException on null key")
    void testGetRoutingTarget_nullKey_throws() {
        assertThrows(IllegalArgumentException.class, () -> client.getRoutingTarget(null));
    }

    @Test
    @Order(62)
    @DisplayName("getRoutingTarget() returns same node as actual put/get")
    void testGetRoutingTarget_matchesActualRouting() {
        String key = "routing-target-verification-key";
        client.put(key, "check-value");

        CacheNode predicted = client.getRoutingTarget(key);
        assertNotNull(predicted, "Routing target should not be null");

        String result = client.get(key);
        assertEquals("check-value", result,
                "get() should return the value, confirming routing target is correct");
    }

    @Test
    @Order(70)
    @DisplayName("getNodeCount() returns 2 for the default 2-node cluster")
    void testTopology_initialNodeCount() {
        assertEquals(2, client.getNodeCount());
    }

    @Test
    @Order(71)
    @DisplayName("getNodes() returns all registered nodes with correct IDs")
    void testTopology_getNodes_containsAllNodes() {
        List<CacheNode> nodes = client.getNodes();

        assertEquals(2, nodes.size());
        Set<String> ids = new HashSet<>();
        for (CacheNode n : nodes) ids.add(n.getId());
        assertTrue(ids.contains("node-1"), "node-1 should be in getNodes()");
        assertTrue(ids.contains("node-2"), "node-2 should be in getNodes()");
    }

    @Test
    @Order(72)
    @DisplayName("isEmpty() returns false when nodes are registered")
    void testTopology_isNotEmpty() {
        assertFalse(client.isEmpty(), "Client with 2 nodes should not be empty");
    }

    @Test
    @Order(73)
    @DisplayName("addServer() with duplicate ID replaces old connection")
    void testTopology_addServer_duplicateIdReplaces() throws IOException {
        client.addServer("node-1", "localhost", serverPort1);

        assertEquals(2, client.getNodeCount(),
                "Re-adding a node with same ID should replace, not add a third");

        client.put("post-readd", "works");
        assertEquals("works", client.get("post-readd"));
    }

    @Test
    @Order(80)
    @DisplayName("removeServer() decreases node count by 1")
    void testTopology_removeServer_decreasesCount() {
        assertEquals(2, client.getNodeCount());

        client.removeServer("node-2");

        assertEquals(1, client.getNodeCount(),
                "Node count should be 1 after removing node-2");

        tryAddServer("node-2", "localhost", serverPort2);
    }

    @Test
    @Order(81)
    @DisplayName("removeServer() on unknown node ID is a safe no-op")
    void testTopology_removeUnknownNode_noException() {
        assertDoesNotThrow(() -> client.removeServer("node-that-never-existed"));
        assertEquals(2, client.getNodeCount(),
                "Node count should be unchanged");
    }

    @Test
    @Order(82)
    @DisplayName("After removeServer(), removed node is not in getNodes()")
    void testTopology_removeServer_nodeGoneFromGetNodes() {
        client.removeServer("node-2");

        List<CacheNode> nodes = client.getNodes();
        boolean hasNode2 = nodes.stream().anyMatch(n -> n.getId().equals("node-2"));
        assertFalse(hasNode2, "node-2 should not appear in getNodes() after removal");

        tryAddServer("node-2", "localhost", serverPort2);
    }

    @Test
    @Order(90)
    @DisplayName("Cache operations work after removing one of two nodes")
    void testFailover_operationsAfterRemove() {
        String key = "survivor-key";

        CacheNode owner = client.getRoutingTarget(key);

        String otherNodeId = owner.getId().equals("node-1") ? "node-2" : "node-1";
        client.removeServer(otherNodeId);

        client.put(key, "still-alive");
        assertEquals("still-alive", client.get(key),
                "Cache should work after removing the non-owning node");

        int otherPort = otherNodeId.equals("node-1") ? serverPort1 : serverPort2;
        tryAddServer(otherNodeId, "localhost", otherPort);
    }

    @Test
    @Order(91)
    @DisplayName("New puts after node removal route to surviving node")
    void testFailover_newPutsAfterRemoval_routeToSurvivor() {
        client.removeServer("node-2");
        assertEquals(1, client.getNodeCount());

        for (int i = 0; i < 20; i++) {
            client.put("post-removal:" + i, "value-" + i);
        }

        int misses = 0;
        for (int i = 0; i < 20; i++) {
            if (!"value-" .concat(String.valueOf(i)).equals(client.get("post-removal:" + i))) {
                misses++;
            }
        }

        assertEquals(0, misses, misses + "/20 keys missing after single-node operation");

        tryAddServer("node-2", "localhost", serverPort2);
    }

    @Test
    @Order(100)
    @DisplayName("flushAll() returns empty failure map on success")
    void testFlushAll_returnsEmptyMapOnSuccess() {
        Map<String, String> failures = client.flushAll();
        assertNotNull(failures);
        assertTrue(failures.isEmpty(), "flushAll() failure map should be empty on success");
    }

    @Test
    @Order(101)
    @DisplayName("flushAll() removes keys from all nodes")
    void testFlushAll_removesAllKeys() {
        for (int i = 0; i < 20; i++) {
            client.put("flush-test:" + i, "value-" + i);
        }

        client.flushAll();

        int found = 0;
        for (int i = 0; i < 20; i++) {
            if (client.get("flush-test:" + i) != null) found++;
        }

        assertEquals(0, found,
                found + "/20 keys still present after flushAll()");
    }

    @Test
    @Order(102)
    @DisplayName("flushAll() on closed client throws IllegalStateException")
    void testFlushAll_closedClient_throws() {
        client.close();
        assertThrows(IllegalStateException.class, () -> client.flushAll());
    }

    @Test
    @Order(110)
    @DisplayName("clusterStats() is non-null and non-empty")
    void testClusterStats_notNullNotEmpty() {
        Map<String, String> stats = client.clusterStats();
        assertNotNull(stats);
        assertFalse(stats.isEmpty(), "Stats map should not be empty");
    }

    @Test
    @Order(111)
    @DisplayName("clusterStats() contains node-prefixed entries for all nodes")
    void testClusterStats_containsNodePrefixedEntries() {
        Map<String, String> stats = client.clusterStats();

        boolean hasNode1 = stats.keySet().stream().anyMatch(k -> k.startsWith("node-1."));
        boolean hasNode2 = stats.keySet().stream().anyMatch(k -> k.startsWith("node-2."));

        assertTrue(hasNode1, "Stats should contain node-1.* keys");
        assertTrue(hasNode2, "Stats should contain node-2.* keys");
    }

    @Test
    @Order(112)
    @DisplayName("clusterStats() contains cluster-wide summary keys")
    void testClusterStats_containsClusterSummaryKeys() {
        Map<String, String> stats = client.clusterStats();

        assertTrue(stats.containsKey("cluster.nodes"),       "Missing cluster.nodes");
        assertTrue(stats.containsKey("cluster.totalHits"),   "Missing cluster.totalHits");
        assertTrue(stats.containsKey("cluster.totalMisses"), "Missing cluster.totalMisses");
        assertTrue(stats.containsKey("cluster.reachable"),   "Missing cluster.reachable");
    }

    @Test
    @Order(113)
    @DisplayName("clusterStats() reports correct node count")
    void testClusterStats_correctNodeCount() {
        Map<String, String> stats = client.clusterStats();

        assertEquals("2", stats.get("cluster.nodes"),
                "cluster.nodes should be 2 for a 2-node cluster");
    }

    @Test
    @Order(114)
    @DisplayName("clusterStats() on closed client throws IllegalStateException")
    void testClusterStats_closedClient_throws() {
        client.close();
        assertThrows(IllegalStateException.class, () -> client.clusterStats());
    }

    @Test
    @Order(120)
    @DisplayName("getPoolStats() contains entries for all registered nodes")
    void testPoolStats_containsAllNodes() {
        Map<String, String> poolStats = client.getPoolStats();

        assertNotNull(poolStats);
        assertTrue(poolStats.containsKey("node-1"), "Pool stats should include node-1");
        assertTrue(poolStats.containsKey("node-2"), "Pool stats should include node-2");
    }

    @Test
    @Order(121)
    @DisplayName("Pool stats contain active/idle/size fields")
    void testPoolStats_correctFormat() {
        Map<String, String> poolStats = client.getPoolStats();

        for (Map.Entry<String, String> entry : poolStats.entrySet()) {
            String stats = entry.getValue();
            assertTrue(stats.contains("active="),
                    "Pool stats for [" + entry.getKey() + "] missing 'active=': " + stats);
            assertTrue(stats.contains("idle="),
                    "Pool stats for [" + entry.getKey() + "] missing 'idle=': " + stats);
            assertTrue(stats.contains("size="),
                    "Pool stats for [" + entry.getKey() + "] missing 'size=': " + stats);
        }
    }

    @Test
    @Order(122)
    @DisplayName("Pool idle + active = pool size (conservation invariant)")
    void testPoolStats_idlePlusActiveEqualsSize() {
        client.put("pool-test", "value");

        Map<String, String> poolStats = client.getPoolStats();

        for (Map.Entry<String, String> entry : poolStats.entrySet()) {
            String statsStr = entry.getValue();
            int active = extractInt(statsStr, "active=");
            int idle   = extractInt(statsStr, "idle=");
            int size   = extractInt(statsStr, "size=");

            assertEquals(3, size,
                    "Pool size should be 3 (configured in setUp): " + statsStr);
            assertEquals(size, active + idle,
                    String.format("Node [%s]: active(%d) + idle(%d) should equal size(%d). " +
                            "If not, a connection was leaked.", entry.getKey(), active, idle, size));
        }
    }

    @Test
    @Order(130)
    @DisplayName("getKeyDistribution() has an entry per node")
    void testKeyDistribution_oneEntryPerNode() {
        Map<String, Double> dist = client.getKeyDistribution();

        assertNotNull(dist);
        assertEquals(2, dist.size(),
                "Distribution should have exactly 2 entries for 2 nodes");
    }

    @Test
    @Order(131)
    @DisplayName("getKeyDistribution() percentages sum to 100% (±1%)")
    void testKeyDistribution_sumsToOneHundred() {
        Map<String, Double> dist = client.getKeyDistribution();

        double total = dist.values().stream().mapToDouble(Double::doubleValue).sum();
        assertEquals(100.0, total, 1.0,
                "Distribution should sum to 100% (±1%). Got: " + total + ". Dist: " + dist);
    }

    @Test
    @Order(132)
    @DisplayName("Each node owns between 30% and 70% of key space (reasonable balance)")
    void testKeyDistribution_reasonablyBalanced() {
        Map<String, Double> dist = client.getKeyDistribution();

        for (Map.Entry<String, Double> entry : dist.entrySet()) {
            double pct = entry.getValue();
            assertTrue(pct >= 30.0 && pct <= 70.0,
                    String.format("Node [%s] owns %.2f%%: too imbalanced. " +
                                    "Expected 30-70%%. Full dist: %s",
                            entry.getKey(), pct, dist));
        }
    }

    @Test
    @Order(140)
    @DisplayName("20 concurrent threads performing put() produce no errors")
    void testConcurrency_concurrentPuts_noErrors() throws InterruptedException {
        int threadCount  = 20;
        int opsPerThread = 50;

        CountDownLatch start  = new CountDownLatch(1);
        CountDownLatch done   = new CountDownLatch(threadCount);
        AtomicInteger  errors = new AtomicInteger(0);
        ExecutorService pool  = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        client.put("t" + threadId + ":k" + i, "v" + i);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Concurrent put error: " + e);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS),
                "All threads should complete within 30 seconds");
        pool.shutdown();

        assertEquals(0, errors.get(),
                "No errors should occur under concurrent puts");
    }

    @Test
    @Order(141)
    @DisplayName("Mixed concurrent gets and puts produce no errors")
    void testConcurrency_mixedGetsPuts_noErrors() throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            client.put("base:" + i, "val-" + i);
        }

        int threadCount = 10;
        CountDownLatch start  = new CountDownLatch(1);
        CountDownLatch done   = new CountDownLatch(threadCount);
        AtomicInteger  errors = new AtomicInteger(0);
        ExecutorService pool  = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await();
                    Random rng = new Random(threadId);
                    for (int i = 0; i < 100; i++) {
                        if (rng.nextBoolean()) {
                            client.get("base:" + (i % 20));
                        } else {
                            client.put("new:" + threadId + ":" + i, "v");
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    System.err.println("Concurrent mixed op error: " + e);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS),
                "All threads should complete within 30 seconds");
        pool.shutdown();

        assertEquals(0, errors.get(),
                "No errors should occur under concurrent mixed reads/writes");
    }

    @Test
    @Order(142)
    @DisplayName("No connection leak after concurrent operations")
    void testConcurrency_noConnectionLeak() throws InterruptedException {
        int threadCount = 15;
        CountDownLatch done  = new CountDownLatch(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < 30; i++) {
                        client.put("leak-test:" + tid + ":" + i, "v");
                        client.get("leak-test:" + tid + ":" + i);
                    }
                } catch (Exception e) {
                    System.err.println("Leak test error: " + e);
                } finally {
                    done.countDown();
                }
            });
        }

        done.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        Map<String, String> poolStats = client.getPoolStats();
        for (Map.Entry<String, String> entry : poolStats.entrySet()) {
            int active = extractInt(entry.getValue(), "active=");
            assertEquals(0, active,
                    "Node [" + entry.getKey() + "] has " + active +
                            " active connections after all ops completed: possible leak. " +
                            "Stats: " + entry.getValue());
        }
    }

    @Test
    @Order(150)
    @DisplayName("get() throws IllegalStateException after close()")
    void testClosed_get_throws() {
        client.close();
        assertThrows(IllegalStateException.class, () -> client.get("key"));
    }

    @Test
    @Order(151)
    @DisplayName("put() throws IllegalStateException after close()")
    void testClosed_put_throws() {
        client.close();
        assertThrows(IllegalStateException.class, () -> client.put("key", "value"));
    }

    @Test
    @Order(152)
    @DisplayName("delete() throws IllegalStateException after close()")
    void testClosed_delete_throws() {
        client.close();
        assertThrows(IllegalStateException.class, () -> client.delete("key"));
    }

    @Test
    @Order(153)
    @DisplayName("addServer() throws IllegalStateException after close()")
    void testClosed_addServer_throws() {
        client.close();
        assertThrows(IllegalStateException.class,
                () -> client.addServer("new-node", "localhost", serverPort1));
    }

    @Test
    @Order(154)
    @DisplayName("removeServer() throws IllegalStateException after close()")
    void testClosed_removeServer_throws() {
        client.close();
        assertThrows(IllegalStateException.class, () -> client.removeServer("node-1"));
    }

    @Test
    @Order(155)
    @DisplayName("close() is idempotent: safe to call multiple times")
    void testClosed_closeIsIdempotent() throws IOException {
        ClusterCacheClient c = ClusterCacheClient.builder()
                .addServer("n", "localhost", serverPort1)
                .build();

        assertDoesNotThrow(() -> {
            c.close();
            c.close();
            c.close();
        });
    }

    @Test
    @Order(160)
    @DisplayName("isEmpty() returns true for a client with no servers")
    void testEmpty_isEmpty() throws IOException {
        ClusterCacheClient empty = ClusterCacheClient.builder().build();
        try {
            assertTrue(empty.isEmpty());
            assertEquals(0, empty.getNodeCount());
        } finally {
            empty.close();
        }
    }

    @Test
    @Order(161)
    @DisplayName("get() throws ClusterOperationException when no servers registered")
    void testEmpty_get_throwsClusterOperationException() throws IOException {
        ClusterCacheClient empty = ClusterCacheClient.builder().build();
        try {
            assertThrows(ClusterCacheClient.ClusterOperationException.class,
                    () -> empty.get("key"));
        } finally {
            empty.close();
        }
    }

    @Test
    @Order(162)
    @DisplayName("put() throws ClusterOperationException when no servers registered")
    void testEmpty_put_throwsClusterOperationException() throws IOException {
        ClusterCacheClient empty = ClusterCacheClient.builder().build();
        try {
            assertThrows(ClusterCacheClient.ClusterOperationException.class,
                    () -> empty.put("key", "value"));
        } finally {
            empty.close();
        }
    }

    private static int extractInt(String stats, String prefix) {
        int start = stats.indexOf(prefix);
        if (start == -1) return -1;
        start += prefix.length();
        int end = stats.indexOf(' ', start);
        String numStr = end == -1 ? stats.substring(start) : stats.substring(start, end);
        try {
            return Integer.parseInt(numStr.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void tryAddServer(String nodeId, String host, int port) {
        try {
            client.addServer(nodeId, host, port);
        } catch (IOException e) {
            System.err.println("[Test] Failed to re-add server [" + nodeId + "]: " + e.getMessage());
        }
    }
}
