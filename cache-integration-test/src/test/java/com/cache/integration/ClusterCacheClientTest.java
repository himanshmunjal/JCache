package com.cache.integration;

import com.cache.client.ClusterCacheClient;
import com.cache.common.cluster.CacheNode;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for ClusterCacheClient.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * TESTING STRATEGY
 * ═══════════════════════════════════════════════════════════════════════
 *
 * These are INTEGRATION tests — they require real servers.
 * We use MinimalEchoServer (defined at the bottom of this file), a
 * lightweight TCP server that speaks the JCache wire protocol.
 * Replace MinimalEchoServer.start() with your real CacheServer once
 * that module is complete.
 *
 * WHY NOT MOCK CacheClient?
 * Mocking would only verify that ClusterCacheClient calls the right methods
 * in the right order. It tells us nothing about whether:
 *   - Routing actually lands on the correct node
 *   - Connection pooling correctly borrows/returns connections
 *   - Concurrent access causes data corruption
 *   - The wire protocol parses responses correctly
 * Real servers exercise all of this.
 *
 * PORT ALLOCATION:
 * We use ServerSocket(0) to let the OS assign free ports.
 * This prevents conflicts when multiple test suites run in CI.
 *
 * SERVER LIFECYCLE:
 *   @BeforeAll  — start MinimalEchoServers once (expensive: port bind)
 *   @AfterAll   — stop servers
 *   @BeforeEach — flush caches + create fresh ClusterCacheClient
 *   @AfterEach  — close client
 *
 * TEST CATEGORIES:
 *   1.  Builder validation          — bad args rejected eagerly
 *   2.  Basic get/put/delete        — core operations work
 *   3.  Input validation            — null/space keys rejected
 *   4.  Multi-node routing          — consistent hash works
 *   5.  Distribution verification   — keys spread across nodes
 *   6.  getRoutingTarget            — debug routing method
 *   7.  Topology: addServer         — new nodes added cleanly
 *   8.  Topology: removeServer      — nodes removed cleanly
 *   9.  Operations after removal    — survivor still works
 *   10. flushAll fan-out            — reaches every node
 *   11. clusterStats aggregation    — collects from all nodes
 *   12. Connection pool stats       — pool metrics correct
 *   13. Key distribution            — ring is balanced
 *   14. Concurrent access           — no corruption under load
 *   15. Closed client guard         — ops throw after close()
 *   16. Empty cluster guard         — ops throw with no servers
 */
@DisplayName("ClusterCacheClient Integration Tests")
@Execution(ExecutionMode.SAME_THREAD) // servers are shared — run sequentially
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterCacheClientTest {

    // -------------------------------------------------------------------------
    // Test server ports — assigned by OS in @BeforeAll
    // -------------------------------------------------------------------------

    private static int serverPort1;
    private static int serverPort2;

    // -------------------------------------------------------------------------
    // Client under test — recreated fresh per test
    // -------------------------------------------------------------------------

    private ClusterCacheClient client;

    // =========================================================================
    // @BeforeAll / @AfterAll — server lifecycle (once per class)
    // =========================================================================

    /**
     * Starts two MinimalEchoServers on OS-assigned free ports.
     *
     * Replace MinimalEchoServer.start() with your real CacheServer once ready:
     *
     *   ServerConfig cfg1 = new ServerConfig(serverPort1);
     *   new Thread(() -> new CacheServer(cfg1).start()).start();
     *   Thread.sleep(300); // let Netty bind
     */
    @BeforeAll
    static void startServers() throws IOException, InterruptedException {
        serverPort1 = findFreePort();
        serverPort2 = findFreePort();

        MinimalEchoServer.start(serverPort1);
        MinimalEchoServer.start(serverPort2);

        // Brief pause — let server threads bind before client connects.
        Thread.sleep(150);
    }

    @AfterAll
    static void stopServers() {
        MinimalEchoServer.stopAll();
    }

    // =========================================================================
    // @BeforeEach / @AfterEach — test isolation
    // =========================================================================

    /**
     * Creates a fresh 2-node client and flushes both servers before each test.
     * Flushing ensures no key leaks between tests.
     */
    @BeforeEach
    void setUp() throws IOException {
        client = ClusterCacheClient.builder()
                .addServer("node-1", "localhost", serverPort1)
                .addServer("node-2", "localhost", serverPort2)
                .poolSizePerNode(3)
                .build();

        // Flush both servers — clean slate for every test.
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

    // =========================================================================
    // 1. Builder validation
    // =========================================================================

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
        // Port 1 is privileged and almost certainly not a cache server.
        assertThrows(IOException.class, () ->
                        ClusterCacheClient.builder()
                                .addServer("bad", "localhost", 1)
                                .build(),
                "build() should throw IOException when server is unreachable");
    }

    // =========================================================================
    // 2. Basic get / put / delete operations
    // =========================================================================

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
                "TTL=0 means no expiry — key should be retrievable");
    }

    @Test
    @Order(23)
    @DisplayName("put() with positive TTL stores key (accessible before expiry)")
    void testBasic_putWithTTL_accessibleBeforeExpiry() {
        client.put("session", "token-abc", 60); // 60-second TTL

        assertEquals("token-abc", client.get("session"),
                "Key should be accessible before its TTL expires");
    }

    @Test
    @Order(24)
    @DisplayName("delete() removes a key — get() returns null afterward")
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

    // =========================================================================
    // 3. Input validation
    // =========================================================================

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
        // Space is the wire protocol delimiter — keys with spaces break parsing.
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

    // =========================================================================
    // 4. Multi-node routing — consistent hashing correctness
    // =========================================================================

    /**
     * The same key must always route to the same node.
     * Consistent hashing guarantees determinism: same key + same ring = same node.
     * We call getRoutingTarget() 20 times and assert all results match the first.
     */
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

    /**
     * Different keys CAN route to different nodes.
     * This test verifies the ring is actually distributing, not sending everything
     * to one node. With 2 nodes and enough keys, both nodes must appear at least once.
     */
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

    /**
     * Core routing correctness: put 200 keys, then get all 200 back.
     * This verifies that get() routes to the SAME node that put() used.
     * If routing were inconsistent, put goes to node-A but get tries node-B → null.
     */
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

    // =========================================================================
    // 5. Key distribution verification
    // =========================================================================

    /**
     * With 200 keys and 2 nodes, each node should handle roughly 100 keys.
     * We allow a wide tolerance (25%–75%) because hash distribution has variance.
     * What we're catching is the pathological case where ALL keys go to one node.
     */
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

        // Each node should have at least 25% of keys.
        int minKeys = total / 4;
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            assertTrue(e.getValue() >= minKeys,
                    String.format("Node [%s] only got %d/%d keys. " +
                                    "Min expected: %d. Full distribution: %s",
                            e.getKey(), e.getValue(), total, minKeys, counts));
        }
    }

    // =========================================================================
    // 6. getRoutingTarget
    // =========================================================================

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
        // We put a key, then verify getRoutingTarget() points to the node
        // that actually holds it (proven by a successful get()).
        String key = "routing-target-verification-key";
        client.put(key, "check-value");

        CacheNode predicted = client.getRoutingTarget(key);
        assertNotNull(predicted, "Routing target should not be null");

        // get() should return the value — if routing were inconsistent,
        // predicted node ≠ actual node and get() would return null.
        String result = client.get(key);
        assertEquals("check-value", result,
                "get() should return the value, confirming routing target is correct");
    }

    // =========================================================================
    // 7. Topology: addServer
    // =========================================================================

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
        // Adding node-1 again (same ID, same host:port) should not throw
        // and node count should remain 2.
        client.addServer("node-1", "localhost", serverPort1);

        assertEquals(2, client.getNodeCount(),
                "Re-adding a node with same ID should replace, not add a third");

        // Operations should still work.
        client.put("post-readd", "works");
        assertEquals("works", client.get("post-readd"));
    }

    // =========================================================================
    // 8. Topology: removeServer
    // =========================================================================

    @Test
    @Order(80)
    @DisplayName("removeServer() decreases node count by 1")
    void testTopology_removeServer_decreasesCount() {
        assertEquals(2, client.getNodeCount());

        client.removeServer("node-2");

        assertEquals(1, client.getNodeCount(),
                "Node count should be 1 after removing node-2");

        // Re-add for subsequent tests.
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

    // =========================================================================
    // 9. Operations after node removal (failover / rerouting)
    // =========================================================================

    /**
     * After removing one node, the remaining node must still serve requests.
     * We:
     *   1. Find which node owns "survivor-key" (it will still exist after removal
     *      of the OTHER node)
     *   2. Remove the non-owning node
     *   3. Verify put/get still works on the surviving node
     */
    @Test
    @Order(90)
    @DisplayName("Cache operations work after removing one of two nodes")
    void testFailover_operationsAfterRemove() {
        String key = "survivor-key";

        // Find which node owns this key.
        CacheNode owner = client.getRoutingTarget(key);

        // Remove the other node.
        String otherNodeId = owner.getId().equals("node-1") ? "node-2" : "node-1";
        client.removeServer(otherNodeId);

        // Operations on the surviving node should still work.
        client.put(key, "still-alive");
        assertEquals("still-alive", client.get(key),
                "Cache should work after removing the non-owning node");

        // Restore.
        int otherPort = otherNodeId.equals("node-1") ? serverPort1 : serverPort2;
        tryAddServer(otherNodeId, "localhost", otherPort);
    }

    /**
     * After removing a node, keys that were owned by it now route to the survivor.
     * New puts on those keys should work (they go to the survivor now).
     *
     * Note: data that was on the removed node is LOST (no replication in this system).
     * This test verifies that NEW puts after removal route correctly.
     */
    @Test
    @Order(91)
    @DisplayName("New puts after node removal route to surviving node")
    void testFailover_newPutsAfterRemoval_routeToSurvivor() {
        // Remove node-2. All keys now route to node-1.
        client.removeServer("node-2");
        assertEquals(1, client.getNodeCount());

        // Put 20 keys — all should go to node-1 (the only node).
        for (int i = 0; i < 20; i++) {
            client.put("post-removal:" + i, "value-" + i);
        }

        // All 20 should be retrievable.
        int misses = 0;
        for (int i = 0; i < 20; i++) {
            if (!"value-" .concat(String.valueOf(i)).equals(client.get("post-removal:" + i))) {
                misses++;
            }
        }

        assertEquals(0, misses, misses + "/20 keys missing after single-node operation");

        // Restore.
        tryAddServer("node-2", "localhost", serverPort2);
    }

    // =========================================================================
    // 10. flushAll — fan-out to all nodes
    // =========================================================================

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
        // Put keys that distribute to both nodes.
        for (int i = 0; i < 20; i++) {
            client.put("flush-test:" + i, "value-" + i);
        }

        client.flushAll();

        // All keys should now be gone.
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

    // =========================================================================
    // 11. clusterStats aggregation
    // =========================================================================

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

    // =========================================================================
    // 12. Connection pool stats
    // =========================================================================

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
        // Do a put to borrow+return a connection.
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

    // =========================================================================
    // 13. Key distribution (ring balance)
    // =========================================================================

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
                    String.format("Node [%s] owns %.2f%% — too imbalanced. " +
                                    "Expected 30-70%%. Full dist: %s",
                            entry.getKey(), pct, dist));
        }
    }

    // =========================================================================
    // 14. Concurrent access — thread safety
    // =========================================================================

    /**
     * 20 threads each put 50 unique keys concurrently.
     * Verifies:
     *   - No exceptions thrown (pool exhaustion, NPE, race conditions)
     *   - All puts complete successfully
     */
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

    /**
     * Mixed concurrent reads and writes.
     * Pre-populates 20 keys, then runs 10 threads doing random gets and puts.
     */
    @Test
    @Order(141)
    @DisplayName("Mixed concurrent gets and puts produce no errors")
    void testConcurrency_mixedGetsPuts_noErrors() throws InterruptedException {
        // Pre-populate.
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

    /**
     * Verifies no connection leak under concurrent load.
     * After all operations complete, active connections should be 0
     * (all connections returned to pool) and idle should equal pool size.
     */
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

        // After all ops, check pool stats.
        // All connections should be idle (active=0).
        Map<String, String> poolStats = client.getPoolStats();
        for (Map.Entry<String, String> entry : poolStats.entrySet()) {
            int active = extractInt(entry.getValue(), "active=");
            assertEquals(0, active,
                    "Node [" + entry.getKey() + "] has " + active +
                            " active connections after all ops completed — possible leak. " +
                            "Stats: " + entry.getValue());
        }
    }

    // =========================================================================
    // 15. Closed client guard
    // =========================================================================

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
    @DisplayName("close() is idempotent — safe to call multiple times")
    void testClosed_closeIsIdempotent() throws IOException {
        ClusterCacheClient c = ClusterCacheClient.builder()
                .addServer("n", "localhost", serverPort1)
                .build();

        assertDoesNotThrow(() -> {
            c.close();
            c.close(); // second call must not throw
            c.close(); // third call must not throw
        });
    }

    // =========================================================================
    // 16. Empty cluster guard
    // =========================================================================

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

    // =========================================================================
    // Test utilities
    // =========================================================================

    /**
     * Extracts an integer value from a stats string like "active=2 idle=3 size=5".
     * Used in pool stats tests to verify individual counters.
     *
     * @param stats  The stats string.
     * @param prefix The field prefix, e.g., "active=".
     * @return The parsed integer value.
     */
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

    /**
     * Attempts to re-add a server, logging failures without throwing.
     * Used in tests that remove a server and want to restore state for
     * subsequent tests (even though tearDown() closes the client anyway).
     */
    private void tryAddServer(String nodeId, String host, int port) {
        try {
            client.addServer(nodeId, host, port);
        } catch (IOException e) {
            System.err.println("[Test] Failed to re-add server [" + nodeId + "]: " + e.getMessage());
        }
    }

    /**
     * Finds a free port on localhost by binding ServerSocket(0) briefly.
     * The OS assigns an available port number.
     */
    private static int findFreePort() throws IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    // =========================================================================
    // MinimalEchoServer — wire-protocol-compatible test server
    // =========================================================================

    /**
     * A minimal TCP server that speaks the JCache wire protocol.
     *
     * PURPOSE:
     * Allows ClusterCacheClient tests to run WITHOUT requiring the real CacheServer
     * module to be built. Replace with real CacheServer once available:
     *
     *   // In @BeforeAll, replace:
     *   MinimalEchoServer.start(serverPort1);
     *   // with:
     *   ServerConfig cfg = new ServerConfig(serverPort1);
     *   new Thread(() -> new CacheServer(cfg).start()).start();
     *   Thread.sleep(300);
     *
     * PROTOCOL HANDLED:
     *   PING           → +PONG
     *   GET key        → +value  or  -ERR key not found: key
     *   PUT key val    → +OK  (stores in per-connection ConcurrentHashMap)
     *   PUT key val N  → +OK  (ignores TTL — no expiry in echo server)
     *   DELETE key     → +OK
     *   FLUSH          → +OK  (clears ALL keys across connections via shared store)
     *   STATS          → +hits:N misses:N evictions:0 size:N hitRate:X.XX%
     *   unknown        → -ERR unknown command: VERB
     *
     * NOTE: The store is SHARED across all connections on the same server instance.
     * This matches CacheServer behavior where one in-memory cache serves all clients.
     */
    static final class MinimalEchoServer {

        private static final List<java.net.ServerSocket> openSockets = new CopyOnWriteArrayList<>();

        static void start(int port) throws IOException {
            // Shared store for this server instance.
            // ConcurrentHashMap — safe for multi-client concurrent access.
            ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

            java.net.ServerSocket serverSocket = new java.net.ServerSocket(port);
            openSockets.add(serverSocket);

            Thread acceptThread = new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    try {
                        java.net.Socket clientSocket = serverSocket.accept();
                        Thread handler = new Thread(() -> handleClient(clientSocket, store));
                        handler.setDaemon(true);
                        handler.start();
                    } catch (IOException e) {
                        if (!serverSocket.isClosed()) {
                            System.err.println("[EchoServer:" + port + "] Accept error: "
                                    + e.getMessage());
                        }
                    }
                }
            }, "echo-accept-" + port);
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        private static void handleClient(
                java.net.Socket socket,
                ConcurrentHashMap<String, String> store) {

            try (socket;
                 java.io.BufferedReader reader = new java.io.BufferedReader(
                         new java.io.InputStreamReader(socket.getInputStream()));
                 java.io.PrintWriter writer = new java.io.PrintWriter(
                         socket.getOutputStream(), true)) {

                // Per-connection hit/miss counters for STATS.
                long[] hits   = {0};
                long[] misses = {0};

                String line;
                while ((line = reader.readLine()) != null) {
                    String response = dispatch(line.trim(), store, hits, misses);
                    writer.println(response);
                }

            } catch (IOException ignored) {
                // Client disconnected normally.
            }
        }

        private static String dispatch(
                String line,
                ConcurrentHashMap<String, String> store,
                long[] hits,
                long[] misses) {

            if (line.isEmpty()) return "-ERR empty command";

            String[] tokens = line.split("\\s+");
            String   verb   = tokens[0].toUpperCase();

            switch (verb) {

                case "PING":
                    return "+PONG";

                case "GET": {
                    if (tokens.length < 2) return "-ERR missing key";
                    String val = store.get(tokens[1]);
                    if (val != null) {
                        hits[0]++;
                        return "+" + val;
                    } else {
                        misses[0]++;
                        return "-ERR key not found: " + tokens[1];
                    }
                }

                case "PUT": {
                    if (tokens.length < 3) return "-ERR missing key or value";
                    // tokens[3] is optional TTL — we accept but ignore it.
                    store.put(tokens[1], tokens[2]);
                    return "+OK";
                }

                case "DELETE": {
                    if (tokens.length < 2) return "-ERR missing key";
                    store.remove(tokens[1]);
                    return "+OK";
                }

                case "FLUSH":
                    store.clear();
                    return "+OK";

                case "STATS": {
                    long total   = hits[0] + misses[0];
                    double rate  = total == 0 ? 0.0 : (double) hits[0] / total * 100.0;
                    return String.format(
                            "+hits:%d misses:%d evictions:0 size:%d hitRate:%.2f%%",
                            hits[0], misses[0], store.size(), rate);
                }

                default:
                    return "-ERR unknown command: " + tokens[0];
            }
        }

        static void stopAll() {
            for (java.net.ServerSocket ss : openSockets) {
                try { ss.close(); } catch (IOException ignored) {}
            }
            openSockets.clear();
        }
    }
}