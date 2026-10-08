package com.cache.common.cluster;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ConsistentHashRing Tests")
class ConsistentHashRingTest {
    private static final int SAMPLE_SIZE = 10_000;

    private static final double DISTRIBUTION_TOLERANCE_PERCENT = 10.0;

    private ConsistentHashRing ring;

    @BeforeEach
    void setUp() {
        ring = new ConsistentHashRing();
    }

    @Test
    @DisplayName("isEmpty() returns true on new ring with no nodes")
    void testEmpty_newRingIsEmpty() {
        assertTrue(ring.isEmpty(), "Newly created ring should be empty");
    }

    @Test
    @DisplayName("getPhysicalNodeCount() returns 0 on empty ring")
    void testEmpty_physicalNodeCountIsZero() {
        assertEquals(0, ring.getPhysicalNodeCount());
    }

    @Test
    @DisplayName("getNode() throws on empty ring")
    void testEmpty_getNodeThrows() {
        assertThrows(IllegalStateException.class,
                () -> ring.getNode("any-key"),
                "getNode() on empty ring should throw IllegalStateException"
        );
    }

    @Test
    @DisplayName("getActiveNodes() returns empty list on empty ring")
    void testEmpty_getActiveNodesReturnsEmptyList() {
        List<CacheNode> nodes = ring.getActiveNodes();
        assertNotNull(nodes, "getActiveNodes() should never return null");
        assertTrue(nodes.isEmpty(), "No active nodes in empty ring");
    }

    @Test
    @DisplayName("Single node: getNode() always returns that node")
    void testSingleNode_allKeysRouteToIt() {
        CacheNode onlyNode = new CacheNode("solo", "localhost", 6379);
        ring.addNode(onlyNode);

        for (int i = 0; i < 100; i++) {
            CacheNode result = ring.getNode("key-" + i);
            assertEquals("solo", result.getId(),
                    "With one node, every key must route to it. Failed for key-" + i);
        }
    }

    @Test
    @DisplayName("Single node: physical count is 1, virtual count is 150")
    void testSingleNode_nodeCounts() {
        ring.addNode(new CacheNode("solo", "localhost", 6379));

        assertEquals(1, ring.getPhysicalNodeCount(), "Should have 1 physical node");
        assertEquals(150, ring.getVirtualNodeCount(),
                "Default 150 virtual nodes per physical node");
    }

    @Test
    @DisplayName("Single node: removing it empties the ring")
    void testSingleNode_removeEmptiesRing() {
        CacheNode node = new CacheNode("solo", "localhost", 6379);
        ring.addNode(node);
        ring.removeNode(node);

        assertTrue(ring.isEmpty(), "Ring should be empty after removing last node");
        assertEquals(0, ring.getPhysicalNodeCount());
    }

    @Test
    @DisplayName("getNode() is deterministic: same key always same node")
    void testDeterminism_sameKeyAlwaysSameNode() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));
        ring.addNode(new CacheNode("node-3", "localhost", 6381));

        for (int i = 0; i < 50; i++) {
            String key = "determinism-test-key-" + i;
            String firstResult = ring.getNode(key).getId();

            for (int repeat = 0; repeat < 4; repeat++) {
                String repeated = ring.getNode(key).getId();
                assertEquals(firstResult, repeated,
                        "getNode() returned different nodes for the same key [" + key + "]"
                );
            }
        }
    }

    @Test
    @DisplayName("getNode() produces same results after ring reconstruction")
    void testDeterminism_ringReconstructionGivesSameResults() {
        CacheNode n1 = new CacheNode("node-1", "localhost", 6379);
        CacheNode n2 = new CacheNode("node-2", "localhost", 6380);
        ring.addNode(n1);
        ring.addNode(n2);

        Map<String, String> routesBefore = new HashMap<>();
        for (int i = 0; i < 200; i++) {
            String key = "key-" + i;
            routesBefore.put(key, ring.getNode(key).getId());
        }

        ring = new ConsistentHashRing();
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));

        for (int i = 0; i < 200; i++) {
            String key = "key-" + i;
            String newRoute = ring.getNode(key).getId();
            assertEquals(routesBefore.get(key), newRoute,
                    "Route for [" + key + "] changed after ring reconstruction"
            );
        }
    }

    @Test
    @DisplayName("CORE: Remove 1 of 3 nodes: only its keys remap, others stay")
    void testRemoveNode_remapsOnlyAffectedKeys() {
        CacheNode node1 = new CacheNode("node-1", "localhost", 6379);
        CacheNode node2 = new CacheNode("node-2", "localhost", 6380);
        CacheNode node3 = new CacheNode("node-3", "localhost", 6381);
        ring.addNode(node1);
        ring.addNode(node2);
        ring.addNode(node3);

        Map<String, String> routesBefore = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            routesBefore.put(key, ring.getNode(key).getId());
        }

        long keysOnNode2Before = routesBefore.values().stream()
                .filter("node-2"::equals)
                .count();

        ring.removeNode(node2);

        Map<String, String> routesAfter = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            routesAfter.put(key, ring.getNode(key).getId());
        }

        long keysMoved = 0;
        long keysMovedFromNonNode2 = 0;

        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            String before = routesBefore.get(key);
            String after  = routesAfter.get(key);

            if (!before.equals(after)) {
                keysMoved++;

                if (!"node-2".equals(before)) {
                    keysMovedFromNonNode2++;
                }
            }
        }

        assertEquals(0, keysMovedFromNonNode2,
                String.format(
                        "CONSISTENCY VIOLATION: %d keys that were NOT on node-2 changed their " +
                                "node after removing node-2. Only node-2's keys should move.",
                        keysMovedFromNonNode2
                )
        );

        assertEquals(keysOnNode2Before, keysMoved,
                String.format(
                        "Expected exactly %d keys to move (all of node-2's keys), but %d moved.",
                        keysOnNode2Before, keysMoved
                )
        );

        double movedFraction = (double) keysMoved / SAMPLE_SIZE * 100.0;

        assertTrue(movedFraction >= 33.33 - DISTRIBUTION_TOLERANCE_PERCENT,
                String.format("Only %.2f%% of keys remapped: surprisingly low (expected ~33%%)", movedFraction));
        assertTrue(movedFraction <= 33.33 + DISTRIBUTION_TOLERANCE_PERCENT,
                String.format("%.2f%% of keys remapped: suspiciously high (expected ~33%%)", movedFraction));
    }

    @Test
    @DisplayName("Add 1 node to 2-node ring: only new node's keys remap")
    void testAddNode_remapsOnlyNewNodeKeys() {
        CacheNode node1 = new CacheNode("node-1", "localhost", 6379);
        CacheNode node2 = new CacheNode("node-2", "localhost", 6380);
        ring.addNode(node1);
        ring.addNode(node2);

        Map<String, String> routesBefore = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            routesBefore.put(key, ring.getNode(key).getId());
        }

        CacheNode node3 = new CacheNode("node-3", "localhost", 6381);
        ring.addNode(node3);

        long keysChangedToNode3     = 0;
        long keysChangedToNonNode3  = 0;

        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key    = "key-" + i;
            String before = routesBefore.get(key);
            String after  = ring.getNode(key).getId();

            if (!before.equals(after)) {
                if ("node-3".equals(after)) {
                    keysChangedToNode3++;
                } else {
                    keysChangedToNonNode3++;
                }
            }
        }

        assertEquals(0, keysChangedToNonNode3,
                String.format(
                        "CONSISTENCY VIOLATION: %d keys moved to an existing node when adding " +
                                "node-3. Keys should only move TO the new node, never between old nodes.",
                        keysChangedToNonNode3
                )
        );

        double newNodeFraction = (double) keysChangedToNode3 / SAMPLE_SIZE * 100.0;

        assertTrue(newNodeFraction > 20.0,
                String.format("New node got only %.2f%% of keys: possible ring imbalance", newNodeFraction));
        assertTrue(newNodeFraction < 50.0,
                String.format("New node got %.2f%% of keys: excessive, possible bug", newNodeFraction));
    }

    @ParameterizedTest(name = "{0} nodes: distribution within ±10% of ideal")
    @ValueSource(ints = {2, 3, 5})
    @DisplayName("Load distribution is within tolerance for different cluster sizes")
    void testDistribution_withinTolerance(int nodeCount) {
        for (int i = 1; i <= nodeCount; i++) {
            ring.addNode(new CacheNode("node-" + i, "localhost", 6378 + i));
        }

        Map<String, Integer> countPerNode = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String owner = ring.getNode("key-" + i).getId();
            countPerNode.merge(owner, 1, Integer::sum);
        }

        double idealPercent = 100.0 / nodeCount;

        for (Map.Entry<String, Integer> entry : countPerNode.entrySet()) {
            double actualPercent = (double) entry.getValue() / SAMPLE_SIZE * 100.0;

            assertTrue(actualPercent >= idealPercent - DISTRIBUTION_TOLERANCE_PERCENT,
                    String.format("Node [%s] owns only %.2f%% (ideal: %.2f%% ± %.1f%%)",
                            entry.getKey(), actualPercent, idealPercent, DISTRIBUTION_TOLERANCE_PERCENT));
            assertTrue(actualPercent <= idealPercent + DISTRIBUTION_TOLERANCE_PERCENT,
                    String.format("Node [%s] owns %.2f%% (ideal: %.2f%% ± %.1f%%)",
                            entry.getKey(), actualPercent, idealPercent, DISTRIBUTION_TOLERANCE_PERCENT));
        }
    }

    @Test
    @DisplayName("Node with weight=2 owns approximately twice the key space")
    void testWeightedNodes_doubleWeightNodeOwnsMoreKeys() {
        CacheNode lightNode = new CacheNode("light", "localhost", 6379, 1);
        CacheNode heavyNode = new CacheNode("heavy", "localhost", 6380, 2);
        ring.addNode(lightNode);
        ring.addNode(heavyNode);

        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String owner = ring.getNode("key-" + i).getId();
            counts.merge(owner, 1, Integer::sum);
        }

        int lightCount = counts.getOrDefault("light", 0);
        int heavyCount = counts.getOrDefault("heavy", 0);
        double ratio = (double) heavyCount / lightCount;

        assertTrue(ratio >= 1.5,
                String.format("Heavy node (weight=2) should own ~2x more keys. Actual ratio: %.2f", ratio));
        assertTrue(ratio <= 2.5,
                String.format("Heavy node (weight=2) owns too many keys. Actual ratio: %.2f", ratio));
    }

    @Test
    @DisplayName("Re-adding an existing node updates it (no duplicate virtual nodes)")
    void testAddNode_reAddingSameNodeDoesNotDuplicate() {
        CacheNode node = new CacheNode("node-1", "localhost", 6379);
        ring.addNode(node);
        ring.addNode(node);

        assertEquals(1, ring.getPhysicalNodeCount(),
                "Re-adding same node should not create a second physical node entry");
        assertEquals(150, ring.getVirtualNodeCount(),
                "Re-adding same node should not double virtual node count");
    }

    @Test
    @DisplayName("addNode() with null throws IllegalArgumentException")
    void testNullHandling_addNodeNull() {
        assertThrows(IllegalArgumentException.class, () -> ring.addNode(null));
    }

    @Test
    @DisplayName("removeNode() with null throws IllegalArgumentException")
    void testNullHandling_removeNodeNull() {
        assertThrows(IllegalArgumentException.class, () -> ring.removeNode(null));
    }

    @Test
    @DisplayName("getNode() with null key throws IllegalArgumentException")
    void testNullHandling_getNodeNullKey() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        assertThrows(IllegalArgumentException.class, () -> ring.getNode(null));
    }

    @Test
    @DisplayName("removeNode() on nonexistent node is a safe no-op")
    void testRemoveNonexistent_isNoOp() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        CacheNode ghost = new CacheNode("ghost-node", "localhost", 9999);
        assertDoesNotThrow(() -> ring.removeNode(ghost),
                "Removing a node not in the ring should not throw");
        assertEquals(1, ring.getPhysicalNodeCount(),
                "Removing nonexistent node should not affect existing nodes");
    }

    @Test
    @DisplayName("getReplicationNodes() returns correct number of distinct nodes")
    void testReplication_returnsCorrectCount() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));
        ring.addNode(new CacheNode("node-3", "localhost", 6381));

        List<CacheNode> replicas = ring.getReplicationNodes("user:123", 3);

        assertEquals(3, replicas.size(),
                "getReplicationNodes(3) should return exactly 3 nodes");

        Set<String> ids = replicas.stream()
                .map(CacheNode::getId)
                .collect(Collectors.toSet());
        assertEquals(3, ids.size(),
                "All replication nodes must be distinct physical nodes");
    }

    @Test
    @DisplayName("getReplicationNodes() first node is the primary (same as getNode)")
    void testReplication_firstNodeIsPrimary() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));
        ring.addNode(new CacheNode("node-3", "localhost", 6381));

        String key = "session:abc";
        CacheNode primary     = ring.getNode(key);
        List<CacheNode> repls = ring.getReplicationNodes(key, 2);

        assertEquals(primary.getId(), repls.get(0).getId(),
                "First node in replication list must be the primary (same as getNode())");
    }

    @Test
    @DisplayName("getReplicationNodes() with factor > node count throws")
    void testReplication_factorExceedsNodeCount_throws() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));

        assertThrows(IllegalArgumentException.class,
                () -> ring.getReplicationNodes("key", 3));
    }

    @Test
    @DisplayName("getKeyDistribution() sums to 100% across all nodes")
    void testKeyDistribution_sumsTo100() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));
        ring.addNode(new CacheNode("node-3", "localhost", 6381));

        Map<String, Double> distribution = ring.getKeyDistribution();

        double total = distribution.values().stream()
                .mapToDouble(Double::doubleValue)
                .sum();

        assertEquals(100.0, total, 0.01,
                "Key distribution percentages should sum to ~100%");
    }

    @Test
    @DisplayName("getKeyDistribution() returns empty map for empty ring")
    void testKeyDistribution_emptyRingReturnsEmptyMap() {
        Map<String, Double> distribution = ring.getKeyDistribution();
        assertNotNull(distribution);
        assertTrue(distribution.isEmpty(),
                "Empty ring should return empty distribution map");
    }

    @Test
    @DisplayName("Replication nodes are distinct, active and start with the owner")
    void replicationNodesAreDistinctAndOrdered() {
        ConsistentHashRing r = new ConsistentHashRing();
        for (int i = 1; i <= 4; i++) {
            r.addNode(new CacheNode("n" + i, "localhost", 7000 + i));
        }
        r.addNode(new CacheNode("n4", "localhost", 7004).withStatus(CacheNode.Status.FAILED));

        List<CacheNode> replicas = r.getReplicationNodes("some-key", 3);

        assertEquals(3, replicas.size());
        assertEquals(r.getNode("some-key"), replicas.get(0));
        assertEquals(3, replicas.stream().map(CacheNode::getId).distinct().count());
        assertTrue(replicas.stream().allMatch(CacheNode::isActive));
    }
}
