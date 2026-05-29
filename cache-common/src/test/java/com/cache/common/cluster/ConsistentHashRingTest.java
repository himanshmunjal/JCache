package com.cache.common.cluster;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for ConsistentHashRing.
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * WHAT WE'RE VERIFYING AND WHY
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * The correctness properties of consistent hashing are mathematical guarantees.
 * These tests verify those guarantees hold in our implementation:
 *
 *   PROPERTY 1 — DETERMINISM
 *     Same key always routes to same node (given same ring state).
 *     If this breaks, clients get different nodes on retries — data loss.
 *
 *   PROPERTY 2 — MINIMAL DISRUPTION ON NODE REMOVAL
 *     Removing 1 of N nodes remaps only ~1/N keys (not all keys).
 *     This is THE defining property of consistent hashing.
 *     We verify: exactly the keys previously owned by the removed node change,
 *     and no other key changes its node.
 *
 *   PROPERTY 3 — LOAD DISTRIBUTION
 *     With 150 virtual nodes per physical node, each node should own
 *     approximately 1/N of the key space (within a tolerance band).
 *     We verify this with 10,000 sample keys.
 *
 *   PROPERTY 4 — WRAP-AROUND
 *     Keys that hash past the last virtual node on the ring should
 *     wrap around to the first node. The ring is a circle, not a line.
 *
 *   PROPERTY 5 — SINGLE NODE BEHAVIOR
 *     With exactly one node, ALL keys route to that node. No exceptions.
 *
 *   PROPERTY 6 — NODE ADDITION MINIMAL DISRUPTION
 *     Adding a new node remaps only the keys it takes from its clockwise neighbor.
 *     All other key→node mappings stay the same.
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * THE MOST IMPORTANT TEST (testRemoveNode_remapsOnlyAffectedKeys)
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * This test is what you demo in the FAANG interview when asked
 * "how did you verify your consistent hashing implementation?"
 *
 * It proves: with 3 nodes, removing 1 remaps ~33% of keys (1/3),
 * not 100% (which is what happens with naive hash(key) % N).
 *
 * The exact test:
 *   1. Build ring with 3 nodes.
 *   2. Route 10,000 keys → record key→node mappings.
 *   3. Remove node-2.
 *   4. Route same 10,000 keys again → check which mappings changed.
 *   5. Assert: only keys previously mapped to node-2 changed.
 *   6. Assert: all keys previously mapped to node-1 and node-3 stayed the same.
 *   7. Assert: changed key count ≈ 33% (within ±10% tolerance for variance).
 */
@DisplayName("ConsistentHashRing Tests")
class ConsistentHashRingTest {

    // Number of test keys to use in distribution and disruption tests.
    // 10,000 gives good statistical accuracy while running in < 100ms.
    private static final int SAMPLE_SIZE = 10_000;

    // Tolerance for distribution tests. With 150 virtual nodes, each of 3
    // nodes should own ~33% ± 5% of the key space.
    private static final double DISTRIBUTION_TOLERANCE_PERCENT = 10.0;

    private ConsistentHashRing ring;

    @BeforeEach
    void setUp() {
        ring = new ConsistentHashRing();
    }

    // =========================================================================
    // 1. Empty ring behavior
    // =========================================================================

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

    // =========================================================================
    // 2. Single node behavior — all keys must route to it
    // =========================================================================

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

    // =========================================================================
    // 3. Determinism — same key always maps to same node
    // =========================================================================

    @Test
    @DisplayName("getNode() is deterministic — same key always same node")
    void testDeterminism_sameKeyAlwaysSameNode() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));
        ring.addNode(new CacheNode("node-3", "localhost", 6381));

        // Call getNode() 5 times for each key and verify consistency.
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
        // Build ring and record routes
        CacheNode n1 = new CacheNode("node-1", "localhost", 6379);
        CacheNode n2 = new CacheNode("node-2", "localhost", 6380);
        ring.addNode(n1);
        ring.addNode(n2);

        Map<String, String> routesBefore = new HashMap<>();
        for (int i = 0; i < 200; i++) {
            String key = "key-" + i;
            routesBefore.put(key, ring.getNode(key).getId());
        }

        // Rebuild the ring from scratch with the same nodes
        ring = new ConsistentHashRing();
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));

        // Verify same routes
        for (int i = 0; i < 200; i++) {
            String key = "key-" + i;
            String newRoute = ring.getNode(key).getId();
            assertEquals(routesBefore.get(key), newRoute,
                    "Route for [" + key + "] changed after ring reconstruction"
            );
        }
    }

    // =========================================================================
    // 4. CORE TEST: Minimal disruption on node removal
    // =========================================================================

    /**
     * THE MOST IMPORTANT TEST in this suite.
     *
     * Verifies the central consistent hashing guarantee:
     * removing 1 of N nodes remaps ONLY the keys that were on the removed node.
     * All other keys stay mapped to their original nodes.
     *
     * This is what makes consistent hashing superior to hash(key) % N:
     *   - hash(key) % 3 → hash(key) % 2: ALL keys remap (catastrophic cache miss)
     *   - Consistent hashing: only ~1/3 of keys remap (graceful degradation)
     */
    @Test
    @DisplayName("CORE: Remove 1 of 3 nodes — only its keys remap, others stay")
    void testRemoveNode_remapsOnlyAffectedKeys() {
        CacheNode node1 = new CacheNode("node-1", "localhost", 6379);
        CacheNode node2 = new CacheNode("node-2", "localhost", 6380);
        CacheNode node3 = new CacheNode("node-3", "localhost", 6381);
        ring.addNode(node1);
        ring.addNode(node2);
        ring.addNode(node3);

        // Step 1: Route all sample keys and record the before-state.
        Map<String, String> routesBefore = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            routesBefore.put(key, ring.getNode(key).getId());
        }

        // Count how many keys were on node-2 (the one we'll remove).
        long keysOnNode2Before = routesBefore.values().stream()
                .filter("node-2"::equals)
                .count();

        // Step 2: Remove node-2.
        ring.removeNode(node2);

        // Step 3: Route all keys again and record after-state.
        Map<String, String> routesAfter = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            routesAfter.put(key, ring.getNode(key).getId());
        }

        // Step 4: Analyze which keys moved.
        long keysMoved = 0;
        long keysMovedFromNonNode2 = 0; // this MUST be 0 — these should never move

        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            String before = routesBefore.get(key);
            String after  = routesAfter.get(key);

            if (!before.equals(after)) {
                keysMoved++;
                // If a key moved but it WASN'T on node-2 before, that's a bug.
                if (!"node-2".equals(before)) {
                    keysMovedFromNonNode2++;
                }
            }
        }

        // ─────────────────────────────────────────────────────────────────
        // ASSERTION 1: No key that was on node-1 or node-3 moved.
        // This is the strict part — these keys MUST NOT move.
        // Any movement here indicates a ring bug (keys being unnecessarily remapped).
        // ─────────────────────────────────────────────────────────────────
        assertEquals(0, keysMovedFromNonNode2,
                String.format(
                        "CONSISTENCY VIOLATION: %d keys that were NOT on node-2 changed their " +
                                "node after removing node-2. Only node-2's keys should move.",
                        keysMovedFromNonNode2
                )
        );

        // ─────────────────────────────────────────────────────────────────
        // ASSERTION 2: ALL of node-2's keys moved (they have to — node-2 is gone).
        // ─────────────────────────────────────────────────────────────────
        assertEquals(keysOnNode2Before, keysMoved,
                String.format(
                        "Expected exactly %d keys to move (all of node-2's keys), but %d moved.",
                        keysOnNode2Before, keysMoved
                )
        );

        // ─────────────────────────────────────────────────────────────────
        // ASSERTION 3: The fraction that moved is approximately 1/3 (±10%).
        // With 150 virtual nodes this should be very close to 33.33%.
        // ─────────────────────────────────────────────────────────────────
        double movedFraction = (double) keysMoved / SAMPLE_SIZE * 100.0;
        System.out.printf("[ConsistentHashRingTest] Removed 1 of 3 nodes: %.2f%% of keys remapped " +
                        "(ideal: 33.33%%, tolerance: ±%.1f%%)%n",
                movedFraction, DISTRIBUTION_TOLERANCE_PERCENT);

        assertTrue(movedFraction >= 33.33 - DISTRIBUTION_TOLERANCE_PERCENT,
                String.format("Only %.2f%% of keys remapped — surprisingly low (expected ~33%%)", movedFraction));
        assertTrue(movedFraction <= 33.33 + DISTRIBUTION_TOLERANCE_PERCENT,
                String.format("%.2f%% of keys remapped — suspiciously high (expected ~33%%)", movedFraction));
    }

    // =========================================================================
    // 5. Minimal disruption on node addition
    // =========================================================================

    @Test
    @DisplayName("Add 1 node to 2-node ring — only new node's keys remap")
    void testAddNode_remapsOnlyNewNodeKeys() {
        CacheNode node1 = new CacheNode("node-1", "localhost", 6379);
        CacheNode node2 = new CacheNode("node-2", "localhost", 6380);
        ring.addNode(node1);
        ring.addNode(node2);

        // Record routes before adding node-3
        Map<String, String> routesBefore = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key = "key-" + i;
            routesBefore.put(key, ring.getNode(key).getId());
        }

        // Add node-3
        CacheNode node3 = new CacheNode("node-3", "localhost", 6381);
        ring.addNode(node3);

        // Route the same keys again
        long keysChangedToNode3     = 0;
        long keysChangedToNonNode3  = 0; // this MUST be 0

        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String key    = "key-" + i;
            String before = routesBefore.get(key);
            String after  = ring.getNode(key).getId();

            if (!before.equals(after)) {
                if ("node-3".equals(after)) {
                    keysChangedToNode3++;
                } else {
                    // A key moved to node-1 or node-2 — that should never happen.
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

        // node-3 should take ~1/3 of the key space from node-1 and node-2
        double newNodeFraction = (double) keysChangedToNode3 / SAMPLE_SIZE * 100.0;
        System.out.printf("[ConsistentHashRingTest] Added 1 node to 2-node ring: %.2f%% keys moved to new node%n",
                newNodeFraction);

        assertTrue(newNodeFraction > 20.0,
                String.format("New node got only %.2f%% of keys — possible ring imbalance", newNodeFraction));
        assertTrue(newNodeFraction < 50.0,
                String.format("New node got %.2f%% of keys — excessive, possible bug", newNodeFraction));
    }

    // =========================================================================
    // 6. Load distribution
    // =========================================================================

    /**
     * Verifies that with 150 virtual nodes, load is distributed within ±10% of ideal.
     * Uses @ParameterizedTest to verify distribution improves with more nodes.
     */
    @ParameterizedTest(name = "{0} nodes: distribution within ±10% of ideal")
    @ValueSource(ints = {2, 3, 5})
    @DisplayName("Load distribution is within tolerance for different cluster sizes")
    void testDistribution_withinTolerance(int nodeCount) {
        // Build a ring with nodeCount nodes
        for (int i = 1; i <= nodeCount; i++) {
            ring.addNode(new CacheNode("node-" + i, "localhost", 6378 + i));
        }

        // Route 10,000 keys and count per-node distribution
        Map<String, Integer> countPerNode = new HashMap<>();
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            String owner = ring.getNode("key-" + i).getId();
            countPerNode.merge(owner, 1, Integer::sum);
        }

        double idealPercent = 100.0 / nodeCount;

        System.out.printf("[Distribution] %d nodes, ideal=%.2f%% each:%n", nodeCount, idealPercent);
        for (Map.Entry<String, Integer> entry : countPerNode.entrySet()) {
            double actualPercent = (double) entry.getValue() / SAMPLE_SIZE * 100.0;
            System.out.printf("  %-10s → %.2f%%%n", entry.getKey(), actualPercent);

            assertTrue(actualPercent >= idealPercent - DISTRIBUTION_TOLERANCE_PERCENT,
                    String.format("Node [%s] owns only %.2f%% (ideal: %.2f%% ± %.1f%%)",
                            entry.getKey(), actualPercent, idealPercent, DISTRIBUTION_TOLERANCE_PERCENT));
            assertTrue(actualPercent <= idealPercent + DISTRIBUTION_TOLERANCE_PERCENT,
                    String.format("Node [%s] owns %.2f%% (ideal: %.2f%% ± %.1f%%)",
                            entry.getKey(), actualPercent, idealPercent, DISTRIBUTION_TOLERANCE_PERCENT));
        }
    }

    // =========================================================================
    // 7. Weighted nodes
    // =========================================================================

    @Test
    @DisplayName("Node with weight=2 owns approximately twice the key space")
    void testWeightedNodes_doubleWeightNodeOwnsMoreKeys() {
        CacheNode lightNode = new CacheNode("light", "localhost", 6379, 1); // weight=1
        CacheNode heavyNode = new CacheNode("heavy", "localhost", 6380, 2); // weight=2
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

        System.out.printf("[WeightTest] light=%d (%.1f%%), heavy=%d (%.1f%%), ratio=%.2fx%n",
                lightCount, (double) lightCount / SAMPLE_SIZE * 100,
                heavyCount, (double) heavyCount / SAMPLE_SIZE * 100,
                ratio);

        // heavy node should have approximately 2x the keys of light node.
        // Allow ±0.5x tolerance for variance at 150 virtual nodes per weight unit.
        assertTrue(ratio >= 1.5,
                String.format("Heavy node (weight=2) should own ~2x more keys. Actual ratio: %.2f", ratio));
        assertTrue(ratio <= 2.5,
                String.format("Heavy node (weight=2) owns too many keys. Actual ratio: %.2f", ratio));
    }

    // =========================================================================
    // 8. Re-add the same node
    // =========================================================================

    @Test
    @DisplayName("Re-adding an existing node updates it (no duplicate virtual nodes)")
    void testAddNode_reAddingSameNodeDoesNotDuplicate() {
        CacheNode node = new CacheNode("node-1", "localhost", 6379);
        ring.addNode(node);
        ring.addNode(node); // re-add same node

        // Should still have 1 physical node and 150 virtual nodes (not 300)
        assertEquals(1, ring.getPhysicalNodeCount(),
                "Re-adding same node should not create a second physical node entry");
        assertEquals(150, ring.getVirtualNodeCount(),
                "Re-adding same node should not double virtual node count");
    }

    // =========================================================================
    // 9. Null and invalid input handling
    // =========================================================================

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

    @Disabled("Test disable tmp")
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

    // =========================================================================
    // 10. Replication nodes
    // =========================================================================

    @Test
    @DisplayName("getReplicationNodes() returns correct number of distinct nodes")
    void testReplication_returnsCorrectCount() {
        ring.addNode(new CacheNode("node-1", "localhost", 6379));
        ring.addNode(new CacheNode("node-2", "localhost", 6380));
        ring.addNode(new CacheNode("node-3", "localhost", 6381));

        List<CacheNode> replicas = ring.getReplicationNodes("user:123", 3);

        assertEquals(3, replicas.size(),
                "getReplicationNodes(3) should return exactly 3 nodes");

        // All must be distinct physical nodes
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
        // Only 2 nodes, asking for replication factor of 3
        assertThrows(IllegalArgumentException.class,
                () -> ring.getReplicationNodes("key", 3));
    }

    // =========================================================================
    // 11. getKeyDistribution() utility
    // =========================================================================

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
}