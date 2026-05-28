package com.cache.common.cluster;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * NodeRouter — server-side routing policy layer.
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * RESPONSIBILITY SPLIT
 * ═══════════════════════════════════════════════════════════════════════════
 *
 *   ConsistentHashRing (cache-common): pure algorithm — "which node owns key X?"
 *   NodeRouter         (cache-server): policy layer   — "should I handle this
 *                                       key, or redirect to the correct node?"
 *
 * NodeRouter adds identity awareness: it knows which node "this" server is,
 * so it can compare the ring's answer against its own identity and decide
 * LOCAL vs REDIRECT.
 *
 * CacheServerHandler calls NodeRouter.route(key) for every incoming request:
 *   - RoutingDecision.isLocal() == true  → handle with local cache engine
 *   - RoutingDecision.isLocal() == false → emit REDIRECT to decision.getTargetNode()
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * ROUTING DECISION MODEL
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * We use a RoutingDecision value object instead of a plain boolean because:
 *   - On redirect, the caller needs the target node's address (host:port).
 *   - Returning a boolean would require a second ring lookup for the address.
 *   - RoutingDecision carries both pieces of information in one allocation.
 *
 * This is the same pattern as Redis Cluster's MOVED response:
 *   it doesn't just say "wrong server" — it says "go to server X at host:port".
 *
 * ═══════════════════════════════════════════════════════════════════════════
 * REPLICATION
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * replicationFactor=1  → no replication (single copy per key).
 * replicationFactor=2  → one primary + one replica.
 *
 * isReplicaForKey(key) answers: "am I a backup holder for this key?"
 * Used during primary failure: the replica node takes over read requests.
 *
 * getReplicationTargets(key) returns [primary, replica1, ...].
 * CacheServerHandler uses this on PUT to fan-out writes to replica nodes.
 *
 * INTERVIEW TALKING POINT:
 *   "NodeRouter is the server-side identity layer on top of ConsistentHashRing.
 *    It separates routing algorithm (which node?) from routing policy (am I that node?).
 *    This mirrors how Redis Cluster separates its hash slot table from its
 *    MOVED/ASK redirect logic."
 */
public class NodeRouter {

    private static final Logger log = Logger.getLogger(NodeRouter.class.getName());

    // -------------------------------------------------------------------------
    // RoutingDecision — value object for routing results
    // -------------------------------------------------------------------------

    /**
     * The result of a routing decision for a given key.
     *
     * isLocal() == true  → process request using the local cache engine.
     * isLocal() == false → redirect to getTargetNode().getAddress().
     *
     * getTargetNode() is always non-null regardless of local/redirect:
     *   - Local:    target is the local node itself.
     *   - Redirect: target is the remote node to redirect to.
     */
    public static final class RoutingDecision {

        private final boolean   local;
        private final CacheNode targetNode;
        private final String    reason;

        private RoutingDecision(boolean local, CacheNode targetNode, String reason) {
            this.local      = local;
            this.targetNode = targetNode;
            this.reason     = reason;
        }

        static RoutingDecision local(CacheNode localNode) {
            return new RoutingDecision(true, localNode,
                    "key belongs to local node [" + localNode.getId() + "]");
        }

        static RoutingDecision redirect(CacheNode targetNode, String key) {
            return new RoutingDecision(false, targetNode,
                    "redirect key [" + key + "] to node ["
                            + targetNode.getId() + "] at " + targetNode.getAddress());
        }

        /** @return true if the key belongs to this node (handle locally). */
        public boolean isLocal()             { return local;      }

        /**
         * @return The owning node. Local → this node. Redirect → remote node.
         *         Never null.
         */
        public CacheNode getTargetNode()     { return targetNode; }

        /** @return Human-readable reason, useful for logs and debugging. */
        public String getReason()            { return reason;     }

        @Override
        public String toString() {
            return String.format("RoutingDecision{local=%b, target=%s, reason='%s'}",
                    local, targetNode.getAddress(), reason);
        }
    }

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    /** The ring — delegates all hash-based routing to this. */
    private final ConsistentHashRing ring;

    /**
     * This server's own identity in the cluster.
     * MUST match the CacheNode added to the ring (same id field).
     * ID mismatch → every key appears remote → server handles nothing.
     */
    private final CacheNode localNode;

    /**
     * How many copies of each key exist.
     * 1 = no replication. 2 = primary + one replica.
     * Must be <= number of ACTIVE physical nodes.
     */
    private final int replicationFactor;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Creates a NodeRouter with no replication (factor=1). */
    public NodeRouter(ConsistentHashRing ring, CacheNode localNode) {
        this(ring, localNode, 1);
    }

    /** Creates a NodeRouter with a custom replication factor. */
    public NodeRouter(ConsistentHashRing ring, CacheNode localNode, int replicationFactor) {
        if (ring == null)      throw new IllegalArgumentException("Ring cannot be null");
        if (localNode == null) throw new IllegalArgumentException("LocalNode cannot be null");
        if (replicationFactor < 1)
            throw new IllegalArgumentException(
                    "Replication factor must be >= 1, got: " + replicationFactor);

        this.ring              = ring;
        this.localNode         = localNode;
        this.replicationFactor = replicationFactor;

        // Warn if localNode is not in the ring — common misconfiguration.
        boolean foundInRing = ring.getActiveNodes().stream()
                .anyMatch(n -> n.getId().equals(localNode.getId()));

        if (!foundInRing) {
            log.warning("NodeRouter: localNode [" + localNode.getId() + "] is NOT in the ring. "
                    + "All routing decisions will redirect. Call ring.addNode(localNode) first.");
        } else {
            log.info("NodeRouter initialized: localNode=[" + localNode.getId()
                    + "], clusterSize=" + ring.getPhysicalNodeCount()
                    + ", replicationFactor=" + replicationFactor);
        }
    }

    // -------------------------------------------------------------------------
    // Core routing
    // -------------------------------------------------------------------------

    /**
     * Routes a key — the primary method called by CacheServerHandler.
     *
     * ALGORITHM:
     *   1. ring.getNode(key)                        → targetNode
     *   2. targetNode.getId().equals(localNode.getId())
     *      → true:  RoutingDecision.local(localNode)
     *      → false: RoutingDecision.redirect(targetNode, key)
     *
     * WHY ID COMPARISON (not object equality ==)?
     * The ring may hold a different CacheNode instance than localNode
     * (e.g., ring was rebuilt). Comparing by id string is always correct.
     *
     * @param key The cache key. Cannot be null.
     * @return RoutingDecision — LOCAL or REDIRECT with target address.
     * @throws IllegalStateException    if ring is empty.
     * @throws IllegalArgumentException if key is null.
     */
    public RoutingDecision route(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        if (ring.isEmpty()) throw new IllegalStateException(
                "Cannot route key [" + key + "]: ring is empty");

        CacheNode targetNode = ring.getNode(key);
        boolean   isLocal    = targetNode.getId().equals(localNode.getId());

        if (isLocal) {
            log.fine(() -> "LOCAL route: key [" + key + "] → [" + localNode.getId() + "]");
            return RoutingDecision.local(localNode);
        } else {
            log.fine(() -> "REDIRECT: key [" + key + "] → ["
                    + targetNode.getId() + "] at " + targetNode.getAddress());
            return RoutingDecision.redirect(targetNode, key);
        }
    }

    /**
     * Convenience method — true if this node is the primary owner of the key.
     * Equivalent to route(key).isLocal() but avoids allocating a RoutingDecision.
     *
     * @param key The cache key.
     * @return true if this node owns the key.
     */
    public boolean isLocalKey(String key) {
        if (key == null || ring.isEmpty()) return false;
        return ring.getNode(key).getId().equals(localNode.getId());
    }

    /**
     * Returns the node that owns the given key (primary owner).
     *
     * @param key The cache key.
     * @return Owning CacheNode.
     * @throws IllegalStateException if ring is empty.
     */
    public CacheNode getOwnerNode(String key) {
        if (ring.isEmpty()) throw new IllegalStateException(
                "Ring is empty — no node owns key [" + key + "]");
        return ring.getNode(key);
    }

    // -------------------------------------------------------------------------
    // Replication
    // -------------------------------------------------------------------------

    /**
     * Returns true if this node is a REPLICA (non-primary) holder of the key.
     *
     * A replica is responsible for:
     *   - Storing a copy of the key (written during PUT fan-out).
     *   - Serving reads if the primary node fails (if router is configured to do so).
     *
     * Returns false if replicationFactor == 1 (no replication configured).
     *
     * @param key The cache key.
     * @return true if this node holds a replica of the key.
     */
    public boolean isReplicaForKey(String key) {
        if (replicationFactor <= 1) return false;
        if (ring.getPhysicalNodeCount() < replicationFactor) return false;

        List<CacheNode> targets = ring.getReplicationNodes(key, replicationFactor);
        // Skip index 0 (primary) — check replica positions (1+)
        for (int i = 1; i < targets.size(); i++) {
            if (targets.get(i).getId().equals(localNode.getId())) return true;
        }
        return false;
    }

    /**
     * Returns [primary, replica1, replica2, ...] for this key.
     * CacheServerHandler uses this on PUT to fan-out writes to all replica nodes.
     *
     * @param key The cache key.
     * @return Ordered list of replication target nodes.
     */
    public List<CacheNode> getReplicationTargets(String key) {
        int effectiveFactor = Math.min(replicationFactor, ring.getPhysicalNodeCount());
        return ring.getReplicationNodes(key, effectiveFactor);
    }

    // -------------------------------------------------------------------------
    // Cluster membership
    // -------------------------------------------------------------------------

    /** Adds a node to the ring. ~1/N keys remap to the new node. */
    public void addNode(CacheNode node) {
        if (node == null) throw new IllegalArgumentException("Node cannot be null");
        ring.addNode(node);
        log.info("NodeRouter: added node [" + node.getId() + "] at " + node.getAddress()
                + ". Ring size: " + ring.getPhysicalNodeCount());
    }

    /** Removes a node from the ring. Its keys remap to the next clockwise node. */
    public void removeNode(CacheNode node) {
        if (node == null) throw new IllegalArgumentException("Node cannot be null");
        ring.removeNode(node);
        log.info("NodeRouter: removed node [" + node.getId()
                + "]. Ring size: " + ring.getPhysicalNodeCount());
    }

    /**
     * Marks a node as FAILED without removing it from the registry.
     * Traffic routes around it. Node can recover and be re-added later.
     *
     * @param nodeId ID of the failed node.
     */
    public void markNodeFailed(String nodeId) {
        ring.getActiveNodes().stream()
                .filter(n -> n.getId().equals(nodeId))
                .findFirst()
                .ifPresent(node -> {
                    ring.removeNode(node);
                    ring.addNode(node.withStatus(CacheNode.Status.FAILED));
                    log.warning("NodeRouter: marked node [" + nodeId + "] as FAILED.");
                });
    }

    // -------------------------------------------------------------------------
    // Inspection
    // -------------------------------------------------------------------------

    public CacheNode         getLocalNode()        { return localNode;                    }
    public int               getReplicationFactor() { return replicationFactor;            }
    public List<CacheNode>   getClusterNodes()     { return ring.getActiveNodes();         }
    public int               getClusterSize()      { return ring.getPhysicalNodeCount();  }
    public Map<String,Double>getKeyDistribution()  { return ring.getKeyDistribution();    }

    /** Returns a human-readable cluster status string for the STATS command. */
    public String getClusterStatus() {
        return String.format(
                "localNode=%s at %s | clusterSize=%d | vnodes=%d | replication=%d | dist=%s",
                localNode.getId(), localNode.getAddress(),
                ring.getPhysicalNodeCount(), ring.getVirtualNodeCount(),
                replicationFactor, ring.getKeyDistribution());
    }

    @Override
    public String toString() {
        return String.format("NodeRouter{localNode=%s, clusterSize=%d, replicationFactor=%d}",
                localNode.getId(), ring.getPhysicalNodeCount(), replicationFactor);
    }
}