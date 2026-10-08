package com.cache.common.cluster;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Server-side view of the ring: tells a node whether it owns a key or which
 * peer does.
 */
public class NodeRouter {

    private static final Logger log = Logger.getLogger(NodeRouter.class.getName());

    /**
     * Where a key should be served.
     *
     * @param local      whether the local node owns the key
     * @param targetNode the owning node
     */
    public record RoutingDecision(boolean local, CacheNode targetNode) {

        /** @return whether the local node owns the key */
        public boolean isLocal() {
            return local;
        }

        /** @return the owning node */
        public CacheNode getTargetNode() {
            return targetNode;
        }
    }

    private final ConsistentHashRing ring;
    private final CacheNode localNode;
    private final int replicationFactor;

    /**
     * Creates a router without replication.
     *
     * @param ring      the cluster ring
     * @param localNode this server's node
     */
    public NodeRouter(ConsistentHashRing ring, CacheNode localNode) {
        this(ring, localNode, 1);
    }

    /**
     * Creates a router.
     *
     * @param ring              the cluster ring
     * @param localNode         this server's node
     * @param replicationFactor copies kept of each key, including the owner's
     */
    public NodeRouter(ConsistentHashRing ring, CacheNode localNode, int replicationFactor) {
        if (ring == null) {
            throw new IllegalArgumentException("Ring cannot be null");
        }
        if (localNode == null) {
            throw new IllegalArgumentException("LocalNode cannot be null");
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication factor must be >= 1, got: " + replicationFactor);
        }
        this.ring = ring;
        this.localNode = localNode;
        this.replicationFactor = replicationFactor;
        if (!ring.containsNode(localNode.getId())) {
            log.warning("Local node " + localNode.getId() + " is not on the ring; every key will be redirected");
        }
    }

    /**
     * Decides where {@code key} should be served.
     *
     * @param key the key
     * @return the routing decision
     */
    public RoutingDecision route(String key) {
        CacheNode owner = getOwnerNode(key);
        return new RoutingDecision(owner.getId().equals(localNode.getId()), owner);
    }

    /**
     * @param key the key
     * @return whether this node owns the key
     */
    public boolean isLocalKey(String key) {
        return key != null && !ring.isEmpty() && ring.getNode(key).getId().equals(localNode.getId());
    }

    /**
     * @param key the key
     * @return the node that owns the key
     */
    public CacheNode getOwnerNode(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (ring.isEmpty()) {
            throw new IllegalStateException("Ring is empty; no node owns key " + key);
        }
        return ring.getNode(key);
    }

    /**
     * @param key the key
     * @return whether this node holds a replica (not the primary copy) of the key
     */
    public boolean isReplicaForKey(String key) {
        if (replicationFactor <= 1 || ring.getActiveNodes().size() < replicationFactor) {
            return false;
        }
        List<CacheNode> targets = ring.getReplicationNodes(key, replicationFactor);
        return targets.stream().skip(1).anyMatch(n -> n.getId().equals(localNode.getId()));
    }

    /**
     * @param key the key
     * @return the owner followed by the replicas, capped at the number of active nodes
     */
    public List<CacheNode> getReplicationTargets(String key) {
        int factor = Math.min(replicationFactor, ring.getActiveNodes().size());
        return ring.getReplicationNodes(key, factor);
    }

    /**
     * Adds a node to the ring.
     *
     * @param node the node
     */
    public void addNode(CacheNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Node cannot be null");
        }
        ring.addNode(node);
    }

    /**
     * Removes a node from the ring.
     *
     * @param node the node
     */
    public void removeNode(CacheNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Node cannot be null");
        }
        ring.removeNode(node);
    }

    /**
     * Marks a node as failed. It stays on the ring but stops receiving keys.
     *
     * @param nodeId id of the failed node
     */
    public void markNodeFailed(String nodeId) {
        ring.getNodes().stream()
                .filter(n -> n.getId().equals(nodeId) && n.isActive())
                .findFirst()
                .ifPresent(n -> {
                    ring.addNode(n.withStatus(CacheNode.Status.FAILED));
                    log.warning("Marked node " + nodeId + " as FAILED");
                });
    }

    /** @return this server's node */
    public CacheNode getLocalNode() {
        return localNode;
    }

    /** @return the configured replication factor */
    public int getReplicationFactor() {
        return replicationFactor;
    }

    /** @return the active nodes */
    public List<CacheNode> getClusterNodes() {
        return ring.getActiveNodes();
    }

    /** @return number of nodes on the ring */
    public int getClusterSize() {
        return ring.getPhysicalNodeCount();
    }

    /** @return each node's share of the key space, as a percentage */
    public Map<String, Double> getKeyDistribution() {
        return ring.getKeyDistribution();
    }

    @Override
    public String toString() {
        return String.format("NodeRouter{localNode=%s, clusterSize=%d, replicationFactor=%d}",
                localNode.getId(), getClusterSize(), replicationFactor);
    }
}
