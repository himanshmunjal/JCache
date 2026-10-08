package com.cache.common.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Consistent hash ring that maps keys to {@link CacheNode}s.
 *
 * <p>Each node is placed on the ring {@value #VIRTUAL_NODES_PER_NODE} times
 * (times its weight) at positions derived from an MD5 hash. A key belongs to
 * the first active node at or after its own hash, wrapping around at the end.
 * Adding or removing one of N nodes therefore moves only about 1/N of the
 * keys, and the virtual nodes keep the load roughly even.
 *
 * <p>Lookups are lock-free; membership changes are serialised.
 */
public class ConsistentHashRing {

    /** Virtual nodes per unit of weight. */
    public static final int VIRTUAL_NODES_PER_NODE = 150;

    private static final ThreadLocal<MessageDigest> MD5 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 is required by every Java platform", e);
        }
    });

    private final ConcurrentSkipListMap<Integer, CacheNode> ring = new ConcurrentSkipListMap<>();
    private final Map<String, CacheNode> nodeById = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Creates an empty ring. */
    public ConsistentHashRing() {
    }

    /**
     * Creates a ring containing the given nodes.
     *
     * @param initialNodes nodes to add
     */
    public ConsistentHashRing(Collection<CacheNode> initialNodes) {
        initialNodes.forEach(this::addNode);
    }

    /**
     * Adds a node, replacing any existing node with the same id.
     *
     * @param node the node to add
     */
    public void addNode(CacheNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Cannot add null node to ring");
        }
        lock.writeLock().lock();
        try {
            if (nodeById.containsKey(node.getId())) {
                removeInternal(node.getId());
            }
            nodeById.put(node.getId(), node);
            int vnodes = VIRTUAL_NODES_PER_NODE * node.getWeight();
            for (int i = 0; i < vnodes; i++) {
                ring.put(hash(node.getId() + ":" + i), node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a node. Unknown nodes are ignored.
     *
     * @param node the node to remove
     */
    public void removeNode(CacheNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Cannot remove null node from ring");
        }
        lock.writeLock().lock();
        try {
            removeInternal(node.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the active node that owns {@code key}.
     *
     * @param key the key to route
     * @return the owning node
     * @throws IllegalStateException if the ring is empty or has no active node
     */
    public CacheNode getNode(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (ring.isEmpty()) {
            throw new IllegalStateException("Ring is empty; add a node before routing keys");
        }
        for (CacheNode candidate : walkFrom(hash(key))) {
            if (candidate.isActive()) {
                return candidate;
            }
        }
        throw new IllegalStateException("No active nodes in the ring");
    }

    /**
     * Returns the distinct active nodes that should hold copies of {@code key},
     * owner first, in ring order.
     *
     * @param key               the key to route
     * @param replicationFactor number of nodes wanted
     * @return up to {@code replicationFactor} nodes
     * @throws IllegalArgumentException if there are fewer active nodes than requested
     */
    public List<CacheNode> getReplicationNodes(String key, int replicationFactor) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        int active = getActiveNodes().size();
        if (replicationFactor > active) {
            throw new IllegalArgumentException(String.format(
                    "Replication factor %d exceeds active node count %d", replicationFactor, active));
        }
        List<CacheNode> result = new ArrayList<>(replicationFactor);
        Set<String> seen = new HashSet<>();
        for (CacheNode node : walkFrom(hash(key))) {
            if (result.size() == replicationFactor) {
                break;
            }
            if (node.isActive() && seen.add(node.getId())) {
                result.add(node);
            }
        }
        return Collections.unmodifiableList(result);
    }

    /** @return every node, in no particular order */
    public Collection<CacheNode> getNodes() {
        lock.readLock().lock();
        try {
            return List.copyOf(nodeById.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** @return the active nodes */
    public List<CacheNode> getActiveNodes() {
        lock.readLock().lock();
        try {
            return nodeById.values().stream().filter(CacheNode::isActive).toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param nodeId a node id
     * @return whether a node with that id is on the ring
     */
    public boolean containsNode(String nodeId) {
        lock.readLock().lock();
        try {
            return nodeById.containsKey(nodeId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** @return number of physical nodes */
    public int getPhysicalNodeCount() {
        lock.readLock().lock();
        try {
            return nodeById.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** @return number of positions on the ring */
    public int getVirtualNodeCount() {
        return ring.size();
    }

    /** @return whether the ring has no nodes */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    /**
     * Returns each node's share of the ring positions, as a percentage. This
     * approximates the share of keys each node will receive.
     *
     * @return node id to percentage
     */
    public Map<String, Double> getKeyDistribution() {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<String, Integer> counts = new LinkedHashMap<>();
            for (CacheNode node : ring.values()) {
                counts.merge(node.getId(), 1, Integer::sum);
            }
            Map<String, Double> distribution = new LinkedHashMap<>();
            int total = ring.size();
            counts.forEach((id, n) -> distribution.put(id, n * 100.0 / total));
            return Collections.unmodifiableMap(distribution);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        return String.format("ConsistentHashRing{nodes=%d, vnodes=%d, distribution=%s}",
                getPhysicalNodeCount(), getVirtualNodeCount(), getKeyDistribution());
    }

    /** Iterates the ring once, clockwise, starting at {@code position}. Lazy, so a hit costs O(log n). */
    private Iterable<CacheNode> walkFrom(int position) {
        return () -> Stream.concat(
                ring.tailMap(position, true).values().stream(),
                ring.headMap(position, false).values().stream()).iterator();
    }

    private void removeInternal(String nodeId) {
        ring.values().removeIf(node -> node.getId().equals(nodeId));
        nodeById.remove(nodeId);
    }

    private static int hash(String input) {
        byte[] digest = MD5.get().digest(input.getBytes(StandardCharsets.UTF_8));
        return (digest[0] & 0xFF)
                | (digest[1] & 0xFF) << 8
                | (digest[2] & 0xFF) << 16
                | (digest[3] & 0xFF) << 24;
    }
}
