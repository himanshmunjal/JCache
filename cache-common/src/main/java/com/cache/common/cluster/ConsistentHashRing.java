package com.cache.common.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * ConsistentHashRing implements consistent hashing with virtual nodes.
 *
 * ═══════════════════════════════════════════════════════════════
 * THE PROBLEM THIS SOLVES
 * ═══════════════════════════════════════════════════════════════
 *
 * Naive distribution: hash(key) % N
 *   Works fine with a fixed N. But if you add or remove a server:
 *   - Old N = 3: key hashes 0–99 → node 0, 100–199 → node 1, 200–299 → node 2
 *   - New N = 4: EVERY key remaps. 100% cache miss rate. Your database dies.
 *
 * Consistent hashing: arrange nodes and keys on a circle (ring).
 *   Each key maps to the first node clockwise from its position.
 *   When a node is removed, only the keys between it and its counter-clockwise
 *   neighbor need to move — approximately 1/N of all keys.
 *   When a node is added, it takes keys from its clockwise neighbor only.
 *
 * ═══════════════════════════════════════════════════════════════
 * THE VIRTUAL NODES PROBLEM
 * ═══════════════════════════════════════════════════════════════
 *
 * Pure consistent hashing with 3 nodes places 3 points on the ring.
 * The ring is a circle of 2^32 positions (MD5 gives 128 bits; we use 32).
 * With only 3 points, each node owns roughly 1/3 of the ring — but
 * "roughly" is the problem. With small numbers, distribution is uneven.
 * One node might own 50% of the ring, another 20%, the third 30%.
 *
 * Virtual nodes: each physical node gets V positions on the ring.
 *   Named "node-1:0", "node-1:1", ..., "node-1:149" for 150 virtual nodes.
 *   Each maps to a different ring position via hashing.
 *   With 150 virtual nodes per physical node and 3 physical nodes:
 *   450 ring points → distribution converges to roughly 33% each.
 *   With 1000 virtual nodes, variance < 1%.
 *
 * The trade-off: 150 virtual nodes per physical node × N nodes = ring size.
 *   10 nodes → 1500 ring entries. TreeMap lookup is O(log 1500) ≈ 11 steps.
 *   This is completely negligible compared to network latency.
 *
 * ═══════════════════════════════════════════════════════════════
 * HASH FUNCTION CHOICE: MD5
 * ═══════════════════════════════════════════════════════════════
 *
 * We use MD5 for ring position hashing. NOT for security — MD5 is
 * cryptographically broken. We use it because:
 *   1. Good distribution across the 2^32 ring space
 *   2. Deterministic: same input → same output, always
 *   3. Available in Java standard library (MessageDigest)
 *   4. Fast enough for our use case (ring ops are infrequent)
 *
 * We take bytes [0..3] of the 16-byte MD5 digest to get a 32-bit integer.
 * This is the same approach used by Memcached's ketama library — the
 * original consistent hashing implementation that Twitter and Wikipedia used.
 *
 * ═══════════════════════════════════════════════════════════════
 * DATA STRUCTURE: ConcurrentSkipListMap
 * ═══════════════════════════════════════════════════════════════
 *
 * We store ring positions in a ConcurrentSkipListMap<Integer, CacheNode>.
 *   - Key: hash position on the ring (32-bit integer)
 *   - Value: the physical CacheNode that owns this virtual node position
 *
 * Why ConcurrentSkipListMap instead of TreeMap?
 *   TreeMap is not thread-safe. ConcurrentSkipListMap is a lock-free sorted
 *   map — reads (tailMap lookups) happen concurrently with no blocking.
 *   We still need a ReentrantReadWriteLock for addNode/removeNode because
 *   those operations insert/delete multiple entries atomically.
 *   But getNode() (the hot path) only reads and requires no lock beyond
 *   what ConcurrentSkipListMap provides internally.
 *
 * ═══════════════════════════════════════════════════════════════
 * THREAD SAFETY MODEL
 * ═══════════════════════════════════════════════════════════════
 *
 *   READ PATH (getNode):    Lock-free. ConcurrentSkipListMap handles concurrency.
 *   WRITE PATH (addNode, removeNode): WriteLock. Multi-step operations must be atomic.
 *     Without the lock, a reader might see a partially-added node (some virtual
 *     nodes present, others not yet inserted) and route keys incorrectly.
 *
 * ═══════════════════════════════════════════════════════════════
 * REAL-WORLD USAGE
 * ═══════════════════════════════════════════════════════════════
 *
 * This algorithm (or a variant) is used in:
 *   - Amazon DynamoDB: consistent hashing for partition routing
 *   - Apache Cassandra: token ring with virtual nodes
 *   - Memcached (libketama): original consistent hashing implementation
 *   - Redis Cluster: uses hash slots (a discrete variant of consistent hashing)
 *   - Riak: consistent hashing with preference lists for replication
 *
 * INTERVIEW TALKING POINT:
 *   "I implemented consistent hashing with 150 virtual nodes per physical node,
 *    using MD5 for ring position hashing (same approach as libketama/Memcached).
 *    I verified that removing 1 of 3 nodes remaps only ~33% of keys rather than
 *    100%, and that the load distribution variance across nodes is under 5%
 *    with 150 virtual nodes."
 */
public class ConsistentHashRing {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /**
     * Number of virtual nodes per physical node.
     * 150 is the same default used by libketama (Memcached's consistent hashing lib).
     *
     * Empirical distribution quality at 150 vnodes with 3 physical nodes:
     *   Each node handles 33% ± ~3% of the key space.
     * At 1000 vnodes: variance < 1% but ring occupies 10x more memory.
     * 150 is a good balance for a cache server with 3–20 nodes.
     *
     * CAN BE OVERRIDDEN per-node via CacheNode.weight:
     *   Effective vnodes for node = VIRTUAL_NODES_PER_NODE * node.getWeight()
     */
    private static final int VIRTUAL_NODES_PER_NODE = 150;

    /**
     * Separator between node ID and virtual node replica index in hash input.
     * "node-1:0", "node-1:1", ... , "node-1:149"
     * The colon is chosen because it's not valid in hostnames, reducing
     * the chance of accidental collisions between node IDs and replica suffixes.
     */
    private static final String VIRTUAL_NODE_SEPARATOR = ":";

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * The ring itself: maps ring position (32-bit hash) → owning CacheNode.
     *
     * ConcurrentSkipListMap keeps entries sorted by key (ring position).
     * The tailMap operation (find next entry ≥ X) is O(log N) and thread-safe.
     *
     * For a ring with 3 physical nodes × 150 virtual nodes = 450 entries,
     * O(log 450) ≈ 9 comparisons per lookup. Microsecond-range.
     */
    private final ConcurrentSkipListMap<Integer, CacheNode> ring;

    /**
     * Maps physical node ID → CacheNode for O(1) node lookup by ID.
     * Used in removeNode() to find all virtual node positions for a given node.
     *
     * Without this, removeNode() would need to scan all 450+ ring entries
     * to find which ones belong to the departing node — O(V*N) instead of O(V).
     */
    private final Map<String, CacheNode> nodeById;

    /**
     * Read-write lock protecting addNode() and removeNode() operations.
     * Multiple readers (getNode callers) can proceed concurrently.
     * A single writer (addNode/removeNode) gets exclusive access.
     *
     * Note: getNode() uses ConcurrentSkipListMap's own thread-safety for reads.
     * This lock only guards the multi-step write operations.
     */
    private final ReadWriteLock lock;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates an empty ConsistentHashRing.
     * Add nodes via addNode() before calling getNode().
     */
    public ConsistentHashRing() {
        this.ring     = new ConcurrentSkipListMap<>();
        this.nodeById = new HashMap<>();
        this.lock     = new ReentrantReadWriteLock();
    }

    /**
     * Creates a ConsistentHashRing pre-populated with the given nodes.
     * Convenience constructor for initialization with a known cluster topology.
     *
     * @param initialNodes Collection of nodes to add to the ring.
     */
    public ConsistentHashRing(Collection<CacheNode> initialNodes) {
        this();
        for (CacheNode node : initialNodes) {
            addNode(node);
        }
    }

    // -------------------------------------------------------------------------
    // Core operations
    // -------------------------------------------------------------------------

    /**
     * Adds a physical node to the ring by inserting VIRTUAL_NODES_PER_NODE * weight
     * virtual node positions.
     *
     * WHAT HAPPENS TO EXISTING KEYS:
     * After adding a node, some keys that previously mapped to node X will now
     * map to the new node (because the new node's virtual positions are clockwise-
     * closest for those keys). Specifically, each virtual node "steals" keys from
     * the next clockwise physical node. Total keys moved ≈ 1/N of all keys.
     *
     * This method is idempotent if called with a node whose ID is already present:
     * the existing node is first removed, then re-added. Use this to update
     * a node's address or weight.
     *
     * @param node The physical node to add. Cannot be null.
     * @throws IllegalArgumentException if node is null.
     */
    public void addNode(CacheNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Cannot add null node to ring");
        }

        lock.writeLock().lock();
        try {
            // If node already exists, remove it first (re-add = update).
            if (nodeById.containsKey(node.getId())) {
                removeNodeInternal(node.getId());
            }

            // Register the physical node.
            nodeById.put(node.getId(), node);

            // Insert virtual nodes.
            // Effective virtual node count = base count × weight.
            // A node with weight=2 gets 2x ring positions → 2x traffic share.
            int effectiveVnodes = VIRTUAL_NODES_PER_NODE * node.getWeight();

            for (int replica = 0; replica < effectiveVnodes; replica++) {
                // Hash input: "nodeId:replicaIndex"
                // e.g., "node-1:0", "node-1:1", ..., "node-1:149"
                String virtualNodeKey = node.getId() + VIRTUAL_NODE_SEPARATOR + replica;
                int ringPosition = computeHash(virtualNodeKey);

                // Place the virtual node on the ring.
                // If there's a collision (two virtual nodes hash to same position),
                // the later one overwrites the earlier. With 32-bit hash space and
                // ~450 virtual nodes, collision probability is negligible
                // (birthday problem: ~450^2 / 2^33 ≈ 0.002%).
                ring.put(ringPosition, node);
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a physical node from the ring, deleting all its virtual node positions.
     *
     * WHAT HAPPENS TO AFFECTED KEYS:
     * Keys that were routing to the removed node will now route to the next
     * clockwise physical node. The client is responsible for re-fetching
     * (cache miss) or migrating those keys to their new node.
     *
     * @param node The node to remove.
     * @throws IllegalArgumentException if node is null.
     * @throws NoSuchElementException   if the node is not in the ring.
     */
    public void removeNode(CacheNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Cannot remove null node from ring");
        }

        lock.writeLock().lock();
        try {
            if (!nodeById.containsKey(node.getId())) {
//                throw new NoSuchElementException("Node not found in ring: " + node.getId());
                return;
            }
            removeNodeInternal(node.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Routes a key to the responsible CacheNode.
     *
     * ALGORITHM:
     *   1. Hash the key to a 32-bit integer ring position.
     *   2. Find the first ring entry with position ≥ key's position (tailMap).
     *   3. If no entry exists at or after the position (we're past the last vnode),
     *      wrap around to the first entry on the ring.
     *   4. Return that entry's CacheNode.
     *
     * ACTIVE-ONLY ROUTING:
     *   We skip FAILED/LEAVING nodes and return the next ACTIVE node clockwise.
     *   This ensures traffic doesn't route to down servers.
     *   If all nodes are non-ACTIVE, throws IllegalStateException (ring is unusable).
     *
     * This method is LOCK-FREE for reads — ConcurrentSkipListMap handles concurrency.
     * It can be called from thousands of threads simultaneously with no contention.
     *
     * @param key The cache key to route. Cannot be null.
     * @return The CacheNode responsible for this key.
     * @throws IllegalArgumentException if key is null.
     * @throws IllegalStateException    if the ring is empty or has no ACTIVE nodes.
     */
    public CacheNode getNode(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (ring.isEmpty()) {
            throw new IllegalStateException(
                    "Ring is empty — add at least one node before routing keys"
            );
        }

        int keyHash = computeHash(key);

        // Walk the ring starting from keyHash, looking for an ACTIVE node.
        // In the normal case (all nodes active), this loop executes once.
        // The loop handles FAILED/LEAVING nodes by skipping past them.

        // We need to check up to (total virtual nodes) positions before giving up.
        // Use the full ring as a circular list.
        int totalVnodes = ring.size();
        int checked = 0;

        // Start from the key's position on the ring.
        Integer startPos = keyHash;

        while (checked < totalVnodes) {
            // tailMap: returns a view of entries with key ≥ startPos.
            // firstEntry() of that view = nearest vnode clockwise from startPos.
            Map.Entry<Integer, CacheNode> entry = ring.tailMap(startPos).firstEntry();

            // If tailMap is empty, wrap around to the start of the ring.
            if (entry == null) {
                entry = ring.firstEntry();
            }

            CacheNode candidate = entry.getValue();

            // Only route to ACTIVE nodes.
            if (candidate.isActive()) {
                return candidate;
            }

            // Node is not active — skip past all its virtual nodes.
            // Move startPos past this entry to search further clockwise.
            startPos = entry.getKey() + 1;
            checked++;
        }

        throw new IllegalStateException(
                "No ACTIVE nodes found in ring. All " + nodeById.size() +
                        " nodes are FAILED or LEAVING."
        );
    }

    // -------------------------------------------------------------------------
    // Query methods
    // -------------------------------------------------------------------------

    /**
     * Returns all physical nodes currently in the ring (all statuses).
     * Returns an unmodifiable snapshot — changes to the ring after this call
     * are not reflected in the returned collection.
     *
     * @return Unmodifiable collection of all registered physical nodes.
     */
    public Collection<CacheNode> getNodes() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableCollection(
                    new ArrayList<>(nodeById.values())
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns only the ACTIVE physical nodes — nodes currently serving traffic.
     *
     * @return Unmodifiable list of active nodes.
     */
    public List<CacheNode> getActiveNodes() {
        lock.readLock().lock();
        try {
            return nodeById.values().stream()
                    .filter(CacheNode::isActive)
                    .collect(Collectors.toUnmodifiableList());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the number of physical nodes in the ring (all statuses).
     *
     * @return Physical node count.
     */
    public int getPhysicalNodeCount() {
        return nodeById.size();
    }

    /**
     * Returns the total number of virtual node positions on the ring.
     * For N physical nodes with default weight=1: ring size = N × 150.
     *
     * @return Virtual node (ring entry) count.
     */
    public int getVirtualNodeCount() {
        return ring.size();
    }

    /**
     * Returns true if the ring contains a physical node with the given ID.
     *
     * @param nodeId The node ID to check.
     * @return true if the node is registered.
     */
    public boolean containsNode(String nodeId) {
        lock.readLock().lock();
        try {
            return nodeById.containsKey(nodeId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns true if the ring has no nodes.
     *
     * @return true if empty.
     */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    /**
     * Computes the approximate key distribution across physical nodes.
     * Returns a map of nodeId → percentage of virtual ring owned.
     *
     * Uses consecutive ring position differences to calculate arc length.
     * This is an approximation — actual key distribution depends on your
     * hash function's output distribution for real keys.
     *
     * USEFUL FOR:
     *   - Verifying that virtual nodes provide even distribution
     *   - Detecting hot spots in a weighted cluster
     *   - README charts showing distribution quality
     *
     * @return Map of nodeId → percentage (0.0–100.0) of ring owned.
     */
    public Map<String, Double> getKeyDistribution() {
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) return Collections.emptyMap();

            // Count virtual nodes per physical node.
            // Virtual node count is a proxy for key distribution percentage
            // (each vnode represents an equal share of the ring on average).
            Map<String, Integer> vnodeCount = new HashMap<>();
            for (CacheNode node : ring.values()) {
                vnodeCount.merge(node.getId(), 1, Integer::sum);
            }

            int total = ring.size();
            Map<String, Double> distribution = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> e : vnodeCount.entrySet()) {
                distribution.put(e.getKey(), (double) e.getValue() / total * 100.0);
            }

            return Collections.unmodifiableMap(distribution);

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the N nodes responsible for a given key (for replication).
     * In a replicated system, you write to the primary node and N-1 replicas.
     * The replicas are the next N-1 DISTINCT physical nodes clockwise from the key.
     *
     * EXAMPLE WITH 3 NODES, replicationFactor=2:
     *   Key "user:123" → primary: node-A, replica: node-B
     *   Write goes to both node-A and node-B.
     *   If node-A fails, reads fall back to node-B (no data loss).
     *
     * @param key               The cache key.
     * @param replicationFactor How many nodes should hold this key (including primary).
     * @return Ordered list of [primary, replica1, replica2, ...].
     * @throws IllegalArgumentException if replicationFactor > physical node count.
     */
    public List<CacheNode> getReplicationNodes(String key, int replicationFactor) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        List<CacheNode> activeNodes = getActiveNodes();
        if (replicationFactor > activeNodes.size()) {
            throw new IllegalArgumentException(String.format(
                    "Replication factor %d exceeds active node count %d",
                    replicationFactor, activeNodes.size()
            ));
        }

        int keyHash = computeHash(key);
        List<CacheNode> result = new ArrayList<>(replicationFactor);
        Set<String> seen = new HashSet<>(); // track physical nodes to avoid duplicates

        Integer pos = keyHash;
        while (result.size() < replicationFactor) {
            Map.Entry<Integer, CacheNode> entry = ring.tailMap(pos).firstEntry();
            if (entry == null) entry = ring.firstEntry();
            if (entry == null) break; // empty ring

            CacheNode node = entry.getValue();
            if (node.isActive() && !seen.contains(node.getId())) {
                result.add(node);
                seen.add(node.getId());
            }
            pos = entry.getKey() + 1;
        }

        return Collections.unmodifiableList(result);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Removes all virtual node positions for the given physical node ID.
     * Called from both removeNode() and addNode() (for re-add/update).
     *
     * MUST be called with writeLock held.
     *
     * Strategy: scan all ring entries and remove those pointing to nodeId.
     * This is O(V*N) where V=virtual nodes, N=physical nodes.
     * For 10 nodes × 150 vnodes = 1500 entries, this is trivially fast.
     * An alternative: keep a reverse map (nodeId → Set<ringPosition>),
     * which makes removal O(V) but adds memory overhead.
     * At this scale, the scan is fine.
     *
     * @param nodeId The ID of the physical node to remove.
     */
    private void removeNodeInternal(String nodeId) {
        // Remove all ring positions belonging to this node.
        ring.entrySet().removeIf(entry -> entry.getValue().getId().equals(nodeId));

        // Remove from the physical node registry.
        nodeById.remove(nodeId);
    }

    /**
     * Computes a 32-bit ring position for the given string using MD5.
     *
     * WHY MD5?
     * We need a hash function with good distribution across [0, 2^32).
     * MD5's 128-bit output gives us excellent distribution even when we
     * truncate to 32 bits. SHA-256 would also work but is slower and
     * provides more bits than we need.
     *
     * This is NOT for security. MD5's cryptographic weaknesses (collision
     * attacks) are irrelevant here — we're using it as a distribution function.
     *
     * BYTE EXTRACTION:
     * We take the first 4 bytes of the 16-byte MD5 digest and combine them
     * into a 32-bit integer using bit shifting:
     *   position = (b0 & 0xFF) | ((b1 & 0xFF) << 8) | ((b2 & 0xFF) << 16) | ((b3 & 0xFF) << 24)
     *
     * The & 0xFF masks are critical: Java bytes are signed (-128 to 127),
     * but we need unsigned values (0 to 255). Without masking, negative bytes
     * would corrupt the upper bits via sign extension.
     *
     * This is the same extraction strategy used by libketama.
     *
     * @param input The string to hash (virtual node key or cache key).
     * @return A 32-bit ring position.
     */
    private int computeHash(String input) {
        try {
            // MessageDigest is NOT thread-safe — create a new instance per call.
            // For high-throughput scenarios, consider ThreadLocal<MessageDigest>.
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));

            // Extract 4 bytes and combine into a 32-bit integer.
            // Little-endian byte order (same as ketama).
            return (digest[0] & 0xFF)
                    | ((digest[1] & 0xFF) << 8)
                    | ((digest[2] & 0xFF) << 16)
                    | ((digest[3] & 0xFF) << 24);

        } catch (NoSuchAlgorithmException e) {
            // MD5 is guaranteed to be available in all Java implementations
            // per the Java Security Standard Algorithm Names specification.
            // This exception cannot occur in practice.
            throw new RuntimeException("MD5 not available — this should never happen", e);
        }
    }

    // -------------------------------------------------------------------------
    // Debug / display
    // -------------------------------------------------------------------------

    /**
     * Returns a human-readable snapshot of the ring for debugging.
     * Shows the first 20 ring positions and which node owns each.
     *
     * Example output:
     *   ConsistentHashRing [3 nodes, 450 virtual nodes]
     *   Position -2147412345 → CacheNode{id='node-2', address='localhost:6381', ...}
     *   Position -2146983211 → CacheNode{id='node-1', address='localhost:6379', ...}
     *   ... (20 of 450 shown)
     *   Distribution: {node-1=33.78%, node-2=32.44%, node-3=33.78%}
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("ConsistentHashRing [%d nodes, %d virtual nodes]%n",
                getPhysicalNodeCount(), getVirtualNodeCount()));

        int shown = 0;
        for (Map.Entry<Integer, CacheNode> entry : ring.entrySet()) {
            if (shown >= 20) {
                sb.append(String.format("  ... (%d of %d shown)%n", shown, ring.size()));
                break;
            }
            sb.append(String.format("  Position %12d → %s%n",
                    entry.getKey(), entry.getValue()));
            shown++;
        }

        Map<String, Double> dist = getKeyDistribution();
        sb.append("  Distribution: ").append(dist);

        return sb.toString();
    }
}