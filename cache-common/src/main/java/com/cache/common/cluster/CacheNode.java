package com.cache.common.cluster;

import java.util.Objects;

/**
 * CacheNode represents a single physical cache server in the distributed cluster.
 *
 * ROLE IN THE SYSTEM:
 *   CacheNode is the unit of identity in the ConsistentHashRing. The ring maps
 *   key hashes → CacheNode instances. When a client wants to GET or PUT a key,
 *   it asks the ring "which CacheNode owns this key?" and then connects to that
 *   node's host:port.
 *
 * DESIGN DECISIONS:
 *
 *   1. IMMUTABILITY — all fields are final.
 *      A node's identity (id, host, port) never changes after creation.
 *      This is critical for hash ring correctness: if host or port could change,
 *      a node's virtual node positions on the ring would become stale, routing
 *      keys to the wrong (or unreachable) server. Immutability prevents this class
 *      of bug entirely.
 *
 *   2. ID IS THE IDENTITY, NOT HOST:PORT.
 *      equals() and hashCode() are based on `id` only, not on host+port.
 *      Why? Consider a node that changes its IP (e.g., after a restart in a
 *      containerized environment). If identity were tied to host:port, the ring
 *      would treat the restarted node as a new node, orphaning all its keys.
 *      With id-based identity, you can update host/port while preserving ring position.
 *      In this simple implementation both are final, but the design principle holds.
 *
 *   3. WEIGHT for non-uniform capacity distribution.
 *      A node with weight=2 gets twice as many virtual nodes on the ring as a node
 *      with weight=1. This means it handles ~twice the key space — appropriate if
 *      that server has more RAM or CPU. Default weight=1 means uniform distribution.
 *      This is how production consistent hashing (e.g., Cassandra's vnodes) works.
 *
 *   4. STATUS ENUM for cluster awareness.
 *      The ring only routes to ACTIVE nodes. JOINING nodes are being added but
 *      not yet ready. LEAVING nodes are draining (keys being migrated off them).
 *      FAILED nodes are down — the ring should route around them.
 *      This simple status model matches what real systems like Cassandra use.
 *
 * THREAD SAFETY:
 *   CacheNode is immutable (all fields final, no setters) so it is inherently
 *   thread-safe. Multiple threads can read the same CacheNode concurrently
 *   without synchronization.
 *
 * INTERVIEW TALKING POINT:
 *   "I separated node identity (id) from node address (host:port) and from
 *    node capacity (weight). This mirrors how production systems like Cassandra
 *    handle node identity — a node's token position is tied to its ID, not its
 *    IP, so the ring stays stable across restarts and IP changes."
 */
public final class CacheNode {

    // -------------------------------------------------------------------------
    // Status enum
    // -------------------------------------------------------------------------

    /**
     * Lifecycle status of a cache node in the cluster.
     * The ring uses this to decide whether to route traffic to a node.
     */
    public enum Status {

        /**
         * Node is healthy and accepting traffic.
         * The ring routes keys to ACTIVE nodes only.
         */
        ACTIVE,

        /**
         * Node is being added to the cluster but hasn't finished receiving
         * its share of the key space. Ring knows about it but may not route to it
         * until it transitions to ACTIVE.
         */
        JOINING,

        /**
         * Node is being removed. It's still serving traffic for now,
         * but keys are being migrated away from it. Once migration completes,
         * it transitions to REMOVED and is deleted from the ring.
         */
        LEAVING,

        /**
         * Node is unreachable or has crashed. The ring should route
         * its keys to the next node clockwise (or use replication).
         */
        FAILED
    }

    // -------------------------------------------------------------------------
    // Fields — all final for immutability
    // -------------------------------------------------------------------------

    /**
     * Stable unique identifier for this node. Used as the basis for:
     *   1. equals() and hashCode() — node identity in Java collections
     *   2. Virtual node hash seeds — "nodeId:0", "nodeId:1", ..., "nodeId:149"
     *
     * Convention: use a meaningful ID like "node-1", "us-east-1a-cache-01",
     * or a UUID. Avoid using host:port as the ID (it changes on restart).
     */
    private final String id;

    /**
     * Hostname or IP address of this cache server.
     * Used by the client to open a TCP connection.
     * Examples: "localhost", "192.168.1.10", "cache-1.internal.example.com"
     */
    private final String host;

    /**
     * TCP port this cache server listens on.
     * Default in our system: 6379 (same as Redis, for familiarity).
     */
    private final int port;

    /**
     * Relative capacity weight for this node.
     * A node with weight=2 gets 2x virtual nodes on the ring → handles 2x the key space.
     * Use this to give more powerful servers more traffic.
     * Must be > 0.
     */
    private final int weight;

    /**
     * Current lifecycle status of this node.
     * Mutable conceptually (nodes transition states) but we treat the node object
     * as a snapshot — create a new CacheNode with updated status rather than mutating.
     * In a real system, this would be read from a distributed coordination service.
     */
    private final Status status;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Primary constructor with all fields.
     *
     * @param id     Stable unique identifier. Cannot be null or empty.
     * @param host   Hostname or IP. Cannot be null or empty.
     * @param port   TCP port. Must be in range [1, 65535].
     * @param weight Relative capacity weight. Must be > 0.
     * @param status Node lifecycle status. Cannot be null.
     */
    public CacheNode(String id, String host, int port, int weight, Status status) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Node id cannot be null or blank");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Node host cannot be null or blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be in range [1, 65535], got: " + port);
        }
        if (weight <= 0) {
            throw new IllegalArgumentException("Weight must be > 0, got: " + weight);
        }
        if (status == null) {
            throw new IllegalArgumentException("Status cannot be null");
        }

        this.id     = id;
        this.host   = host;
        this.port   = port;
        this.weight = weight;
        this.status = status;
    }

    /**
     * Convenience constructor with default weight=1 and status=ACTIVE.
     * Use this for most cases: a healthy server with default capacity.
     *
     * @param id   Stable unique identifier.
     * @param host Hostname or IP.
     * @param port TCP port.
     */
    public CacheNode(String id, String host, int port) {
        this(id, host, port, 1, Status.ACTIVE);
    }

    /**
     * Convenience constructor with custom weight but default status=ACTIVE.
     * Use when adding a higher-capacity server to an existing cluster.
     *
     * @param id     Stable unique identifier.
     * @param host   Hostname or IP.
     * @param port   TCP port.
     * @param weight Relative capacity weight.
     */
    public CacheNode(String id, String host, int port, int weight) {
        this(id, host, port, weight, Status.ACTIVE);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /** @return The stable unique identifier for this node. */
    public String getId()     { return id; }

    /** @return The hostname or IP address of this cache server. */
    public String getHost()   { return host; }

    /** @return The TCP port this cache server listens on. */
    public int getPort()      { return port; }

    /** @return The relative capacity weight of this node. */
    public int getWeight()    { return weight; }

    /** @return The current lifecycle status of this node. */
    public Status getStatus() { return status; }

    /**
     * Returns a new CacheNode identical to this one but with a different status.
     * Use instead of mutating status directly (immutability principle).
     *
     * Example:
     *   CacheNode leaving = node.withStatus(Status.LEAVING);
     *   ring.updateNode(leaving);
     *
     * @param newStatus The new status.
     * @return A new CacheNode with the updated status.
     */
    public CacheNode withStatus(Status newStatus) {
        return new CacheNode(this.id, this.host, this.port, this.weight, newStatus);
    }

    /**
     * Returns whether this node should receive traffic.
     * The ring calls this before routing a key — only ACTIVE nodes are valid targets.
     *
     * @return true if the node is ACTIVE and can serve requests.
     */
    public boolean isActive() {
        return status == Status.ACTIVE;
    }

    /**
     * Returns the network address string used for display and logging.
     * Format: "host:port"
     *
     * @return Network address string.
     */
    public String getAddress() {
        return host + ":" + port;
    }

    // -------------------------------------------------------------------------
    // equals, hashCode, toString
    // -------------------------------------------------------------------------

    /**
     * Two CacheNodes are equal if and only if they have the same id.
     *
     * WHY NOT INCLUDE HOST/PORT?
     * If a node restarts at a new IP address, we still want it to be considered
     * "the same node" for ring membership purposes. The ring should update the
     * address while preserving the node's identity and virtual node positions.
     * ID-based equality enables this.
     *
     * WHY NOT INCLUDE STATUS?
     * A node transitioning from JOINING to ACTIVE is still the same node.
     * Status changes should not affect equality — they're state transitions,
     * not identity changes.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CacheNode)) return false;
        CacheNode other = (CacheNode) obj;
        return Objects.equals(this.id, other.id);
    }

    /**
     * Hash code based solely on id, consistent with equals().
     *
     * CONTRACT: if a.equals(b), then a.hashCode() == b.hashCode().
     * Since equals is based on id only, hashCode must also use id only.
     */
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Human-readable representation for logs and debug output.
     *
     * Format: "CacheNode{id='node-1', address='localhost:6379', weight=1, status=ACTIVE}"
     *
     * Useful in ring debug output:
     *   "Key 'user:123' → virtualNode 'node-1:47' → CacheNode{id='node-1', address='...'}"
     */
    @Override
    public String toString() {
        return String.format(
                "CacheNode{id='%s', address='%s', weight=%d, status=%s}",
                id, getAddress(), weight, status
        );
    }
}