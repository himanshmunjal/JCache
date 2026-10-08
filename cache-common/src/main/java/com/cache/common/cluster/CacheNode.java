package com.cache.common.cluster;

import java.util.Objects;

/**
 * One server in a cluster. Nodes are immutable and identified by their id
 * alone, so a node with a changed status still equals the original.
 */
public final class CacheNode {

    /** Lifecycle state of a node. Only {@link #ACTIVE} nodes receive keys. */
    public enum Status {
        /** Serving traffic. */
        ACTIVE,
        /** Being added; not routed to yet. */
        JOINING,
        /** Being removed; not routed to any more. */
        LEAVING,
        /** Unreachable. */
        FAILED
    }

    private final String id;
    private final String host;
    private final int port;
    private final int weight;
    private final Status status;

    /**
     * Creates a node.
     *
     * @param id     unique id
     * @param host   host name or address
     * @param port   TCP port
     * @param weight relative share of the key space; a weight of 2 gets twice the virtual nodes
     * @param status lifecycle state
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
        this.id = id;
        this.host = host;
        this.port = port;
        this.weight = weight;
        this.status = status;
    }

    /**
     * Creates an active node with weight 1.
     *
     * @param id   unique id
     * @param host host name or address
     * @param port TCP port
     */
    public CacheNode(String id, String host, int port) {
        this(id, host, port, 1, Status.ACTIVE);
    }

    /**
     * Creates an active node.
     *
     * @param id     unique id
     * @param host   host name or address
     * @param port   TCP port
     * @param weight relative share of the key space
     */
    public CacheNode(String id, String host, int port, int weight) {
        this(id, host, port, weight, Status.ACTIVE);
    }

    /** @return the node id */
    public String getId() {
        return id;
    }

    /** @return the host */
    public String getHost() {
        return host;
    }

    /** @return the port */
    public int getPort() {
        return port;
    }

    /** @return the weight */
    public int getWeight() {
        return weight;
    }

    /** @return the status */
    public Status getStatus() {
        return status;
    }

    /** @return {@code host:port} */
    public String getAddress() {
        return host + ":" + port;
    }

    /** @return whether the node is {@link Status#ACTIVE} */
    public boolean isActive() {
        return status == Status.ACTIVE;
    }

    /**
     * @param newStatus the status for the copy
     * @return a copy of this node with a different status
     */
    public CacheNode withStatus(Status newStatus) {
        return new CacheNode(id, host, port, weight, newStatus);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CacheNode other && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("CacheNode{id='%s', address='%s', weight=%d, status=%s}",
                id, getAddress(), weight, status);
    }
}
