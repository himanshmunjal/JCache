package com.cache.client;

import com.cache.common.cluster.CacheNode;
import com.cache.common.cluster.ConsistentHashRing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for a group of independent JCache servers. Keys are spread across
 * the servers with a {@link ConsistentHashRing}, so there is no proxy or
 * coordinator: every client computes the owner of a key itself.
 *
 * <pre>{@code
 * try (ClusterCacheClient cluster = ClusterCacheClient.builder()
 *         .addServer("node-1", "localhost", 6379)
 *         .addServer("node-2", "localhost", 6380)
 *         .build()) {
 *     cluster.put("user:1", "Alice", 3600);
 *     String name = cluster.get("user:1");
 * }
 * }</pre>
 *
 * <p>If the owning server cannot be reached, the operation is retried once on
 * another server. A value written during such a failover lives on the other
 * server and will read as a miss once the owner is back, which is acceptable
 * for a cache. Server error replies are not retried. With a health check
 * enabled, a server that fails three checks in a row is removed
 * from the ring.
 *
 * <p>Thread-safe.
 */
public class ClusterCacheClient implements AutoCloseable {

    private static final Logger log = Logger.getLogger(ClusterCacheClient.class.getName());

    static final int DEFAULT_POOL_SIZE_PER_NODE = 5;
    static final int HEALTH_FAILURE_THRESHOLD = 3;
    private static final int MAX_ATTEMPTS = 2;

    private final ConsistentHashRing ring = new ConsistentHashRing();
    private final Map<String, ConnectionPool> pools = new ConcurrentHashMap<>();
    private final Map<String, CacheNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, Integer> consecutiveFailures = new ConcurrentHashMap<>();
    private final ReadWriteLock topologyLock = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final int poolSizePerNode;
    private final long healthCheckIntervalMs;
    private final ScheduledExecutorService healthChecker;

    private ClusterCacheClient(Builder builder) throws IOException {
        this.poolSizePerNode = builder.poolSizePerNode;
        this.healthCheckIntervalMs = builder.healthCheckIntervalMs;
        try {
            for (CacheNode node : builder.initialNodes.values()) {
                addServer(node.getId(), node.getHost(), node.getPort());
            }
        } catch (IOException | RuntimeException e) {
            pools.values().forEach(ConnectionPool::close);
            throw e;
        }
        if (healthCheckIntervalMs > 0) {
            healthChecker = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "jcache-health-check");
                t.setDaemon(true);
                return t;
            });
            healthChecker.scheduleWithFixedDelay(this::checkHealth,
                    healthCheckIntervalMs, healthCheckIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            healthChecker = null;
        }
    }

    /** @return a builder */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Adds a server, or replaces the one with the same id.
     *
     * @param nodeId unique id
     * @param host   server host
     * @param port   server port
     * @throws IOException if no connection can be opened to the server
     */
    public void addServer(String nodeId, String host, int port) throws IOException {
        ensureOpen();
        validateNode(nodeId, host, port);
        ConnectionPool pool;
        try {
            pool = new ConnectionPool(host, port, poolSizePerNode);
        } catch (IllegalStateException e) {
            throw new IOException("Cannot connect to node " + nodeId + " at " + host + ":" + port, e);
        }
        topologyLock.writeLock().lock();
        try {
            removeInternal(nodeId);
            CacheNode node = new CacheNode(nodeId, host, port);
            ring.addNode(node);
            nodes.put(nodeId, node);
            pools.put(nodeId, pool);
            consecutiveFailures.put(nodeId, 0);
        } finally {
            topologyLock.writeLock().unlock();
        }
        log.fine(() -> "Added node " + nodeId + " at " + host + ":" + port);
    }

    /**
     * Removes a server. Its keys are routed to the remaining servers from now on.
     *
     * @param nodeId id of the server to remove
     */
    public void removeServer(String nodeId) {
        ensureOpen();
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be null or blank");
        }
        topologyLock.writeLock().lock();
        try {
            if (removeInternal(nodeId)) {
                log.fine(() -> "Removed node " + nodeId);
            }
        } finally {
            topologyLock.writeLock().unlock();
        }
    }

    /**
     * Reads a value from the server that owns the key.
     *
     * @param key the key
     * @return the value, or {@code null} if absent
     */
    public String get(String key) {
        return route(key, conn -> conn.get(key));
    }

    /**
     * Stores a value without an explicit TTL.
     *
     * @param key   the key
     * @param value the value
     */
    public void put(String key, String value) {
        put(key, value, 0);
    }

    /**
     * Stores a value with a TTL.
     *
     * @param key        the key
     * @param value      the value
     * @param ttlSeconds TTL in seconds; 0 uses the server's default
     */
    public void put(String key, String value, long ttlSeconds) {
        CacheClient.validateKey(key);
        CacheClient.validateValue(value);
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("TTL cannot be negative: " + ttlSeconds);
        }
        route(key, conn -> {
            conn.put(key, value, ttlSeconds);
            return null;
        });
    }

    /**
     * Removes a key.
     *
     * @param key the key
     */
    public void delete(String key) {
        route(key, conn -> {
            conn.delete(key);
            return null;
        });
    }

    /**
     * Pings every server.
     *
     * @return node id to whether it answered
     */
    public Map<String, Boolean> ping() {
        Map<String, Boolean> result = new LinkedHashMap<>();
        forEachNode((id, conn) -> result.put(id, conn.ping()), (id, e) -> result.put(id, false));
        return result;
    }

    /**
     * Sends FLUSH to every server.
     *
     * @return node id to error message for the servers that failed; empty if all succeeded
     */
    public Map<String, String> flushAll() {
        Map<String, String> failures = new LinkedHashMap<>();
        forEachNode((id, conn) -> conn.flush(), (id, e) -> failures.put(id, e.getMessage()));
        return Collections.unmodifiableMap(failures);
    }

    /**
     * Collects STATS from every server. Keys are prefixed with the node id
     * ({@code node-1.hits}); cluster totals are under {@code cluster.*}.
     *
     * @return the combined metrics
     */
    public Map<String, String> clusterStats() {
        Map<String, String> result = new LinkedHashMap<>();
        long[] totals = new long[3];
        forEachNode((id, conn) -> {
            Map<String, String> stats = conn.stats();
            stats.forEach((k, v) -> result.put(id + "." + k, v));
            totals[0] += parseLong(stats.get("hits"));
            totals[1] += parseLong(stats.get("misses"));
            totals[2]++;
        }, (id, e) -> result.put(id + ".status", "UNREACHABLE"));

        result.put("cluster.nodes", String.valueOf(getNodeCount()));
        result.put("cluster.reachable", String.valueOf(totals[2]));
        result.put("cluster.totalHits", String.valueOf(totals[0]));
        result.put("cluster.totalMisses", String.valueOf(totals[1]));
        long lookups = totals[0] + totals[1];
        if (lookups > 0) {
            result.put("cluster.hitRate", String.format("%.2f%%", totals[0] * 100.0 / lookups));
        }
        return Collections.unmodifiableMap(result);
    }

    /** @return the registered servers */
    public List<CacheNode> getNodes() {
        return List.copyOf(nodes.values());
    }

    /** @return number of registered servers */
    public int getNodeCount() {
        return ring.getPhysicalNodeCount();
    }

    /** @return whether no server is registered */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    /**
     * @param key a key
     * @return the server that owns the key
     */
    public CacheNode getRoutingTarget(String key) {
        ensureNotEmpty();
        CacheClient.validateKey(key);
        return ring.getNode(key);
    }

    /** @return each server's share of the key space, as a percentage */
    public Map<String, Double> getKeyDistribution() {
        return ring.getKeyDistribution();
    }

    /** @return per-server pool usage, for diagnostics */
    public Map<String, String> getPoolStats() {
        Map<String, String> stats = new LinkedHashMap<>();
        pools.forEach((id, p) -> stats.put(id,
                String.format("active=%d idle=%d size=%d", p.getActiveCount(), p.getIdleCount(), p.getPoolSize())));
        return Collections.unmodifiableMap(stats);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (healthChecker != null) {
            healthChecker.shutdownNow();
        }
        topologyLock.writeLock().lock();
        try {
            for (String id : new ArrayList<>(nodes.keySet())) {
                removeInternal(id);
            }
        } finally {
            topologyLock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        return String.format("ClusterCacheClient{nodes=%d, poolPerNode=%d, healthCheck=%s, closed=%b}",
                getNodeCount(), poolSizePerNode,
                healthCheckIntervalMs > 0 ? healthCheckIntervalMs + "ms" : "off", closed.get());
    }

    @FunctionalInterface
    private interface Operation<T> {
        T run(CacheClient conn) throws IOException;
    }

    @FunctionalInterface
    private interface NodeAction {
        void run(String nodeId, CacheClient conn) throws IOException;
    }

    @FunctionalInterface
    private interface NodeFailure {
        void handle(String nodeId, Exception e);
    }

    private <T> T route(String key, Operation<T> op) {
        ensureOpen();
        CacheClient.validateKey(key);
        topologyLock.readLock().lock();
        try {
            ensureNotEmpty();
            Set<String> tried = new HashSet<>();
            Exception lastError = null;
            for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
                CacheNode target = pickNode(key, tried);
                if (target == null) {
                    break;
                }
                tried.add(target.getId());
                try {
                    return withConnection(pools.get(target.getId()), op);
                } catch (CacheClient.CacheClientException e) {
                    throw new ClusterOperationException(e.getMessage(), e);
                } catch (Exception e) {
                    lastError = e;
                    log.warning("Request for key " + key + " failed on " + target.getId() + ": " + e.getMessage());
                }
            }
            throw new ClusterOperationException("All attempts failed for key " + key
                    + (lastError != null ? ": " + lastError.getMessage() : ""), lastError);
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    /** The owner of the key, or else any active node not tried yet. */
    private CacheNode pickNode(String key, Set<String> tried) {
        CacheNode owner = ring.getNode(key);
        if (!tried.contains(owner.getId())) {
            return owner;
        }
        return ring.getActiveNodes().stream()
                .filter(n -> !tried.contains(n.getId()))
                .findFirst()
                .orElse(null);
    }

    private void forEachNode(NodeAction action, NodeFailure onFailure) {
        ensureOpen();
        topologyLock.readLock().lock();
        try {
            pools.forEach((id, pool) -> {
                try {
                    withConnection(pool, conn -> {
                        action.run(id, conn);
                        return null;
                    });
                } catch (Exception e) {
                    onFailure.handle(id, e);
                }
            });
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    private static <T> T withConnection(ConnectionPool pool, Operation<T> op) throws Exception {
        CacheClient conn = pool.acquire();
        try {
            return op.run(conn);
        } finally {
            pool.release(conn);
        }
    }

    private void checkHealth() {
        try {
            removeUnhealthyNodes();
        } catch (RuntimeException e) {
            // Must not escape, or the scheduler stops running health checks.
            log.log(Level.WARNING, "Health check failed", e);
        }
    }

    private void removeUnhealthyNodes() {
        List<String> failed = new ArrayList<>();
        Map<String, Boolean> alive = ping();
        alive.forEach((id, ok) -> {
            int failures = ok ? 0 : consecutiveFailures.merge(id, 1, Integer::sum);
            consecutiveFailures.put(id, failures);
            if (failures >= HEALTH_FAILURE_THRESHOLD) {
                failed.add(id);
            }
        });
        for (String id : failed) {
            log.log(Level.WARNING, "Removing node {0} after {1} failed health checks",
                    new Object[]{id, HEALTH_FAILURE_THRESHOLD});
            removeServer(id);
        }
    }

    /** Caller must hold the write lock. */
    private boolean removeInternal(String nodeId) {
        CacheNode node = nodes.remove(nodeId);
        if (node != null) {
            ring.removeNode(node);
        }
        ConnectionPool pool = pools.remove(nodeId);
        if (pool != null) {
            pool.close();
        }
        consecutiveFailures.remove(nodeId);
        return node != null;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("ClusterCacheClient is closed");
        }
    }

    private void ensureNotEmpty() {
        if (ring.isEmpty()) {
            throw new ClusterOperationException("No servers registered; call addServer() first");
        }
    }


    private static void validateNode(String nodeId, String host, int port) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be null or blank");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be null or blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be in [1, 65535], got: " + port);
        }
    }

    private static long parseLong(String value) {
        try {
            return value == null ? 0 : Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Builder for {@link ClusterCacheClient}. */
    public static final class Builder {
        private final Map<String, CacheNode> initialNodes = new LinkedHashMap<>();
        private int poolSizePerNode = DEFAULT_POOL_SIZE_PER_NODE;
        private long healthCheckIntervalMs;

        private Builder() {
        }

        /**
         * Adds a server to connect to on {@link #build()}.
         *
         * @param nodeId unique id
         * @param host   server host
         * @param port   server port
         * @return this builder
         */
        public Builder addServer(String nodeId, String host, int port) {
            validateNode(nodeId, host, port);
            initialNodes.put(nodeId, new CacheNode(nodeId, host, port));
            return this;
        }

        /**
         * @param size connections per server, at least 1 (default 5)
         * @return this builder
         */
        public Builder poolSizePerNode(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("Pool size must be >= 1, got: " + size);
            }
            this.poolSizePerNode = size;
            return this;
        }

        /**
         * Enables periodic health checks.
         *
         * @param intervalMs interval between checks, positive
         * @return this builder
         */
        public Builder withHealthCheck(long intervalMs) {
            if (intervalMs <= 0) {
                throw new IllegalArgumentException("Health check interval must be positive, got: " + intervalMs);
            }
            this.healthCheckIntervalMs = intervalMs;
            return this;
        }

        /**
         * Connects to every server added so far.
         *
         * @return the client
         * @throws IOException if a server cannot be reached
         */
        public ClusterCacheClient build() throws IOException {
            return new ClusterCacheClient(this);
        }
    }

    /** Thrown when an operation could not be completed on any server. */
    public static class ClusterOperationException extends RuntimeException {

        /**
         * @param message description
         */
        public ClusterOperationException(String message) {
            super(message);
        }

        /**
         * @param message description
         * @param cause   the last underlying failure
         */
        public ClusterOperationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
