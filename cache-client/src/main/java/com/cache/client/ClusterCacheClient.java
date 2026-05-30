package com.cache.client;

import com.cache.common.cluster.CacheNode;
import com.cache.common.cluster.ConsistentHashRing;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * ClusterCacheClient is the public-facing API for interacting with a
 * multi-node distributed JCache cluster.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * RELATIONSHIP WITH ClientRouter
 * ═══════════════════════════════════════════════════════════════════════
 *
 * ClientRouter (in cache-client/routing/) already handles:
 *   - Consistent hash ring management
 *   - Key → node routing
 *   - Single connection per node
 *   - Retry on node failure
 *
 * ClusterCacheClient sits on TOP of that and adds:
 *   - CONNECTION POOLING per node (N connections instead of 1)
 *   - CLEAN PUBLIC API  (no routing internals exposed)
 *   - HEALTH MONITORING (background thread pings all nodes)
 *   - GRACEFUL SHUTDOWN (drains pools before closing)
 *   - BUILDER PATTERN   (fluent construction for tests and production)
 *
 * The layering looks like this:
 *
 *   Application code
 *       ↓  calls
 *   ClusterCacheClient          ← you use this (this file)
 *       ↓  owns
 *   Map<nodeId, ConnectionPool> ← one pool per physical node
 *       ↓  borrows from pool
 *   CacheClient                 ← one TCP connection
 *       ↓  sends over
 *   TCP → CacheServer
 *
 * ═══════════════════════════════════════════════════════════════════════
 * ROUTING STRATEGY
 * ═══════════════════════════════════════════════════════════════════════
 *
 * ClusterCacheClient maintains its OWN ConsistentHashRing (same algorithm,
 * same virtual node count as the server). This is client-side routing —
 * the client computes which node owns a key locally and connects directly
 * to that node, with zero extra network hops.
 *
 * This matches how Redis Cluster clients (Jedis, Lettuce) work:
 * the client caches the cluster topology and routes locally.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * CONNECTION POOLING DESIGN
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Each physical node has its own ConnectionPool.
 * Default pool size: 5 connections per node.
 *
 * Why 5? Practical default matching Jedis's default pool size.
 * Operations follow this pattern:
 *   1. Route key → CacheNode  (via ConsistentHashRing, O(log N))
 *   2. Look up ConnectionPool for that node (ConcurrentHashMap, O(1))
 *   3. pool.acquire()         (blocks if all connections in use)
 *   4. Execute operation      (send command, read response)
 *   5. pool.release(conn)     (always in finally block)
 *
 * ═══════════════════════════════════════════════════════════════════════
 * FAILURE HANDLING
 * ═══════════════════════════════════════════════════════════════════════
 *
 * When a node is unreachable:
 *   1. The operation throws IOException.
 *   2. ClusterCacheClient retries on the NEXT clockwise node (1 retry).
 *   3. If retry also fails, ClusterOperationException is thrown.
 *   4. ConnectionPool replaces broken connections transparently.
 *
 * For automatic node removal, use the health monitor:
 *   builder.withHealthCheck(intervalMs) — pings all nodes periodically.
 *   Nodes that fail 3 consecutive pings are removed from the ring.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * THREAD SAFETY
 * ═══════════════════════════════════════════════════════════════════════
 *
 * READ PATH (get, put, delete):
 *   ReadLock → ring lookup → ConnectionPool (thread-safe) → CacheClient
 *   Multiple threads route and execute concurrently.
 *
 * WRITE PATH (addServer, removeServer):
 *   WriteLock → modifies ring + poolMap atomically.
 *   No read can see a ring pointing to a node whose pool doesn't exist yet.
 *
 * HEALTH MONITOR:
 *   Runs on a daemon ScheduledExecutorService thread.
 *   Uses writeLock only when removing a failed node.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * USAGE EXAMPLES
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Single node (acts like a regular CacheClient with pooling):
 *   ClusterCacheClient cache = ClusterCacheClient.builder()
 *       .addServer("node-1", "localhost", 6379)
 *       .poolSizePerNode(5)
 *       .build();
 *   cache.put("user:42", "Alice", 3600);
 *   String name = cache.get("user:42");
 *   cache.close();
 *
 * Three-node cluster with health monitoring:
 *   ClusterCacheClient cache = ClusterCacheClient.builder()
 *       .addServer("node-1", "host1", 6379)
 *       .addServer("node-2", "host2", 6379)
 *       .addServer("node-3", "host3", 6379)
 *       .poolSizePerNode(10)
 *       .withHealthCheck(5000)
 *       .build();
 */
public class ClusterCacheClient implements AutoCloseable {

    private static final Logger log = Logger.getLogger(ClusterCacheClient.class.getName());

    // -------------------------------------------------------------------------
    // Defaults
    // -------------------------------------------------------------------------

    /** Default number of TCP connections maintained per physical node. */
    private static final int DEFAULT_POOL_SIZE_PER_NODE = 5;

    /**
     * How many consecutive ping failures before a node is removed from the ring
     * by the health monitor.
     */
    private static final int HEALTH_FAILURE_THRESHOLD = 3;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * The client-side consistent hash ring.
     * Mirrors the server-side ring exactly (same algorithm, same vnode count).
     * Enables client-side routing with zero extra network hops.
     */
    private final ConsistentHashRing ring;

    /**
     * One ConnectionPool per physical node, keyed by node ID.
     * Pool lookups are on the hot path — ConcurrentHashMap for O(1) reads.
     * addServer/removeServer modify this under writeLock for atomicity
     * with ring changes.
     */
    private final ConcurrentHashMap<String, ConnectionPool> poolMap;

    /**
     * Registry of CacheNode objects by node ID.
     * Used to remove nodes from the ring (ring.removeNode needs the CacheNode object)
     * and for admin queries like getNodes().
     */
    private final ConcurrentHashMap<String, CacheNode> nodeRegistry;

    /**
     * Tracks consecutive ping failures per node ID for the health monitor.
     * Reset to 0 when a node pings successfully.
     * When it reaches HEALTH_FAILURE_THRESHOLD, the node is removed.
     */
    private final ConcurrentHashMap<String, Integer> failureCount;

    /**
     * Guards ring + poolMap + nodeRegistry as one atomic unit.
     *
     * READ LOCK:  get(), put(), delete() — concurrent routing allowed.
     * WRITE LOCK: addServer(), removeServer() — exclusive topology changes.
     *
     * Without this lock, a reader could route to a node whose pool
     * was just removed by a concurrent removeServer() call — NPE.
     */
    private final ReadWriteLock topologyLock;

    /** Connections per physical node. Immutable after construction. */
    private final int poolSizePerNode;

    /** Health check interval in milliseconds. 0 means disabled. */
    private final long healthCheckIntervalMs;

    /**
     * Background health check scheduler.
     * null if health checks are disabled.
     * Daemon thread — will not prevent JVM shutdown.
     */
    private final ScheduledExecutorService healthScheduler;

    /**
     * Closed flag. Checked at the start of every public operation.
     * AtomicBoolean for visibility without locking.
     */
    private final AtomicBoolean closed;

    // -------------------------------------------------------------------------
    // Constructor — private, use Builder
    // -------------------------------------------------------------------------

    /**
     * Private constructor called by Builder.build().
     * All validation is done in the Builder — constructor trusts builder values.
     *
     * @param builder The fully configured builder.
     * @throws IOException if any server declared in the builder is unreachable.
     */
    public ClusterCacheClient(Builder builder) throws IOException {
        this.ring                  = new ConsistentHashRing();
        this.poolMap               = new ConcurrentHashMap<>();
        this.nodeRegistry          = new ConcurrentHashMap<>();
        this.failureCount          = new ConcurrentHashMap<>();
        this.topologyLock          = new ReentrantReadWriteLock();
        this.poolSizePerNode       = builder.poolSizePerNode;
        this.healthCheckIntervalMs = builder.healthCheckIntervalMs;
        this.closed                = new AtomicBoolean(false);

        // Connect to all servers declared in the builder.
        // Failure here aborts construction — no partially-initialized cluster.
        for (Map.Entry<String, CacheNode> entry : builder.initialNodes.entrySet()) {
            addServer(
                    entry.getKey(),
                    entry.getValue().getHost(),
                    entry.getValue().getPort()
            );
        }

        // Start health monitor if configured.
        if (healthCheckIntervalMs > 0) {
            this.healthScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "jcache-health-monitor");
                t.setDaemon(true); // won't block JVM shutdown
                return t;
            });
            this.healthScheduler.scheduleAtFixedRate(
                    this::runHealthCheck,
                    healthCheckIntervalMs, // initial delay — don't ping at startup
                    healthCheckIntervalMs,
                    TimeUnit.MILLISECONDS
            );
            log.info(String.format(
                    "Health monitor started: interval=%dms, failureThreshold=%d",
                    healthCheckIntervalMs, HEALTH_FAILURE_THRESHOLD
            ));
        } else {
            this.healthScheduler = null;
        }
    }

    // -------------------------------------------------------------------------
    // Server topology management
    // -------------------------------------------------------------------------

    /**
     * Adds a cache server node to the cluster.
     *
     * What this does, in order:
     *   1. Creates a ConnectionPool to host:port (poolSizePerNode connections).
     *   2. Creates a CacheNode for ring placement.
     *   3. Adds the node to the consistent hash ring.
     *   4. Registers pool and node — routing lookups can now find them.
     *
     * ORDERING — pool created BEFORE ring insertion.
     * If pool creation fails (server unreachable), the ring is never modified.
     * No partial state: either the node is fully ready or it's not added at all.
     *
     * RE-ADD BEHAVIOUR:
     * If a node with this ID already exists, the old pool is closed and a new
     * one is created. Use this to reconnect to a restarted server.
     *
     * @param nodeId Stable unique identifier matching the server-side node ID.
     * @param host   Server hostname or IP address.
     * @param port   Server TCP port.
     * @throws IOException              if ConnectionPool cannot connect.
     * @throws IllegalArgumentException if any parameter is null/invalid.
     * @throws IllegalStateException    if this client has been closed.
     */
    public void addServer(String nodeId, String host, int port) throws IOException {
        ensureOpen();
        validateNodeParams(nodeId, host, port);

        topologyLock.writeLock().lock();
        try {
            // If this node already exists, replace it cleanly.
            if (poolMap.containsKey(nodeId)) {
                log.info("Re-adding existing node [" + nodeId + "] — closing old pool.");
                shutdownPool(nodeId);
                CacheNode old = nodeRegistry.remove(nodeId);
                if (old != null) {
                    try { ring.removeNode(old); } catch (Exception ignored) {}
                }
            }

            // Step 1: Create pool — if this throws, ring stays unchanged.
            ConnectionPool pool = createPool(host, port, nodeId);

            // Step 2: Build CacheNode for ring placement.
            CacheNode node = new CacheNode(nodeId, host, port);

            // Step 3: Add to ring — keys now route here.
            ring.addNode(node);

            // Step 4: Register — routing lookups resolve this node.
            poolMap.put(nodeId, pool);
            nodeRegistry.put(nodeId, node);
            failureCount.put(nodeId, 0);

            log.info(String.format(
                    "Added server [%s] at %s:%d | pool=%d | ring=%d nodes | dist=%s",
                    nodeId, host, port, poolSizePerNode,
                    ring.getPhysicalNodeCount(), ring.getKeyDistribution()
            ));

        } finally {
            topologyLock.writeLock().unlock();
        }
    }

    /**
     * Removes a cache server node from the cluster.
     *
     * What this does, in order:
     *   1. Removes node from ring — keys now route to next clockwise node.
     *   2. Closes the connection pool — no new borrows accepted.
     *   3. Removes from registry and failure tracking.
     *
     * ORDERING — ring removal BEFORE pool closure.
     * New requests stop routing here immediately (step 1).
     * In-flight requests that already borrowed a connection complete normally.
     *
     * Safe to call for unknown nodes — logs a warning and returns.
     *
     * @param nodeId The ID of the node to remove.
     * @throws IllegalArgumentException if nodeId is null or blank.
     * @throws IllegalStateException    if this client has been closed.
     */
    public void removeServer(String nodeId) {
        ensureOpen();
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be null or blank");
        }

        topologyLock.writeLock().lock();
        try {
            if (!poolMap.containsKey(nodeId)) {
                log.warning("removeServer() called for unknown node [" + nodeId + "] — no-op.");
                return;
            }

            // Step 1: Remove from ring first.
            CacheNode node = nodeRegistry.get(nodeId);
            if (node != null) {
                try { ring.removeNode(node); } catch (Exception e) {
                    log.fine("Exception removing node from ring: " + e.getMessage());
                }
            }

            // Step 2: Close pool and clean up registries.
            shutdownPool(nodeId);
            nodeRegistry.remove(nodeId);
            failureCount.remove(nodeId);

            log.info(String.format(
                    "Removed server [%s]. Ring now has %d nodes.",
                    nodeId, ring.getPhysicalNodeCount()
            ));

        } finally {
            topologyLock.writeLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Cache operations — the public API
    // -------------------------------------------------------------------------

    /**
     * Retrieves the value for a key, routing to the correct node automatically.
     *
     * Returns null on cache miss (key absent or expired).
     * Retries once on the next clockwise node if the primary node fails.
     *
     * @param key The cache key. Cannot be null or contain spaces.
     * @return The cached value, or null if not found.
     * @throws ClusterOperationException if all routing attempts fail.
     * @throws IllegalStateException     if this client has been closed.
     */
    public String get(String key) {
        ensureOpen();
        ensureNotEmpty();
        validateKey(key);
        return executeWithRetry(key, client -> client.get(key));
    }

    /**
     * Stores a key-value pair with no TTL.
     * Lives until evicted by the server's eviction policy (LRU/LFU/ARC).
     *
     * @param key   The cache key. Cannot be null or contain spaces.
     * @param value The value to store. Cannot be null.
     * @throws ClusterOperationException if all routing attempts fail.
     * @throws IllegalStateException     if this client has been closed.
     */
    public void put(String key, String value) {
        ensureOpen();
        ensureNotEmpty();
        validateKey(key);
        validateValue(value);
        executeWithRetry(key, client -> {
            client.put(key, value);
            return null;
        });
    }

    /**
     * Stores a key-value pair with a TTL in seconds.
     *
     * @param key        The cache key. Cannot be null or contain spaces.
     * @param value      The value to store. Cannot be null.
     * @param ttlSeconds Time-to-live in seconds. 0 means no expiry.
     * @throws IllegalArgumentException  if ttlSeconds is negative.
     * @throws ClusterOperationException if all routing attempts fail.
     * @throws IllegalStateException     if this client has been closed.
     */
    public void put(String key, String value, long ttlSeconds) {
        ensureOpen();
        ensureNotEmpty();
        validateKey(key);
        validateValue(value);
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("TTL cannot be negative: " + ttlSeconds);
        }
        executeWithRetry(key, client -> {
            client.put(key, value, ttlSeconds);
            return null;
        });
    }

    /**
     * Deletes a key from the cache.
     * Idempotent — no error if the key doesn't exist.
     *
     * @param key The cache key to delete.
     * @throws ClusterOperationException if all routing attempts fail.
     * @throws IllegalStateException     if this client has been closed.
     */
    public void delete(String key) {
        ensureOpen();
        ensureNotEmpty();
        validateKey(key);
        executeWithRetry(key, client -> {
            client.delete(key);
            return null;
        });
    }

    /**
     * Flushes ALL entries from EVERY node in the cluster (fan-out operation).
     *
     * Unlike get/put/delete which route to one node, FLUSH is sent to all nodes.
     * Partial failures are reported in the returned map rather than thrown,
     * so one unreachable node doesn't prevent flushing the others.
     *
     * @return Map of nodeId → error message for nodes that failed to flush.
     *         Empty map means all nodes flushed successfully.
     * @throws IllegalStateException if this client has been closed.
     */
    public Map<String, String> flushAll() {
        ensureOpen();
        Map<String, String> failures = new LinkedHashMap<>();

        topologyLock.readLock().lock();
        try {
            for (Map.Entry<String, ConnectionPool> entry : poolMap.entrySet()) {
                String         nodeId = entry.getKey();
                ConnectionPool pool   = entry.getValue();
                CacheClient    conn   = null;
                try {
                    conn = pool.acquire();
                    conn.flush();
                } catch (Exception e) {
                    failures.put(nodeId, e.getMessage());
                    log.warning("FLUSH failed on node [" + nodeId + "]: " + e.getMessage());
                } finally {
                    if (conn != null) pool.release(conn);
                }
            }
        } finally {
            topologyLock.readLock().unlock();
        }

        if (failures.isEmpty()) {
            log.info("flushAll() succeeded on all " + poolMap.size() + " nodes.");
        }
        return Collections.unmodifiableMap(failures);
    }

    /**
     * Collects and aggregates STATS from every node in the cluster.
     *
     * Each node's stats are prefixed with its node ID.
     * Cluster-wide totals are computed and included under "cluster.*" keys.
     *
     * Example output keys:
     *   node-1.hits, node-1.misses, node-2.hits, node-2.misses,
     *   cluster.nodes, cluster.reachable, cluster.totalHits,
     *   cluster.totalMisses, cluster.hitRate
     *
     * Unreachable nodes appear as: nodeId.status → "UNREACHABLE"
     *
     * @return Aggregated stats map. Never null.
     * @throws IllegalStateException if this client has been closed.
     */
    public Map<String, String> clusterStats() {
        ensureOpen();
        Map<String, String> result      = new LinkedHashMap<>();
        long                totalHits   = 0;
        long                totalMisses = 0;
        int                 reachable   = 0;

        topologyLock.readLock().lock();
        try {
            for (Map.Entry<String, ConnectionPool> entry : poolMap.entrySet()) {
                String         nodeId = entry.getKey();
                ConnectionPool pool   = entry.getValue();
                CacheClient    conn   = null;
                try {
                    conn = pool.acquire();
                    Map<String, String> nodeStats = conn.stats();
                    for (Map.Entry<String, String> stat : nodeStats.entrySet()) {
                        result.put(nodeId + "." + stat.getKey(), stat.getValue());
                    }
                    totalHits   += parseLong(nodeStats.get("hits"));
                    totalMisses += parseLong(nodeStats.get("misses"));
                    reachable++;
                } catch (Exception e) {
                    result.put(nodeId + ".status", "UNREACHABLE");
                    log.warning("Failed to get stats from [" + nodeId + "]: " + e.getMessage());
                } finally {
                    if (conn != null) pool.release(conn);
                }
            }
        } finally {
            topologyLock.readLock().unlock();
        }

        result.put("cluster.nodes",       String.valueOf(poolMap.size()));
        result.put("cluster.reachable",   String.valueOf(reachable));
        result.put("cluster.totalHits",   String.valueOf(totalHits));
        result.put("cluster.totalMisses", String.valueOf(totalMisses));
        long total = totalHits + totalMisses;
        if (total > 0) {
            result.put("cluster.hitRate",
                    String.format("%.2f%%", (double) totalHits / total * 100.0));
        }
        return Collections.unmodifiableMap(result);
    }

    // -------------------------------------------------------------------------
    // Cluster inspection
    // -------------------------------------------------------------------------

    /**
     * Returns all currently registered server nodes.
     *
     * @return Unmodifiable list of registered CacheNode instances.
     */
    public List<CacheNode> getNodes() {
        topologyLock.readLock().lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(nodeRegistry.values()));
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    /** @return Number of nodes currently in the cluster. */
    public int getNodeCount() {
        return ring.getPhysicalNodeCount();
    }

    /** @return true if no nodes are registered. */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    /**
     * Returns which node a given key would route to, without executing any operation.
     * Useful for debugging cluster distribution.
     *
     * @param key The cache key.
     * @return The CacheNode that would handle this key.
     */
    public CacheNode getRoutingTarget(String key) {
        ensureNotEmpty();
        validateKey(key);
        topologyLock.readLock().lock();
        try {
            return ring.getNode(key);
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    /**
     * Returns the approximate key distribution across all nodes.
     * Each node should ideally own ~1/N of the key space.
     *
     * @return Map from nodeId → percentage of key space (0.0–100.0).
     */
    public Map<String, Double> getKeyDistribution() {
        topologyLock.readLock().lock();
        try {
            return ring.getKeyDistribution();
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    /**
     * Returns connection pool stats per node.
     * Useful for diagnosing pool exhaustion or connection leaks.
     *
     * @return Map of nodeId → "active=N idle=N size=N".
     */
    public Map<String, String> getPoolStats() {
        topologyLock.readLock().lock();
        try {
            Map<String, String> stats = new LinkedHashMap<>();
            for (Map.Entry<String, ConnectionPool> entry : poolMap.entrySet()) {
                ConnectionPool p = entry.getValue();
                stats.put(entry.getKey(), String.format(
                        "active=%d idle=%d size=%d",
                        p.getActiveCount(), p.getIdleCount(), p.getPoolSize()
                ));
            }
            return Collections.unmodifiableMap(stats);
        } finally {
            topologyLock.readLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // AutoCloseable — graceful shutdown
    // -------------------------------------------------------------------------

    /**
     * Shuts down this ClusterCacheClient cleanly.
     *
     * Shutdown sequence:
     *   1. Mark as closed (AtomicBoolean CAS — only first call proceeds).
     *   2. Stop health monitor (if running).
     *   3. Remove all nodes from ring.
     *   4. Drain and close all connection pools.
     *
     * In-flight operations complete normally.
     * New operations after close() throw IllegalStateException.
     * Idempotent — safe to call multiple times.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return; // already closed
        }

        log.info("ClusterCacheClient shutting down. Pools: " + poolMap.size());

        // 1. Stop health monitor.
        if (healthScheduler != null) {
            healthScheduler.shutdown();
            try {
                healthScheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                healthScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 2. Close everything under write lock.
        topologyLock.writeLock().lock();
        try {
            // Remove all nodes from ring.
            for (CacheNode node : new ArrayList<>(nodeRegistry.values())) {
                try { ring.removeNode(node); } catch (Exception ignored) {}
            }
            // Close all pools.
            for (String nodeId : new ArrayList<>(poolMap.keySet())) {
                shutdownPool(nodeId);
            }
            nodeRegistry.clear();
            failureCount.clear();
        } finally {
            topologyLock.writeLock().unlock();
        }

        log.info("ClusterCacheClient shutdown complete.");
    }

    // -------------------------------------------------------------------------
    // Builder pattern
    // -------------------------------------------------------------------------

    /**
     * Returns a new Builder for constructing a ClusterCacheClient.
     *
     * @return A fresh Builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder for ClusterCacheClient.
     *
     * Validates all parameters eagerly (in each setter, not at build time).
     * build() is where actual network connections are made.
     *
     * Example:
     *   ClusterCacheClient client = ClusterCacheClient.builder()
     *       .addServer("node-1", "localhost", 6379)
     *       .addServer("node-2", "localhost", 6380)
     *       .poolSizePerNode(10)
     *       .withHealthCheck(5000)
     *       .build();
     */
    public static final class Builder {

        // LinkedHashMap preserves server declaration order.
        private final LinkedHashMap<String, CacheNode> initialNodes = new LinkedHashMap<>();
        private int  poolSizePerNode       = DEFAULT_POOL_SIZE_PER_NODE;
        private long healthCheckIntervalMs = 0; // 0 = disabled

        private Builder() {}

        /**
         * Registers a server to connect to at build() time.
         *
         * @param nodeId Stable unique ID for this node.
         * @param host   Server hostname or IP.
         * @param port   Server TCP port (1–65535).
         * @return This builder (fluent).
         */
        public Builder addServer(String nodeId, String host, int port) {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("Node ID cannot be null or blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("Host cannot be null or blank");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException(
                        "Port must be in [1, 65535], got: " + port);
            }
            initialNodes.put(nodeId, new CacheNode(nodeId, host, port));
            return this;
        }

        /**
         * Sets the number of TCP connections per physical node.
         * Default: 5. Higher values increase throughput at the cost of server resources.
         *
         * @param size Pool size. Must be >= 1.
         * @return This builder (fluent).
         */
        public Builder poolSizePerNode(int size) {
            if (size < 1) {
                throw new IllegalArgumentException(
                        "Pool size must be >= 1, got: " + size);
            }
            this.poolSizePerNode = size;
            return this;
        }

        /**
         * Enables background health checking.
         * Nodes failing 3 consecutive pings are automatically removed from the ring.
         *
         * @param intervalMs Ping interval in milliseconds. Must be > 0.
         * @return This builder (fluent).
         */
        public Builder withHealthCheck(long intervalMs) {
            if (intervalMs <= 0) {
                throw new IllegalArgumentException(
                        "Health check interval must be positive, got: " + intervalMs);
            }
            this.healthCheckIntervalMs = intervalMs;
            return this;
        }

        /**
         * Builds the ClusterCacheClient and connects to all registered servers.
         *
         * Throws IOException if any server is unreachable.
         * For fault-tolerant initialization (skip unreachable servers), build
         * an empty client and call addServer() individually, catching IOException.
         *
         * @return A ready-to-use ClusterCacheClient.
         * @throws IOException if any server connection fails.
         */
        public ClusterCacheClient build() throws IOException {
            return new ClusterCacheClient(this);
        }
    }

    // -------------------------------------------------------------------------
    // Internal: routing and retry
    // -------------------------------------------------------------------------

    /**
     * Functional interface for operations executed against a CacheClient.
     *
     * @param <T> Return type: String for get(), null for void operations.
     */
    @FunctionalInterface
    private interface PooledOperation<T> {
        T execute(CacheClient client) throws IOException;
    }

    /**
     * Executes a cache operation with one retry on node failure.
     *
     * RETRY ALGORITHM:
     *   Attempt 1: Route to primary node. Execute. On success, return.
     *   Attempt 2: On failure, route to next clockwise node. Execute.
     *   After 2 failures: throw ClusterOperationException.
     *
     * Read lock is held for both attempts. addServer/removeServer
     * must wait for the read lock before changing topology.
     * This means in-flight retries always see a consistent ring + poolMap.
     *
     * Connection is borrowed and released within each attempt's try/finally,
     * so a broken connection is returned to the pool before we try another node.
     *
     * @param key       The cache key for ring-based routing.
     * @param operation The operation to execute.
     * @param <T>       Return type.
     * @return The result, or null for void operations.
     * @throws ClusterOperationException if all attempts fail.
     */
    private <T> T executeWithRetry(String key, PooledOperation<T> operation) {
        topologyLock.readLock().lock();
        try {
            Set<String> triedNodes = new HashSet<>();
            Exception   lastError  = null;

            for (int attempt = 0; attempt < 2; attempt++) {
                // Route key to the next untried node.
                CacheNode target = routeSkipping(key, triedNodes);
                if (target == null) break; // no more nodes to try

                String         nodeId = target.getId();
                ConnectionPool pool   = poolMap.get(nodeId);

                if (pool == null) {
                    // Pool disappeared (race with removeServer) — skip.
                    triedNodes.add(nodeId);
                    continue;
                }

                CacheClient conn = null;
                try {
                    conn = pool.acquire();
                    T result = operation.execute(conn);

                    if (attempt > 0) {
                        log.info(String.format(
                                "Failover success: key [%s] served by [%s] on attempt %d",
                                key, nodeId, attempt + 1
                        ));
                    }
                    return result;

                } catch (Exception e) {
                    lastError = e;
                    triedNodes.add(nodeId);
                    log.warning(String.format(
                            "Attempt %d failed on node [%s] for key [%s]: %s",
                            attempt + 1, nodeId, key, e.getMessage()
                    ));
                } finally {
                    // ALWAYS release — pool handles broken connections transparently.
                    if (conn != null) pool.release(conn);
                }
            }

            throw new ClusterOperationException(
                    String.format("All routing attempts failed for key [%s]. Last error: %s",
                            key, lastError != null ? lastError.getMessage() : "no nodes available"),
                    lastError
            );

        } finally {
            topologyLock.readLock().unlock();
        }
    }

    /**
     * Routes a key to the owning node, skipping node IDs in the exclusion set.
     *
     * Primary: ring.getNode(key) — standard consistent hash lookup.
     * Failover: scan active nodes for any not in skipNodeIds.
     * Returns null if all nodes are exhausted.
     *
     * @param key         The cache key.
     * @param skipNodeIds Already-tried node IDs to skip.
     * @return Next available CacheNode, or null if all nodes tried.
     */
    private CacheNode routeSkipping(String key, Set<String> skipNodeIds) {
        if (ring.isEmpty()) return null;

        // Primary lookup.
        CacheNode primary = ring.getNode(key);
        if (!skipNodeIds.contains(primary.getId())) {
            return primary;
        }

        // Primary was tried — find any other active node.
        for (CacheNode node : ring.getActiveNodes()) {
            if (!skipNodeIds.contains(node.getId())) {
                return node;
            }
        }

        return null; // all nodes exhausted
    }

    // -------------------------------------------------------------------------
    // Internal: health monitoring
    // -------------------------------------------------------------------------

    /**
     * Health check task run by the healthScheduler thread.
     *
     * For each node:
     *   - Borrow one connection, send PING.
     *   - Success: reset failure counter.
     *   - Failure: increment counter. Remove node if threshold reached.
     *
     * Nodes to remove are collected first (under read lock), then
     * removed via removeServer() (which acquires write lock).
     * This avoids lock upgrading (read → write), which is not allowed
     * by ReentrantReadWriteLock.
     *
     * Catches Throwable to keep the scheduler alive despite any RuntimeException.
     */
    private void runHealthCheck() {
        try {
            List<String> toRemove = new ArrayList<>();

            topologyLock.readLock().lock();
            try {
                for (Map.Entry<String, ConnectionPool> entry : poolMap.entrySet()) {
                    String         nodeId = entry.getKey();
                    ConnectionPool pool   = entry.getValue();

                    boolean healthy = pingNode(pool, nodeId);

                    if (healthy) {
                        failureCount.put(nodeId, 0);
                    } else {
                        int failures = failureCount.merge(nodeId, 1, Integer::sum);
                        log.warning(String.format(
                                "Health check failed for [%s] (%d/%d)",
                                nodeId, failures, HEALTH_FAILURE_THRESHOLD
                        ));
                        if (failures >= HEALTH_FAILURE_THRESHOLD) {
                            toRemove.add(nodeId);
                        }
                    }
                }
            } finally {
                topologyLock.readLock().unlock();
            }

            // Remove failed nodes outside the read lock.
            for (String nodeId : toRemove) {
                log.severe(String.format(
                        "Node [%s] failed %d consecutive health checks — removing.",
                        nodeId, HEALTH_FAILURE_THRESHOLD
                ));
                removeServer(nodeId);
            }

        } catch (Throwable t) {
            // Must catch Throwable — uncaught RuntimeException kills the scheduler.
            log.severe("Health check error: " + t.getMessage());
        }
    }

    /**
     * Pings one node by borrowing a single connection from its pool.
     *
     * @param pool   The node's connection pool.
     * @param nodeId Node ID for logging.
     * @return true if the server responds with PONG.
     */
    private boolean pingNode(ConnectionPool pool, String nodeId) {
        CacheClient conn = null;
        try {
            conn = pool.acquireWithTimeout(1000, TimeUnit.MILLISECONDS);
            return conn.ping();
        } catch (Exception e) {
            log.fine("Ping failed for [" + nodeId + "]: " + e.getMessage());
            return false;
        } finally {
            if (conn != null) pool.release(conn);
        }
    }

    // -------------------------------------------------------------------------
    // Internal: pool lifecycle
    // -------------------------------------------------------------------------

    /**
     * Creates a ConnectionPool for a node.
     * Wraps RuntimeException from ConnectionPool in IOException so callers
     * can handle it uniformly.
     *
     * @param host   Server hostname.
     * @param port   Server port.
     * @param nodeId Node ID for error messages.
     * @return A ready ConnectionPool.
     * @throws IOException if the pool cannot connect.
     */
    private ConnectionPool createPool(String host, int port, String nodeId) throws IOException {
        try {
            return new ConnectionPool(host, port, poolSizePerNode);
        } catch (RuntimeException e) {
            throw new IOException(String.format(
                    "Failed to create pool for node [%s] at %s:%d: %s",
                    nodeId, host, port, e.getMessage()), e);
        }
    }

    /**
     * Closes and removes a ConnectionPool from poolMap.
     * Must be called with writeLock held (or during single-threaded close()).
     *
     * @param nodeId The node whose pool to shut down.
     */
    private void shutdownPool(String nodeId) {
        ConnectionPool pool = poolMap.remove(nodeId);
        if (pool != null) {
            try { pool.close(); } catch (Exception e) {
                log.fine("Exception closing pool [" + nodeId + "]: " + e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Internal: guards and validators
    // -------------------------------------------------------------------------

    /** Throws if this client has been closed. */
    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException(
                    "ClusterCacheClient is closed. Create a new instance.");
        }
    }

    /** Throws if no nodes are registered. */
    private void ensureNotEmpty() {
        if (ring.isEmpty()) {
            throw new ClusterOperationException(
                    "No servers registered. Call addServer() or use the Builder.");
        }
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (key.contains(" ")) {
            throw new IllegalArgumentException(
                    "Key cannot contain spaces (wire protocol delimiter): '" + key + "'");
        }
    }

    private void validateValue(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Value cannot be null or empty");
        }
    }

    private void validateNodeParams(String nodeId, String host, int port) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be null or blank");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be null or blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "Port must be in [1, 65535], got: " + port);
        }
    }

    private long parseLong(String value) {
        if (value == null) return 0;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // -------------------------------------------------------------------------
    // ClusterOperationException
    // -------------------------------------------------------------------------

    /**
     * Thrown when ClusterCacheClient cannot complete an operation after all retries.
     *
     * Causes:
     *   - Ring is empty (no servers registered)
     *   - All available nodes failed during retry
     *   - Network partition (all nodes unreachable)
     *
     * Unchecked exception — routing failures are infrastructure problems
     * not recoverable at the call site. Callers should alert, circuit-break,
     * or degrade gracefully. Same pattern as Jedis's JedisClusterException.
     */
    public static class ClusterOperationException extends RuntimeException {

        public ClusterOperationException(String message) {
            super(message);
        }

        public ClusterOperationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // -------------------------------------------------------------------------
    // toString
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "ClusterCacheClient{nodes=%d, poolPerNode=%d, healthCheck=%s, closed=%b, dist=%s}",
                getNodeCount(),
                poolSizePerNode,
                healthCheckIntervalMs > 0 ? healthCheckIntervalMs + "ms" : "disabled",
                closed.get(),
                ring.getKeyDistribution()
        );
    }
}