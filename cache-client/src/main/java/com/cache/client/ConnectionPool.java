package com.cache.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ConnectionPool manages a fixed-size pool of reusable TCP connections
 * to a single CacheServer instance.
 *
 * WHY A CONNECTION POOL?
 * Opening a TCP connection involves a 3-way handshake (~1ms on localhost,
 * ~10-100ms over a network). If every cache operation opened and closed its
 * own connection, that handshake latency would dominate. A pool pre-opens N
 * connections at startup and lends them to callers. The caller does its
 * operation and returns the connection. Next caller gets it immediately —
 * zero handshake cost. This is exactly how HikariCP and Jedis pool work.
 *
 * DESIGN:
 *   - Fixed pool size: N connections created at startup, never more.
 *   - BlockingQueue as the pool: thread-safe, supports blocking acquire.
 *   - acquire() blocks if all connections are in use, up to a timeout.
 *   - release() returns the connection to the queue for reuse.
 *   - Unhealthy connections are replaced transparently on release.
 *
 * THREAD SAFETY:
 *   ArrayBlockingQueue is fully thread-safe. Multiple threads can call
 *   acquire() and release() concurrently without additional locking.
 *   AtomicInteger for activeCount avoids lock overhead on monitoring reads.
 *
 * USAGE:
 *   ConnectionPool pool = new ConnectionPool("localhost", 6379, 5);
 *   CacheClient conn = pool.acquire();
 *   try {
 *       conn.put("key", "value");
 *   } finally {
 *       pool.release(conn);   // ALWAYS release in a finally block
 *   }
 *   pool.close();             // at application shutdown
 */
public class ConnectionPool {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /**
     * Default timeout for acquire() when no explicit timeout is given.
     * 5 seconds is generous — if no connection is free in 5 seconds,
     * the caller has a problem larger than pool configuration.
     */
    private static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 5000L;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Server hostname — all connections in this pool go to the same host. */
    private final String host;

    /** Server port — all connections in this pool go to the same port. */
    private final int port;

    /** Maximum number of connections maintained by this pool. */
    private final int poolSize;

    /**
     * The pool itself: a bounded queue of idle connections.
     *
     * BlockingQueue semantics used here:
     *   poll(timeout, unit) — take head, waiting up to timeout. Returns null on timeout.
     *   offer(conn)         — add to tail. Returns false if full (handled as bug guard).
     *
     * ArrayBlockingQueue chosen over LinkedBlockingQueue because:
     *   1. Fixed capacity matches our fixed pool size — bounded by design.
     *   2. Array layout is more CPU-cache friendly than linked nodes.
     */
    private final BlockingQueue<CacheClient> availableConnections;

    /**
     * All connections ever created — used for cleanup in close().
     * Separate from availableConnections because connections currently
     * in use (acquired) are not in the queue.
     */
    private final List<CacheClient> allConnections;

    /**
     * Count of connections currently lent out (acquired but not released).
     * AtomicInteger for lock-free stat reads.
     *
     * Invariant when pool is healthy:
     *   activeCount + availableConnections.size() == poolSize
     */
    private final AtomicInteger activeCount;

    /**
     * Closed flag. After close(), acquire() throws immediately.
     * volatile ensures visibility across threads without locking.
     */
    private volatile boolean closed;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a ConnectionPool and eagerly opens all N connections.
     *
     * Eager (not lazy) initialization front-loads the TCP handshake cost
     * to startup rather than spreading it across the first N requests.
     * This matches what HikariCP and Jedis pool do by default.
     *
     * @param host     Hostname of the CacheServer (e.g., "localhost").
     * @param port     Port the CacheServer listens on (e.g., 6379).
     * @param poolSize Number of connections to maintain. Must be >= 1.
     * @throws IllegalArgumentException if arguments are invalid.
     * @throws RuntimeException         if any connection fails to open at startup.
     */
    public ConnectionPool(String host, int port, int poolSize) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be null or blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be in [1, 65535], got: " + port);
        }
        if (poolSize < 1) {
            throw new IllegalArgumentException("Pool size must be >= 1, got: " + poolSize);
        }

        this.host                 = host;
        this.port                 = port;
        this.poolSize             = poolSize;
        this.activeCount          = new AtomicInteger(0);
        this.closed               = false;
        this.availableConnections = new ArrayBlockingQueue<>(poolSize);
        this.allConnections       = new ArrayList<>(poolSize);

        initializeConnections();
    }

    // -------------------------------------------------------------------------
    // Core API
    // -------------------------------------------------------------------------

    /**
     * Acquires a connection from the pool, blocking up to DEFAULT_ACQUIRE_TIMEOUT_MS
     * (5 seconds) if all connections are currently in use.
     *
     * CONTRACT: every acquire() MUST be paired with a release() in a finally block.
     *
     * @return A healthy, connected CacheClient ready for use.
     * @throws TimeoutException      if no connection is available within 5 seconds.
     * @throws IllegalStateException if the pool has been closed.
     * @throws InterruptedException  if the thread is interrupted while waiting.
     */
    public CacheClient acquire() throws TimeoutException, InterruptedException {
        return acquireWithTimeout(DEFAULT_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Acquires a connection from the pool, blocking up to the specified timeout.
     *
     * This is the method exercised by CacheClientTest.testConnectionPool_exhaustionThrows:
     *   pool.acquireWithTimeout(100, TimeUnit.MILLISECONDS)
     *   → throws TimeoutException when pool is exhausted and timeout elapses.
     *
     * @param timeout  Maximum time to wait for a connection.
     * @param timeUnit Unit for the timeout.
     * @return A healthy, connected CacheClient.
     * @throws TimeoutException      if no connection becomes available in time.
     * @throws IllegalStateException if the pool has been closed.
     * @throws InterruptedException  if the thread is interrupted while waiting.
     */
    public CacheClient acquireWithTimeout(long timeout, TimeUnit timeUnit)
            throws TimeoutException, InterruptedException {

        if (closed) {
            throw new IllegalStateException("ConnectionPool is closed");
        }

        // poll(timeout, unit): removes and returns the head of the queue,
        // waiting up to timeout if the queue is currently empty.
        // Returns null if the timeout elapses before a connection is available.
        CacheClient connection = availableConnections.poll(timeout, timeUnit);

        if (connection == null) {
            throw new TimeoutException(String.format(
                    "No connection available within %d %s. Pool size=%d, active=%d",
                    timeout, timeUnit, poolSize, activeCount.get()
            ));
        }

        // Health check — the connection might have gone stale if the server
        // restarted or a firewall idle-timeout closed the socket silently.
        if (!isConnectionHealthy(connection)) {
            connection = replaceConnection(connection);
            if (connection == null) {
                throw new RuntimeException("Failed to replace broken connection");
            }
        }

        activeCount.incrementAndGet();
        return connection;
    }

    /**
     * Returns a connection to the pool after use.
     *
     * If the connection is broken (server closed it during the operation),
     * release() replaces it with a fresh one before re-queuing, so the
     * next acquire() always receives a healthy connection.
     *
     * @param connection The connection to return. Must have come from acquire().
     */
    public void release(CacheClient connection) {
        if (connection == null || closed) return;

        activeCount.decrementAndGet();

        // Replace broken connections transparently.
        if (!isConnectionHealthy(connection)) {
            try { connection.close(); } catch (Exception ignored) {}
            connection = createConnection();
            if (connection == null) return; // pool temporarily shrinks; acceptable
        }

        // Return to the available queue.
        // offer() returns false if the queue is full, which indicates a
        // double-release bug. Close the extra connection rather than leak it.
        boolean offered = availableConnections.offer(connection);
        if (!offered) {
            try { connection.close(); } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Monitoring
    // -------------------------------------------------------------------------

    /**
     * Returns the number of connections currently lent out (in use by callers).
     *
     * Used by CacheClientTest:
     *   assertTrue(pool.getActiveCount() <= poolSize)
     *
     * @return Number of acquired-but-not-released connections.
     */
    public int getActiveCount() {
        return activeCount.get();
    }

    /**
     * Returns the number of connections currently idle in the pool.
     *
     * @return Number of available connections ready for acquire().
     */
    public int getIdleCount() {
        return availableConnections.size();
    }

    /**
     * Returns the configured pool capacity.
     *
     * @return Pool size set at construction.
     */
    public int getPoolSize() {
        return poolSize;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Closes all idle connections and marks this pool as shut down.
     *
     * Connections still in use (acquired but not released) are NOT forcibly
     * closed — the caller is responsible for those. In practice, always call
     * close() after all acquire/release cycles are complete (e.g., in @AfterAll).
     *
     * Idempotent — safe to call multiple times.
     */
    public void close() {
        if (closed) return;
        closed = true;

        CacheClient conn;
        while ((conn = availableConnections.poll()) != null) {
            try { conn.close(); } catch (Exception ignored) {}
        }
        allConnections.clear();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Opens all pool connections eagerly at construction time.
     */
    private void initializeConnections() {
        for (int i = 0; i < poolSize; i++) {
            CacheClient connection = createConnection();
            if (connection == null) {
                close(); // clean up what we opened so far
                throw new RuntimeException(String.format(
                        "Failed to open connection %d/%d to %s:%d during pool initialization",
                        i + 1, poolSize, host, port
                ));
            }
            allConnections.add(connection);
            availableConnections.offer(connection);
        }
    }

    /**
     * Opens a single new TCP connection to the server.
     *
     * @return A connected CacheClient, or null if the attempt fails.
     */
    private CacheClient createConnection() {
        try {
            return new CacheClient(host, port);
        } catch (Exception e) {
            System.err.printf("[ConnectionPool] Failed to create connection to %s:%d — %s%n",
                    host, port, e.getMessage());
            return null;
        }
    }

    /**
     * Checks whether a connection is alive using a PING command.
     * A broken connection (dropped socket, server restart) returns false here.
     *
     * @param connection The connection to check.
     * @return true if the server responds to PING with PONG.
     */
    private boolean isConnectionHealthy(CacheClient connection) {
        try {
            return connection != null && connection.ping();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Closes a broken connection and opens a fresh replacement.
     * Updates allConnections to track the new connection.
     *
     * @param broken The unhealthy connection to replace.
     * @return A new healthy CacheClient, or null if replacement fails.
     */
    private CacheClient replaceConnection(CacheClient broken) {
        try { broken.close(); } catch (Exception ignored) {}

        CacheClient replacement = createConnection();
        if (replacement != null) {
            synchronized (allConnections) {
                allConnections.remove(broken);
                allConnections.add(replacement);
            }
        }
        return replacement;
    }

    // -------------------------------------------------------------------------
    // toString
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "ConnectionPool{host='%s', port=%d, size=%d, active=%d, idle=%d, closed=%b}",
                host, port, poolSize, activeCount.get(), availableConnections.size(), closed
        );
    }
}