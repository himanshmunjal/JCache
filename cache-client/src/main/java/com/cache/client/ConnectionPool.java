package com.cache.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ConnectionPool manages a fixed pool of CacheClient connections,
 * allowing multiple threads to use the cache server concurrently
 * without paying TCP handshake overhead on every request.
 *
 * PROBLEM THIS SOLVES:
 *   Each new CacheClient("localhost", 6379) pays:
 *     - TCP 3-way handshake:     ~0.5–2ms on localhost, ~10–100ms on network
 *     - Socket buffer allocation: small but adds up
 *   At 10,000 requests/second with a new connection per request:
 *     10,000 × 1ms = 10 seconds of handshake overhead per second of work.
 *   A pool creates N connections once at startup and reuses them.
 *   Subsequent borrows/returns are just ArrayBlockingQueue.poll() / offer() — nanoseconds.
 *
 * HOW IT WORKS:
 *   1. At construction, N CacheClient connections are created and placed in a queue.
 *   2. A caller calls acquire() — blocks until a connection is available.
 *   3. Caller uses the connection (get/put/delete).
 *   4. Caller calls release(client) — returns it to the queue.
 *   5. If a connection is unhealthy (ping fails), it's discarded and replaced.
 *
 * DATA STRUCTURE — ArrayBlockingQueue:
 *   - Thread-safe: no external synchronization needed
 *   - Blocking: acquire() waits if pool is empty (all connections in use)
 *   - Bounded: prevents unlimited connection growth
 *   - FIFO: connections are reused in order, giving roughly even load
 *
 * WHY NOT SYNCHRONIZED LIST?
 *   A synchronized list + wait/notify requires manual locking.
 *   ArrayBlockingQueue is purpose-built for producer-consumer patterns,
 *   uses efficient internal locking (two separate locks for head/tail),
 *   and is battle-tested in production systems. No reason to reinvent it.
 *
 * HEALTH CHECKING:
 *   On release(), we PING the connection before returning it to the pool.
 *   If PING fails (server closed the connection, network blip), we discard
 *   the connection and create a fresh one.
 *   This "validate on return" strategy ensures the pool never hands out
 *   a stale connection to a caller.
 *
 *   Alternative: "validate on borrow" — check health when acquiring, not releasing.
 *   Trade-off: validate-on-return catches failures earlier (right when they happen)
 *   and doesn't add latency to the acquire() path. That's why we chose it.
 *
 * TYPICAL USAGE:
 *   // Create pool with 8 connections, 5-second acquire timeout
 *   ConnectionPool pool = new ConnectionPool("localhost", 6379, 8, 5000);
 *
 *   // In a request handler (called from multiple threads):
 *   CacheClient client = pool.acquire();
 *   try {
 *       String value = client.get("session:user1");
 *       // ... use value ...
 *   } finally {
 *       pool.release(client); // ALWAYS release in finally block
 *   }
 *
 *   // Shutdown (e.g., in server shutdown hook):
 *   pool.close();
 *
 * THREAD SAFETY:
 *   ConnectionPool is fully thread-safe.
 *   acquire() and release() can be called from any thread concurrently.
 *   The pool size is fixed — it never grows or shrinks dynamically.
 *   (Dynamic sizing is possible but adds significant complexity — YAGNI here.)
 */
public class ConnectionPool implements Closeable {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /** Default number of connections in the pool. */
    public static final int DEFAULT_POOL_SIZE = 8;

    /**
     * Default acquire timeout in milliseconds.
     * If no connection is available within 5 seconds, acquire() throws.
     * 5 seconds is generous — if all connections are held for 5 seconds,
     * something is wrong (leaked connection, slow server).
     */
    public static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 5_000;

    /**
     * How many times to retry creating a replacement connection on health failure.
     * Three attempts with 100ms between them handles transient network hiccups
     * without giving up too quickly.
     */
    private static final int REPLACEMENT_RETRY_COUNT = 3;
    private static final long REPLACEMENT_RETRY_DELAY_MS = 100;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Remote server hostname. */
    private final String host;

    /** Remote server port. */
    private final int port;

    /** Maximum number of connections in this pool. */
    private final int poolSize;

    /**
     * Maximum time in milliseconds to wait for an available connection.
     * If no connection is available within this window, acquire() throws
     * PoolExhaustedException.
     */
    private final long acquireTimeoutMs;

    /**
     * The connection queue — the heart of the pool.
     *
     * Available connections sit here. acquire() removes one (blocks if empty).
     * release() adds one back (never blocks — pool can never exceed poolSize).
     *
     * ArrayBlockingQueue is bounded, so offer() on a full queue returns false
     * immediately rather than blocking. We use this to detect pool logic bugs
     * (if a caller releases more connections than they acquired, we detect it).
     */
    private final BlockingQueue<CacheClient> availableConnections;

    /**
     * Total number of connections this pool has ever created (including replacements).
     * Useful for monitoring — if this grows continuously, connections are leaking.
     */
    private final AtomicInteger totalCreated = new AtomicInteger(0);

    /**
     * Number of connections discarded due to health check failures.
     * High numbers indicate network instability or server restarts.
     */
    private final AtomicInteger totalDiscarded = new AtomicInteger(0);

    /**
     * Number of currently active (acquired, not yet released) connections.
     * Should never exceed poolSize. If it does, there's a release() leak.
     */
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    /**
     * Whether this pool has been closed.
     * After close(), acquire() throws IllegalStateException.
     */
    private volatile boolean closed = false;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Creates a pool with default size (8 connections) and default acquire timeout.
     *
     * @param host Server hostname.
     * @param port Server port.
     * @throws IOException if initial connections cannot be established.
     */
    public ConnectionPool(String host, int port) throws IOException {
        this(host, port, DEFAULT_POOL_SIZE, DEFAULT_ACQUIRE_TIMEOUT_MS);
    }

    /**
     * Creates a pool with a custom size and acquire timeout.
     *
     * CHOOSING POOL SIZE:
     *   Too small: callers block frequently waiting for connections.
     *   Too large: server has too many concurrent connections; memory overhead.
     *   Rule of thumb: pool_size = expected_concurrent_threads × 1.2
     *   For an 8-worker-thread web server: pool_size = 8–12.
     *
     * @param host             Server hostname.
     * @param port             Server port.
     * @param poolSize         Number of connections to pre-create and maintain.
     * @param acquireTimeoutMs Max milliseconds to wait for an available connection.
     * @throws IOException if initial connections cannot be established.
     */
    public ConnectionPool(String host, int port, int poolSize, long acquireTimeoutMs)
            throws IOException {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (poolSize < 1) {
            throw new IllegalArgumentException("Pool size must be >= 1");
        }
        if (acquireTimeoutMs < 0) {
            throw new IllegalArgumentException("Acquire timeout cannot be negative");
        }

        this.host              = host;
        this.port              = port;
        this.poolSize          = poolSize;
        this.acquireTimeoutMs  = acquireTimeoutMs;
        this.availableConnections = new ArrayBlockingQueue<>(poolSize);

        initializePool();
    }

    // -------------------------------------------------------------------------
    // Core pool operations
    // -------------------------------------------------------------------------

    /**
     * Acquires a connection from the pool, blocking until one is available.
     *
     * BLOCKING BEHAVIOR:
     *   If all connections are currently in use, this method blocks until:
     *     a) A connection is released by another thread, OR
     *     b) The acquireTimeoutMs elapses → throws PoolExhaustedException
     *
     * ALWAYS pair with release() in a finally block:
     *   CacheClient client = pool.acquire();
     *   try {
     *       // use client
     *   } finally {
     *       pool.release(client);  // ← never skip this
     *   }
     *
     * If you forget release(), the connection is leaked and the pool starves.
     * With poolSize=8, just 8 leaked connections deadlock all future callers.
     *
     * @return A healthy CacheClient ready to use.
     * @throws PoolExhaustedException if no connection becomes available within timeout.
     * @throws IllegalStateException if the pool has been closed.
     * @throws InterruptedException if the thread is interrupted while waiting.
     */
    public CacheClient acquire() throws IOException, InterruptedException {
        if (closed) {
            throw new IllegalStateException("ConnectionPool is closed");
        }

        // poll() with timeout: blocks up to acquireTimeoutMs, returns null on timeout.
        // This is the key difference from take() (which blocks forever).
        CacheClient client = availableConnections.poll(acquireTimeoutMs, TimeUnit.MILLISECONDS);

        if (client == null) {
            // Timeout elapsed — no connection became available.
            throw new PoolExhaustedException(
                    "No connection available after " + acquireTimeoutMs + "ms. " +
                            "Pool size: " + poolSize + ", active: " + activeConnections.get()
            );
        }

        activeConnections.incrementAndGet();
        return client;
    }

    /**
     * Returns a connection to the pool after use.
     *
     * HEALTH CHECK ON RETURN:
     *   We PING the connection before returning it.
     *   If the ping fails (server closed connection, network error), we:
     *     1. Discard the unhealthy connection (close it).
     *     2. Create a fresh replacement connection.
     *     3. Add the replacement to the pool instead.
     *   This guarantees the pool always contains live connections.
     *
     * IMPORTANT: Call this in a finally block. Never skip release().
     *
     * @param client The connection to return. If null, this method is a no-op.
     */
    public void release(CacheClient client) {
        if (client == null) {
            return;
        }

        activeConnections.decrementAndGet();

        if (closed) {
            // Pool was closed while this connection was in use — just close it.
            client.close();
            return;
        }

        // Validate the connection before returning it to the pool.
        if (isHealthy(client)) {
            boolean offered = availableConnections.offer(client);
            if (!offered) {
                // Queue is full — this shouldn't happen if acquire/release are balanced.
                // Likely indicates a bug (more releases than acquires). Close and discard.
                System.err.println("[ConnectionPool] WARNING: Pool queue full on release. " +
                        "Possible connection leak. Discarding connection.");
                client.close();
                totalDiscarded.incrementAndGet();
            }
        } else {
            // Connection is unhealthy — discard and replace.
            client.close();
            totalDiscarded.incrementAndGet();
            replaceConnection();
        }
    }

    // -------------------------------------------------------------------------
    // Pool management
    // -------------------------------------------------------------------------

    /**
     * Closes all connections in the pool and marks the pool as closed.
     * After this, acquire() will throw IllegalStateException.
     *
     * Call this in a server shutdown hook or @AfterAll in tests.
     *
     * NOTE: Connections currently acquired (not yet released) are NOT closed here.
     * When those callers call release(), release() detects the pool is closed
     * and closes the connection directly instead of returning it to the queue.
     */
    @Override
    public void close() {
        closed = true;

        // Drain all available connections and close them.
        List<CacheClient> remaining = new ArrayList<>();
        availableConnections.drainTo(remaining);
        for (CacheClient client : remaining) {
            client.close();
        }

        System.out.printf("[ConnectionPool] Closed. Created: %d, Discarded: %d%n",
                totalCreated.get(), totalDiscarded.get());
    }

    // -------------------------------------------------------------------------
    // Monitoring
    // -------------------------------------------------------------------------

    /**
     * Returns the number of connections currently available (not in use).
     * For monitoring and debugging.
     *
     * @return Number of idle connections in the pool.
     */
    public int availableCount() {
        return availableConnections.size();
    }

    /**
     * Returns the number of connections currently acquired (in use by callers).
     *
     * @return Number of active (borrowed) connections.
     */
    public int activeCount() {
        return activeConnections.get();
    }

    /**
     * Returns the total number of connections ever created by this pool,
     * including initial connections and replacements for failed ones.
     *
     * @return Total created connection count.
     */
    public int totalCreatedCount() {
        return totalCreated.get();
    }

    /**
     * Returns the total number of connections discarded due to health failures.
     * High numbers indicate server instability or network issues.
     *
     * @return Total discarded connection count.
     */
    public int totalDiscardedCount() {
        return totalDiscarded.get();
    }

    /**
     * Returns the configured maximum pool size.
     *
     * @return Pool size (maximum concurrent connections).
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Returns whether this pool has been closed.
     *
     * @return true if closed.
     */
    public boolean isClosed() {
        return closed;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Creates all initial connections and fills the pool queue.
     * Called once from the constructor.
     *
     * If ANY connection fails to create (e.g., server not running),
     * we throw immediately. A partially-initialized pool would be confusing
     * to debug — better to fail fast with a clear error.
     *
     * @throws IOException if any connection fails.
     */
    private void initializePool() throws IOException {
        for (int i = 0; i < poolSize; i++) {
            try {
                CacheClient client = new CacheClient(host, port);
                availableConnections.offer(client);
                totalCreated.incrementAndGet();
            } catch (IOException e) {
                // Close any connections we already created before throwing.
                close();
                throw new IOException(
                        "Failed to initialize connection pool (created " + i + "/" + poolSize +
                                " connections): " + e.getMessage(), e
                );
            }
        }
        System.out.printf("[ConnectionPool] Initialized with %d connections to %s:%d%n",
                poolSize, host, port);
    }

    /**
     * Checks if a connection is healthy by sending a PING.
     * Returns false if the ping fails for any reason.
     *
     * WHY PING instead of just checking socket.isConnected()?
     * socket.isConnected() returns true even for dead connections — the JVM doesn't
     * detect a closed remote socket until you actually try to read/write.
     * PING forces an actual read/write cycle, revealing dead connections.
     *
     * @param client The connection to check.
     * @return true if the connection responded to PING.
     */
    private boolean isHealthy(CacheClient client) {
        if (!client.isConnected()) {
            return false;
        }
        return client.ping();
    }

    /**
     * Creates a replacement connection and adds it to the pool.
     * Called when a returned connection fails its health check.
     *
     * Retries REPLACEMENT_RETRY_COUNT times with REPLACEMENT_RETRY_DELAY_MS between attempts.
     * If all retries fail (server is down), logs the failure and leaves the pool
     * one connection short. The next successful release will still return its connection,
     * so the pool self-heals when the server comes back.
     */
    private void replaceConnection() {
        for (int attempt = 1; attempt <= REPLACEMENT_RETRY_COUNT; attempt++) {
            try {
                CacheClient fresh = new CacheClient(host, port);
                availableConnections.offer(fresh);
                totalCreated.incrementAndGet();
                System.out.printf("[ConnectionPool] Replaced unhealthy connection (attempt %d)%n",
                        attempt);
                return;
            } catch (IOException e) {
                System.err.printf("[ConnectionPool] Failed to create replacement connection " +
                        "(attempt %d/%d): %s%n", attempt, REPLACEMENT_RETRY_COUNT, e.getMessage());

                if (attempt < REPLACEMENT_RETRY_COUNT) {
                    try {
                        Thread.sleep(REPLACEMENT_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        // All retries failed — pool is now one connection short.
        // Log prominently so operators notice.
        System.err.printf(
                "[ConnectionPool] WARNING: Could not create replacement after %d attempts. " +
                        "Pool now has %d/%d available connections.%n",
                REPLACEMENT_RETRY_COUNT, availableConnections.size(), poolSize
        );
    }

    // -------------------------------------------------------------------------
    // Inner class: PoolExhaustedException
    // -------------------------------------------------------------------------

    /**
     * Thrown when no connection becomes available within the acquire timeout.
     *
     * This is a distinct exception from IOException because the cause is different:
     *   IOException       = network failure
     *   PoolExhausted     = all connections in use, caller must retry or fail fast
     *
     * Callers can catch this specifically to implement retry logic, circuit breaking,
     * or return a "service unavailable" response to their clients.
     */
    public static class PoolExhaustedException extends IOException {
        public PoolExhaustedException(String message) {
            super(message);
        }
    }
}