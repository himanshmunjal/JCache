package com.cache.client;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Fixed-size pool of {@link CacheClient} connections to one server.
 *
 * <pre>{@code
 * CacheClient conn = pool.acquire();
 * try {
 *     conn.put("k", "v");
 * } finally {
 *     pool.release(conn);
 * }
 * }</pre>
 *
 * <p>A connection that has been idle for a while is checked with a PING
 * before it is handed out, and a connection that broke is reopened the next
 * time its slot is acquired.
 */
public class ConnectionPool implements AutoCloseable {

    private static final Logger log = Logger.getLogger(ConnectionPool.class.getName());

    private static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 5000;
    private static final long VALIDATE_AFTER_IDLE_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final String host;
    private final int port;
    private final int poolSize;
    private final BlockingQueue<CacheClient> idle;
    private final AtomicInteger active = new AtomicInteger();
    private volatile boolean closed;

    /**
     * Opens {@code poolSize} connections.
     *
     * @param host     server host
     * @param port     server port
     * @param poolSize number of connections
     * @throws IllegalStateException if a connection cannot be opened
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
        this.host = host;
        this.port = port;
        this.poolSize = poolSize;
        this.idle = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            CacheClient conn = open();
            if (conn == null) {
                close();
                throw new IllegalStateException(String.format(
                        "Could not open connection %d/%d to %s:%d", i + 1, poolSize, host, port));
            }
            idle.add(conn);
        }
    }

    /**
     * Takes a connection, waiting up to five seconds for one to be free.
     *
     * @return a connection; give it back with {@link #release}
     * @throws TimeoutException     if none became free in time
     * @throws InterruptedException if interrupted while waiting
     */
    public CacheClient acquire() throws TimeoutException, InterruptedException {
        return acquireWithTimeout(DEFAULT_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Takes a connection, waiting up to the given time for one to be free.
     *
     * @param timeout how long to wait
     * @param unit    unit of {@code timeout}
     * @return a connection; give it back with {@link #release}
     * @throws TimeoutException     if none became free in time
     * @throws InterruptedException if interrupted while waiting
     */
    public CacheClient acquireWithTimeout(long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException {
        if (closed) {
            throw new IllegalStateException("ConnectionPool is closed");
        }
        CacheClient conn = idle.poll(timeout, unit);
        if (conn == null) {
            throw new TimeoutException(String.format("No connection to %s:%d free within %d %s (pool size %d)",
                    host, port, timeout, unit, poolSize));
        }
        if (!isUsable(conn)) {
            try {
                conn.close();
                conn.connect();
            } catch (IOException e) {
                // Keep the slot; the next acquire tries to reconnect again.
                idle.offer(conn);
                throw new IllegalStateException("Cannot reconnect to " + host + ":" + port, e);
            }
        }
        active.incrementAndGet();
        return conn;
    }

    /**
     * Returns a connection to the pool. Broken connections are replaced.
     *
     * @param conn a connection obtained from this pool
     */
    public void release(CacheClient conn) {
        if (conn == null) {
            return;
        }
        active.decrementAndGet();
        if (closed) {
            conn.close();
            return;
        }
        // A broken connection goes back as-is and is reconnected by the next acquire.
        if (!idle.offer(conn)) {
            conn.close();
        }
    }

    /** @return connections currently handed out */
    public int getActiveCount() {
        return active.get();
    }

    /** @return connections waiting in the pool */
    public int getIdleCount() {
        return idle.size();
    }

    /** @return the configured pool size */
    public int getPoolSize() {
        return poolSize;
    }

    /** Closes the idle connections; connections still in use are closed when released. */
    @Override
    public void close() {
        closed = true;
        CacheClient conn;
        while ((conn = idle.poll()) != null) {
            conn.close();
        }
    }

    @Override
    public String toString() {
        return String.format("ConnectionPool{%s:%d, size=%d, active=%d, idle=%d, closed=%b}",
                host, port, poolSize, active.get(), idle.size(), closed);
    }

    private boolean isUsable(CacheClient conn) {
        if (!conn.isConnected()) {
            return false;
        }
        return conn.idleNanos() < VALIDATE_AFTER_IDLE_NANOS || conn.ping();
    }

    private CacheClient open() {
        try {
            return new CacheClient(host, port);
        } catch (Exception e) {
            log.warning("Cannot connect to " + host + ":" + port + ": " + e.getMessage());
            return null;
        }
    }
}
