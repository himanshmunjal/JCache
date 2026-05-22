package com.cache.server.handler;

import com.cache.server.metrics.ServerMetrics;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ConnectionManager tracks all active client connections and enforces
 * the maximum connection limit defined in ServerConfig.
 *
 * POSITION IN THE PIPELINE:
 *   ConnectionManager sits BEFORE CacheServerHandler in the pipeline.
 *   This means it gets channelActive() and channelInactive() calls first,
 *   before CacheServerHandler processes any commands.
 *
 *   Pipeline order (for each accepted channel):
 *     LineBasedFrameDecoder
 *     StringDecoder
 *     StringEncoder
 *     ConnectionManager     ← enforces limits, tracks state
 *     CacheServerHandler    ← processes commands
 *
 * WHY @ChannelHandler.Sharable?
 *   Without @Sharable, Netty throws an exception if you add the same handler
 *   instance to multiple pipelines. Since ConnectionManager holds GLOBAL state
 *   (connection count across all channels), we WANT a single shared instance.
 *   The @Sharable annotation tells Netty "I've verified this handler is thread-safe
 *   to share across channels." We back this up with AtomicLong and ConcurrentHashMap.
 *
 * THREAD SAFETY:
 *   channelActive() and channelInactive() are called by Netty worker threads —
 *   one per channel, potentially concurrently. All state mutations use:
 *     - AtomicLong for the connection counter (CAS-based, no locks)
 *     - ConcurrentHashMap for the active channel set (lock-free for reads)
 *
 * CONNECTION ENFORCEMENT:
 *   When the limit is exceeded, we close the new channel immediately in
 *   channelActive() BEFORE any data is read. The client receives a TCP RST
 *   (connection reset) — no application-level error message is sent.
 *
 *   Why no error message? The StringDecoder/encoder are in the pipeline, but
 *   we haven't received any data yet. Sending a message before the client
 *   sends anything would be protocol-incorrect. Redis does the same — it
 *   simply closes the connection when max clients is reached.
 *
 * REJECTED CONNECTIONS COUNTER:
 *   We count rejected connections separately from active ones. This number
 *   appearing in STATS is an operational signal: "your server is overloaded,
 *   increase maxConnections or add more server instances."
 */
@ChannelHandler.Sharable
public class ConnectionManager extends ChannelInboundHandlerAdapter {

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * Maximum number of concurrent connections this server will accept.
     * Set from ServerConfig at construction time. Immutable after that.
     */
    private final int maxConnections;

    /**
     * Reference to ServerMetrics for recording connection open/close events.
     * Shared with CacheServerHandler — single source of truth for metrics.
     */
    private final ServerMetrics metrics;

    /**
     * Set of all currently active channels.
     *
     * WHY TRACK CHANNELS AND NOT JUST A COUNT?
     * A count alone gives you the number but not the identities.
     * Tracking channels lets you:
     *   - Iterate and close all connections on shutdown
     *   - Inspect remote addresses for debugging
     *   - Implement per-IP rate limiting (future feature)
     *
     * ConcurrentHashMap.newKeySet() gives a thread-safe Set backed by CHM.
     * add/remove are O(1) average, iteration is safe (weakly consistent).
     */
    private final Set<io.netty.channel.Channel> activeChannels =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Number of connections rejected due to exceeding maxConnections.
     * Monotonically increasing. Exposed via getters for STATS reporting.
     */
    private final AtomicLong rejectedConnections = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a ConnectionManager.
     *
     * @param maxConnections Maximum allowed concurrent connections. Must be >= 1.
     * @param metrics        ServerMetrics instance for recording connection events.
     */
    public ConnectionManager(int maxConnections, ServerMetrics metrics) {
        if (maxConnections < 1) {
            throw new IllegalArgumentException("maxConnections must be >= 1");
        }
        if (metrics == null) {
            throw new IllegalArgumentException("ServerMetrics cannot be null");
        }
        this.maxConnections = maxConnections;
        this.metrics        = metrics;
    }

    // -------------------------------------------------------------------------
    // Netty lifecycle callbacks
    // -------------------------------------------------------------------------

    /**
     * Called by Netty when a new TCP connection is established.
     *
     * This is the enforcement point for maxConnections.
     * We check the current count BEFORE adding the new channel.
     * If adding would exceed the limit, we close immediately.
     *
     * ORDERING NOTE:
     * Netty guarantees channelActive() is called before any channelRead()
     * events for the same channel. So if we close here, channelRead0()
     * in CacheServerHandler is never called for this channel. Clean rejection.
     *
     * @param ctx The channel handler context for the new connection.
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Check limit BEFORE adding to the set.
        // Using size() for the check is safe here — even if another thread
        // adds between our check and our add, the worst case is briefly
        // exceeding by 1, which is acceptable (vs using a lock).
        if (activeChannels.size() >= maxConnections) {
            // Limit exceeded — reject this connection
            rejectedConnections.incrementAndGet();

            String remoteAddress = getRemoteAddress(ctx);
            System.err.printf("[ConnectionManager] Rejecting connection from %s " +
                            "(limit %d reached, currently %d active)%n",
                    remoteAddress, maxConnections, activeChannels.size());

            // Close the channel. This sends TCP FIN to the client.
            // ctx.close() is non-blocking — it schedules the close.
            ctx.close();
            return; // do NOT propagate channelActive to next handler
        }

        // Connection accepted — register it
        activeChannels.add(ctx.channel());
        metrics.connectionOpened();

        if (isVerboseLogging()) {
            System.out.printf("[ConnectionManager] New connection from %s " +
                            "(active: %d/%d)%n",
                    getRemoteAddress(ctx), activeChannels.size(), maxConnections);
        }

        // Propagate to the next handler in the pipeline (CacheServerHandler)
        super.channelActive(ctx);
    }

    /**
     * Called by Netty when a connection is closed, either by the client
     * (TCP FIN) or by us (ctx.close()).
     *
     * We remove the channel from tracking and update metrics.
     * This is called even if channelActive() closed the channel —
     * Netty always pairs channelActive with channelInactive.
     *
     * @param ctx The channel handler context for the closed connection.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // remove() returns true only if the channel was actually in the set.
        // For rejected connections (closed in channelActive before adding),
        // remove() returns false — we don't decrement metrics for those.
        boolean wasTracked = activeChannels.remove(ctx.channel());

        if (wasTracked) {
            metrics.connectionClosed();

            if (isVerboseLogging()) {
                System.out.printf("[ConnectionManager] Connection closed from %s " +
                                "(active: %d/%d)%n",
                        getRemoteAddress(ctx), activeChannels.size(), maxConnections);
            }
        }

        // Always propagate to allow CacheServerHandler to clean up
        super.channelInactive(ctx);
    }

    /**
     * Called when an unhandled exception occurs in the pipeline.
     *
     * Common causes:
     *   - Client disconnects abruptly (IOException: Connection reset by peer)
     *   - ReadTimeoutException if we added a read timeout handler
     *   - Any RuntimeException thrown by CacheServerHandler that wasn't caught
     *
     * We log the error and close the channel. The channel will trigger
     * channelInactive() which handles cleanup.
     *
     * @param ctx   The channel handler context.
     * @param cause The exception that occurred.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String remoteAddress = getRemoteAddress(ctx);

        // Filter out noisy "connection reset" messages — these are normal
        // when clients disconnect without sending FIN (process crash, etc.)
        String message = cause.getMessage();
        if (message != null && (
                message.contains("Connection reset") ||
                        message.contains("Broken pipe") ||
                        message.contains("connection was forcibly closed"))) {
            // Client-side disconnect — not an error on our end
            if (isVerboseLogging()) {
                System.out.printf("[ConnectionManager] Client %s disconnected abruptly%n",
                        remoteAddress);
            }
        } else {
            System.err.printf("[ConnectionManager] Exception on channel %s: %s%n",
                    remoteAddress, cause.getMessage());
        }

        // Close the channel — this triggers channelInactive() for cleanup
        ctx.close();
    }

    // -------------------------------------------------------------------------
    // Public API for CacheServer and tests
    // -------------------------------------------------------------------------

    /**
     * Returns the current number of active connections.
     * This is a point-in-time snapshot — may change immediately after return.
     *
     * @return Number of currently connected clients.
     */
    public int getActiveConnectionCount() {
        return activeChannels.size();
    }

    /**
     * Returns the total number of connections rejected due to exceeding maxConnections.
     * A persistently non-zero value here means your server is under too much load.
     *
     * @return Cumulative rejected connection count since server start.
     */
    public long getRejectedConnectionCount() {
        return rejectedConnections.get();
    }

    /**
     * Returns the configured maximum connection limit.
     *
     * @return maxConnections value from ServerConfig.
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Closes all currently active connections.
     * Called during server shutdown to ensure all clients receive clean TCP FIN.
     *
     * This method blocks until all close futures complete or are initiated.
     * Actual closure is async — channels may still be in the process of closing
     * when this method returns.
     */
    public void closeAllConnections() {
        System.out.printf("[ConnectionManager] Closing %d active connections...%n",
                activeChannels.size());

        for (io.netty.channel.Channel channel : activeChannels) {
            if (channel.isOpen()) {
                channel.close();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Extracts the remote IP address and port from a channel handler context.
     * Returns "unknown" if the remote address is not available (closed channel).
     *
     * @param ctx The channel handler context.
     * @return Human-readable remote address string, e.g. "/127.0.0.1:52341".
     */
    private String getRemoteAddress(ChannelHandlerContext ctx) {
        try {
            if (ctx.channel().remoteAddress() instanceof InetSocketAddress) {
                InetSocketAddress addr = (InetSocketAddress) ctx.channel().remoteAddress();
                return addr.getAddress().getHostAddress() + ":" + addr.getPort();
            }
        } catch (Exception e) {
            // Channel may have closed — remote address unavailable
        }
        return "unknown";
    }

    /**
     * Controls whether connection open/close events are logged to stdout.
     * In production, this would be driven by a config flag.
     * For now, always true — useful during development.
     *
     * TODO: wire to ServerConfig.isVerbose() when CacheServer passes it here.
     *
     * @return true if verbose logging is enabled.
     */
    private boolean isVerboseLogging() {
        return false; // flip to true for detailed connection logging during dev
    }
}