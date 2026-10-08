package com.cache.server.handler;

import com.cache.common.protocol.ResponseEncoder;
import com.cache.server.metrics.ServerMetrics;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tracks open connections and enforces the connection limit. A single
 * instance is shared by every channel pipeline.
 *
 * <p>A client that connects while the server is full receives
 * {@code -ERR max connections reached} and is disconnected.
 */
@ChannelHandler.Sharable
public class ConnectionManager extends ChannelInboundHandlerAdapter {

    private static final Logger log = Logger.getLogger(ConnectionManager.class.getName());

    private final int maxConnections;
    private final ServerMetrics metrics;
    private final Set<Channel> channels = ConcurrentHashMap.newKeySet();
    private final AtomicLong rejected = new AtomicLong();

    /**
     * Creates the manager.
     *
     * @param maxConnections connection limit, at least 1
     * @param metrics        where connection events are recorded
     */
    public ConnectionManager(int maxConnections, ServerMetrics metrics) {
        if (maxConnections < 1) {
            throw new IllegalArgumentException("maxConnections must be >= 1");
        }
        if (metrics == null) {
            throw new IllegalArgumentException("ServerMetrics cannot be null");
        }
        this.maxConnections = maxConnections;
        this.metrics = metrics;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Add first, then check, so two connections arriving together cannot both slip under the limit.
        channels.add(ctx.channel());
        if (channels.size() > maxConnections) {
            channels.remove(ctx.channel());
            rejected.incrementAndGet();
            log.warning("Rejected connection from " + ctx.channel().remoteAddress()
                    + ": limit of " + maxConnections + " reached");
            ctx.writeAndFlush(ResponseEncoder.error("max connections reached"))
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }
        metrics.connectionOpened();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (channels.remove(ctx.channel())) {
            metrics.connectionClosed();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            // Typically "connection reset by peer"; nothing useful to report.
            log.log(Level.FINE, "I/O error on " + ctx.channel().remoteAddress(), cause);
            ctx.close();
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    /** @return number of open connections */
    public int getActiveConnectionCount() {
        return channels.size();
    }

    /** @return number of connections refused because of the limit */
    public long getRejectedConnectionCount() {
        return rejected.get();
    }

    /** @return the connection limit */
    public int getMaxConnections() {
        return maxConnections;
    }

    /** Closes every open connection. Used during shutdown. */
    public void closeAllConnections() {
        channels.forEach(Channel::close);
    }
}
