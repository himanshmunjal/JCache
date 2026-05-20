package com.cache.server.handler;

import com.cache.api.Cache;
import com.cache.server.ServerConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Stub handler — accepts connections, echoes PONG to everything.
 * Full implementation comes on Day 12.
 */
public class CacheServerHandler extends SimpleChannelInboundHandler<String> {

    private final Cache<String, String> cache;
    private final ServerConfig config;

    public CacheServerHandler(Cache<String, String> cache, ServerConfig config) {
        this.cache  = cache;
        this.config = config;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        // Stub: echo back so we know the connection works
        String command = msg.trim().toUpperCase();
        if (command.equals("PING")) {
            ctx.writeAndFlush("+PONG\n");
        } else {
            ctx.writeAndFlush("+STUB: received [" + msg.trim() + "]\n");
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("[Server] Client connected: " + ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        System.out.println("[Server] Client disconnected: " + ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("[Server] Error: " + cause.getMessage());
        ctx.close();
    }
}