package com.cache.server.handler;

import com.cache.common.protocol.RespDecoder;
import com.cache.common.protocol.RespEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.function.Supplier;

/**
 * Chooses the protocol of a connection from its first byte, then replaces
 * itself with the matching decoder. RESP requests always start with
 * {@code *}; anything else is read as the text protocol, which is how Redis
 * tells its inline commands apart too.
 *
 * <p>The command handler is added at the end of the pipeline, so handlers
 * already there (such as the connection limiter) stay in front of it. Text
 * replies are Strings and need a {@code StringEncoder} in the pipeline.
 */
public class ProtocolDetector extends ByteToMessageDecoder {

    private final int maxLineLength;
    private final Supplier<ChannelHandler> textHandler;
    private final Supplier<ChannelHandler> respHandler;

    /**
     * @param maxLineLength longest accepted text-protocol line, in bytes
     * @param textHandler   creates the handler for a text-protocol connection
     * @param respHandler   creates the handler for a RESP connection
     */
    public ProtocolDetector(int maxLineLength, Supplier<ChannelHandler> textHandler,
                            Supplier<ChannelHandler> respHandler) {
        this.maxLineLength = maxLineLength;
        this.textHandler = textHandler;
        this.respHandler = respHandler;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (!in.isReadable()) {
            return;
        }
        ChannelPipeline pipeline = ctx.pipeline();
        String name = ctx.name();
        if (in.getByte(in.readerIndex()) == '*') {
            pipeline.addAfter(name, "respEncoder", new RespEncoder());
            pipeline.addAfter(name, "respDecoder", new RespDecoder());
            pipeline.addLast("respHandler", respHandler.get());
        } else {
            pipeline.addAfter(name, "stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
            pipeline.addAfter(name, "lineDecoder", new LineBasedFrameDecoder(maxLineLength));
            pipeline.addLast("textHandler", textHandler.get());
        }
        // Removing a ByteToMessageDecoder hands its buffered bytes to the next handler.
        pipeline.remove(this);
    }
}
