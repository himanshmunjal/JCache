package com.cache.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/** Netty encoder that writes {@link RespValue} replies in RESP format. */
public class RespEncoder extends MessageToByteEncoder<RespValue> {

    private static final byte[] CRLF = {'\r', '\n'};

    @Override
    protected void encode(ChannelHandlerContext ctx, RespValue msg, ByteBuf out) {
        switch (msg.getType()) {
            case SIMPLE_STRING -> writeLine(out, '+', msg.getStringValue());
            case ERROR -> writeLine(out, '-', msg.getStringValue());
            case INTEGER -> writeLine(out, ':', Long.toString(msg.getLongValue()));
            case NULL_BULK -> writeLine(out, '$', "-1");
            case BULK_STRING -> {
                byte[] bytes = msg.getStringValue().getBytes(StandardCharsets.UTF_8);
                writeLine(out, '$', Integer.toString(bytes.length));
                out.writeBytes(bytes);
                out.writeBytes(CRLF);
            }
        }
    }

    private static void writeLine(ByteBuf out, char prefix, String text) {
        out.writeByte(prefix);
        out.writeCharSequence(text, StandardCharsets.UTF_8);
        out.writeBytes(CRLF);
    }
}
