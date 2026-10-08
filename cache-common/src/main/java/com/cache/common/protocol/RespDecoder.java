package com.cache.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Netty decoder for RESP (the Redis serialization protocol) requests.
 *
 * <p>Clients send each command as an array of bulk strings, for example
 * {@code *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n}. Each complete array is emitted
 * downstream as a {@code List<String>}. If a frame is split across several
 * TCP reads, nothing is consumed until the whole frame has arrived.
 */
public class RespDecoder extends ByteToMessageDecoder {

    private static final long MAX_BULK_LENGTH = 64L * 1024 * 1024;
    private static final int MAX_ARRAY_ELEMENTS = 1024;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int frameStart = in.readerIndex();
        List<String> args = readFrame(in);
        if (args == null) {
            in.readerIndex(frameStart);
        } else {
            out.add(args);
        }
    }

    /** Returns the decoded frame, or {@code null} if more bytes are needed. */
    private static List<String> readFrame(ByteBuf in) {
        String header = readLine(in);
        if (header == null) {
            return null;
        }
        int count = parseLength(header, '*', MAX_ARRAY_ELEMENTS);
        List<String> args = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String arg = readBulkString(in);
            if (arg == null) {
                return null;
            }
            args.add(arg);
        }
        return args;
    }

    private static String readBulkString(ByteBuf in) {
        String header = readLine(in);
        if (header == null) {
            return null;
        }
        int length = parseLength(header, '$', MAX_BULK_LENGTH);
        if (in.readableBytes() < length + 2) {
            return null;
        }
        String value = in.readCharSequence(length, StandardCharsets.UTF_8).toString();
        if (in.readByte() != '\r' || in.readByte() != '\n') {
            throw new RespProtocolException("Bulk string of length " + length + " is not followed by CRLF");
        }
        return value;
    }

    private static int parseLength(String line, char prefix, long max) {
        if (line.isEmpty() || line.charAt(0) != prefix) {
            throw new RespProtocolException("Expected '" + prefix + "', got: '" + abbreviate(line) + "'");
        }
        long length;
        try {
            length = Long.parseLong(line.substring(1));
        } catch (NumberFormatException e) {
            throw new RespProtocolException("Invalid length: '" + abbreviate(line) + "'");
        }
        if (length < 0 || length > max) {
            throw new RespProtocolException("Length " + length + " is outside 0.." + max);
        }
        return (int) length;
    }

    /** Reads up to the next CRLF, or returns {@code null} if no full line is buffered. */
    private static String readLine(ByteBuf in) {
        int cr = in.indexOf(in.readerIndex(), in.writerIndex(), (byte) '\r');
        if (cr < 0 || cr + 1 >= in.writerIndex()) {
            return null;
        }
        if (in.getByte(cr + 1) != '\n') {
            throw new RespProtocolException("Expected LF after CR");
        }
        String line = in.readCharSequence(cr - in.readerIndex(), StandardCharsets.UTF_8).toString();
        in.skipBytes(2);
        return line;
    }

    private static String abbreviate(String s) {
        return s.length() <= 32 ? s : s.substring(0, 32) + "...";
    }
}
