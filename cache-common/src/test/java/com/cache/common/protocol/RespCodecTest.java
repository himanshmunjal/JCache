package com.cache.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RESP codec")
class RespCodecTest {

    private static ByteBuf bytes(String s) {
        return Unpooled.copiedBuffer(s, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Decodes a complete array of bulk strings")
    void decodesFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        ch.writeInbound(bytes("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello world\r\n"));

        assertEquals(List.of("SET", "key", "hello world"), ch.readInbound());
        assertNull(ch.readInbound());
    }

    @Test
    @DisplayName("A frame split across reads is decoded once complete")
    void decodesSplitFrame() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        String frame = "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
        for (String part : new String[]{frame.substring(0, 7), frame.substring(7, 20), frame.substring(20)}) {
            ch.writeInbound(bytes(part));
        }

        assertEquals(List.of("GET", "mykey"), ch.readInbound());
        assertNull(ch.readInbound());
    }

    @Test
    @DisplayName("Two frames in one read produce two messages")
    void decodesPipelinedFrames() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        ch.writeInbound(bytes("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"));

        assertEquals(List.of("PING"), ch.readInbound());
        assertEquals(List.of("PING"), ch.readInbound());
    }

    @Test
    @DisplayName("Input that is not a RESP array is rejected")
    void rejectsInlineCommand() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder());
        DecoderException e = assertThrows(DecoderException.class, () -> ch.writeInbound(bytes("PING\r\n")));
        assertInstanceOf(RespProtocolException.class, e.getCause());
    }

    @Test
    @DisplayName("Encodes every reply type")
    void encodes() {
        EmbeddedChannel ch = new EmbeddedChannel(new RespEncoder());
        ch.writeOutbound(RespValue.simpleString("OK"), RespValue.error("ERR nope"), RespValue.integer(42),
                RespValue.bulkString("héllo"), RespValue.nullBulk());

        StringBuilder out = new StringBuilder();
        ByteBuf buf;
        while ((buf = ch.readOutbound()) != null) {
            out.append(buf.toString(StandardCharsets.UTF_8));
            buf.release();
        }
        assertEquals("+OK\r\n-ERR nope\r\n:42\r\n$6\r\nhéllo\r\n$-1\r\n", out.toString());
    }
}
