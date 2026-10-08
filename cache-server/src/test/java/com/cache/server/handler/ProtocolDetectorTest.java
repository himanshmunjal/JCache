package com.cache.server.handler;

import com.cache.concurrent.SegmentedCache;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import com.cache.ttl.TTLCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProtocolDetectorTest {

    private TTLCache<String, String> cache;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        cache = new TTLCache<>(new SegmentedCache<>(100));
        ServerMetrics metrics = new ServerMetrics();
        ServerConfig config = ServerConfig.defaults();
        channel = new EmbeddedChannel(
                new StringEncoder(CharsetUtil.UTF_8),
                new ProtocolDetector(1024,
                        () -> new CacheServerHandler(cache, metrics, config),
                        () -> new RespCommandHandler(cache, metrics, config, null)));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
        cache.shutdown();
    }

    private String exchange(String request) {
        channel.writeInbound(Unpooled.copiedBuffer(request, CharsetUtil.UTF_8));
        return readAll();
    }

    private String readAll() {
        StringBuilder sb = new StringBuilder();
        for (Object out; (out = channel.readOutbound()) != null; ) {
            ByteBuf buf = (ByteBuf) out;
            sb.append(buf.toString(CharsetUtil.UTF_8));
            buf.release();
        }
        return sb.toString();
    }

    @Test
    void textConnection() {
        assertEquals("+OK\r\n", exchange("PUT k hello world\r\n"));
        assertEquals("+hello world\r\n", exchange("GET k\n"));
        assertEquals("-ERR key not found\r\n", exchange("GET missing\r\n"));
        assertNotNull(channel.pipeline().get(CacheServerHandler.class));
        assertNull(channel.pipeline().get(ProtocolDetector.class));
    }

    @Test
    void respConnection() {
        assertEquals("+OK\r\n", exchange("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nhi\r\n"));
        assertEquals("$2\r\nhi\r\n", exchange("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"));
        assertEquals("$-1\r\n", exchange("*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n"));
        assertNotNull(channel.pipeline().get(RespCommandHandler.class));
        assertNull(channel.pipeline().get(ProtocolDetector.class));
    }

    @Test
    void firstRequestSplitAcrossReads() {
        assertEquals("", exchange("*1\r\n$4"));
        assertEquals("+PONG\r\n", exchange("\r\nPING\r\n"));
    }

    @Test
    void textLineSplitAcrossReads() {
        assertEquals("", exchange("PI"));
        assertEquals("+PONG\r\n", exchange("NG\r\n"));
    }

    @Test
    void bothProtocolsShareTheCache() {
        exchange("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nhi\r\n");
        assertEquals("hi", cache.get("k"));
    }
}
