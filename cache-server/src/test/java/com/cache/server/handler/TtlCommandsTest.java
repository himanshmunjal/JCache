package com.cache.server.handler;

import com.cache.concurrent.SegmentedCache;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import com.cache.ttl.TTLCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CacheServerHandler with a TTL-capable cache")
class TtlCommandsTest {
    private TTLCache<String, String> cache;
    private EmbeddedChannel channel;

    private void start(ServerConfig config) {
        cache = new TTLCache<>(new SegmentedCache<>(100));
        channel = new EmbeddedChannel(
                new LineBasedFrameDecoder(64),
                new StringDecoder(CharsetUtil.UTF_8),
                new StringEncoder(CharsetUtil.UTF_8),
                new CacheServerHandler(cache, new ServerMetrics(), config));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
        cache.shutdown();
    }

    private String send(String line) {
        channel.writeInbound(Unpooled.copiedBuffer(line + "\r\n", CharsetUtil.UTF_8));
        ByteBuf out = channel.readOutbound();
        try {
            return out.toString(CharsetUtil.UTF_8).trim();
        } finally {
            out.release();
        }
    }

    @Test
    @DisplayName("PUT with a TTL, then TTL reports the remaining seconds")
    void putWithTtl() {
        start(ServerConfig.defaults());
        assertEquals("+OK", send("PUT session abc 100"));
        long remaining = Long.parseLong(send("TTL session").substring(1));
        assertTrue(remaining > 0 && remaining <= 100, "remaining: " + remaining);
    }

    @Test
    @DisplayName("TTL is -1 for a key without expiry and an error for a missing key")
    void ttlSpecialValues() {
        start(ServerConfig.defaults());
        send("PUT k v");
        assertEquals("+-1", send("TTL k"));
        assertEquals("-ERR key not found", send("TTL missing"));
    }

    @Test
    @DisplayName("PERSIST removes the TTL")
    void persist() {
        start(ServerConfig.defaults());
        send("PUT k v 100");
        assertEquals("+OK", send("PERSIST k"));
        assertEquals("+-1", send("TTL k"));
        assertEquals("-ERR key not found", send("PERSIST missing"));
    }

    @Test
    @DisplayName("EXPIRE 0 deletes the key; EXPIRE on a missing key is an error")
    void expire() {
        start(ServerConfig.defaults());
        send("PUT k v");
        assertEquals("+OK", send("EXPIRE k 0"));
        assertEquals("-ERR key not found", send("GET k"));
        assertEquals("-ERR key not found", send("EXPIRE missing 10"));
    }

    @Test
    @DisplayName("The legacy form 'TTL key seconds' still sets a TTL")
    void legacyTtlSetter() {
        start(ServerConfig.defaults());
        send("PUT k v");
        assertEquals("+OK", send("TTL k 50"));
        long remaining = Long.parseLong(send("TTL k").substring(1));
        assertTrue(remaining > 0 && remaining <= 50);
    }

    @Test
    @DisplayName("The server's default TTL applies to PUT without a TTL")
    void defaultTtl() {
        start(ServerConfig.builder().defaultTtlSeconds(30).build());
        send("PUT k v");
        long remaining = Long.parseLong(send("TTL k").substring(1));
        assertTrue(remaining > 0 && remaining <= 30);
    }

    @Test
    @DisplayName("A key that matches text inside the verb is stored under the right key")
    void keyMatchingVerb() {
        start(ServerConfig.defaults());
        assertEquals("+OK", send("PUT P hello"));
        assertEquals("+hello", send("GET P"));
    }

    @Test
    @DisplayName("Values keep their inner spacing")
    void multiWordValue() {
        start(ServerConfig.defaults());
        send("PUT greeting hello  wide   world");
        assertEquals("+hello  wide   world", send("GET greeting"));
    }

    @Test
    @DisplayName("An over-long line is rejected but the connection stays usable")
    void lineTooLong() {
        start(ServerConfig.defaults());
        assertEquals("-ERR line too long", send("PUT k " + "x".repeat(100)));
        assertEquals("+PONG", send("PING"));
    }

    @Test
    @DisplayName("QUIT replies and closes the connection")
    void quit() {
        start(ServerConfig.defaults());
        assertEquals("+BYE", send("QUIT"));
        assertFalse(channel.isOpen());
    }

    @Test
    @DisplayName("DELETE is counted in deletes, not in evictions")
    void deleteIsNotAnEviction() {
        start(ServerConfig.defaults());
        send("PUT a 1");
        send("PUT b 2");
        send("DELETE a");
        send("DELETE b");
        String stats = send("STATS");
        assertTrue(stats.contains(" evictions:0 "), stats);
        assertTrue(stats.contains(" deletes:2 "), stats);
    }
}
