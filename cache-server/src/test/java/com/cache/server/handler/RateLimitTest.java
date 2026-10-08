package com.cache.server.handler;

import com.cache.policy.LRUCache;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Rate limiting in CacheServerHandler")
class RateLimitTest {

    private static final String LIMITED = "-ERR rate limit exceeded\r\n";

    private final AtomicLong now = new AtomicLong();
    private final ServerMetrics metrics = new ServerMetrics();
    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    private void open(ServerConfig config) {
        channel = new EmbeddedChannel(
                new LineBasedFrameDecoder(8192),
                new StringDecoder(CharsetUtil.UTF_8),
                new StringEncoder(CharsetUtil.UTF_8),
                new CacheServerHandler(new LRUCache<>(100), metrics, config, null, now::get));
    }

    private void open(int perSecond, int burst) {
        open(ServerConfig.builder().rateLimitPerSecond(perSecond).rateLimitBurst(burst).build());
    }

    private String send(String command) {
        channel.writeInbound(command + "\n");
        ByteBuf buf = channel.readOutbound();
        assertNotNull(buf, "no reply to " + command);
        try {
            return buf.toString(CharsetUtil.UTF_8);
        } finally {
            buf.release();
        }
    }

    @Test
    @DisplayName("Commands over the burst are rejected and not run")
    void rejectsOverBurst() {
        open(1, 2);

        assertEquals("+OK\r\n", send("PUT a 1"));
        assertEquals("+OK\r\n", send("PUT b 2"));
        assertEquals(LIMITED, send("PUT c 3"));
        assertEquals(1, metrics.snapshot().rateLimited);
        assertEquals(2, metrics.snapshot().totalPuts, "the rejected PUT must not run");
    }

    @Test
    @DisplayName("Commands are accepted again once tokens refill")
    void acceptsAfterRefill() {
        open(10, 1);

        assertEquals("+OK\r\n", send("PUT a 1"));
        assertEquals(LIMITED, send("GET a"));

        now.addAndGet(100_000_000L);
        assertEquals("+1\r\n", send("GET a"));
        assertEquals(LIMITED, send("GET a"));
    }

    @Test
    @DisplayName("PING is never limited and does not use a token")
    void pingExempt() {
        open(1, 1);

        assertEquals("+OK\r\n", send("PUT a 1"));
        for (int i = 0; i < 5; i++) {
            assertEquals("+PONG\r\n", send("PING"));
        }
        assertEquals(LIMITED, send("GET a"));
    }

    @Test
    @DisplayName("STATS is limited")
    void statsLimited() {
        open(1, 1);

        assertTrue(send("STATS").startsWith("+"));
        assertEquals(LIMITED, send("STATS"));
    }

    @Test
    @DisplayName("QUIT closes the connection even when over the limit")
    void quitExempt() {
        open(1, 1);
        send("PUT a 1");
        assertEquals(LIMITED, send("GET a"));

        assertEquals("+BYE\r\n", send("QUIT"));
        assertFalse(channel.isOpen());
    }

    @Test
    @DisplayName("No limit is applied by default")
    void offByDefault() {
        open(ServerConfig.defaults());

        for (int i = 0; i < 1000; i++) {
            assertEquals("+OK\r\n", send("PUT k " + i));
        }
        assertEquals(0, metrics.snapshot().rateLimited);
    }
}
