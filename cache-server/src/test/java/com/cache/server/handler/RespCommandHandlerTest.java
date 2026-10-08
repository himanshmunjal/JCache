package com.cache.server.handler;

import com.cache.common.protocol.RespDecoder;
import com.cache.common.protocol.RespEncoder;
import com.cache.concurrent.SegmentedCache;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import com.cache.ttl.TTLCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RespCommandHandlerTest {

    private TTLCache<String, String> cache;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        cache = new TTLCache<>(new SegmentedCache<>(100));
        channel = new EmbeddedChannel(new RespDecoder(), new RespEncoder(),
                new RespCommandHandler(cache, new ServerMetrics(), ServerConfig.defaults(), null));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
        cache.shutdown();
    }

    /** Sends one command as a RESP array of bulk strings and returns the raw reply. */
    private String send(String... args) {
        StringBuilder sb = new StringBuilder("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append('$').append(arg.getBytes(StandardCharsets.UTF_8).length).append("\r\n")
                    .append(arg).append("\r\n");
        }
        channel.writeInbound(Unpooled.copiedBuffer(sb, CharsetUtil.UTF_8));
        ByteBuf out = channel.readOutbound();
        try {
            return out.toString(CharsetUtil.UTF_8);
        } finally {
            out.release();
        }
    }

    @Test
    void ping() {
        assertEquals("+PONG\r\n", send("PING"));
        assertEquals("$5\r\nhello\r\n", send("ping", "hello"));
    }

    @Test
    void setAndGetUseBulkStrings() {
        assertEquals("+OK\r\n", send("SET", "greeting", "hello world"));
        assertEquals("$11\r\nhello world\r\n", send("GET", "greeting"));
        assertEquals("$-1\r\n", send("GET", "missing"));
    }

    @Test
    void keysAndValuesMayContainSpacesAndLineBreaks() {
        assertEquals("+OK\r\n", send("SET", "my key", "line1\r\nline2"));
        assertEquals("$12\r\nline1\r\nline2\r\n", send("GET", "my key"));
    }

    @Test
    void utf8LengthsAreInBytes() {
        send("SET", "k", "héllo");
        assertEquals("$6\r\nhéllo\r\n", send("GET", "k"));
    }

    @Test
    void setWithExpiry() {
        assertEquals("+OK\r\n", send("SET", "a", "1", "EX", "100"));
        long ttl = cache.getRemainingTTL("a");
        assertTrue(ttl > 98 && ttl <= 100, "ttl " + ttl);

        assertEquals("+OK\r\n", send("SET", "b", "1", "px", "1500"));
        assertEquals(":2\r\n", send("TTL", "b"));

        assertEquals("+OK\r\n", send("SETEX", "c", "50", "v"));
        assertEquals(":50\r\n", send("TTL", "c"));
    }

    @Test
    void setRejectsUnsupportedOptions() {
        assertEquals("-ERR SET option 'NX' is not supported\r\n", send("SET", "a", "1", "NX"));
        assertEquals("-ERR syntax error\r\n", send("SET", "a", "1", "EX"));
        assertEquals("-ERR invalid expire time in 'set' command\r\n", send("SET", "a", "1", "EX", "0"));
        assertEquals("-ERR value is not an integer or out of range\r\n", send("SET", "a", "1", "EX", "x"));
        assertNull(cache.peek("a"));
    }

    @Test
    void delAndExistsCountKeys() {
        send("SET", "a", "1");
        send("SET", "b", "2");
        assertEquals(":2\r\n", send("EXISTS", "a", "b", "c"));
        assertEquals(":2\r\n", send("DEL", "a", "b", "c"));
        assertEquals(":0\r\n", send("DEL", "a"));
        assertEquals(":0\r\n", send("EXISTS", "a"));
    }

    @Test
    void ttlCommandsFollowRedis() {
        assertEquals(":-2\r\n", send("TTL", "nope"));
        send("SET", "k", "v");
        assertEquals(":-1\r\n", send("TTL", "k"));
        assertEquals(":0\r\n", send("PERSIST", "k"));

        assertEquals(":1\r\n", send("EXPIRE", "k", "100"));
        assertEquals(":100\r\n", send("TTL", "k"));
        assertEquals(":1\r\n", send("PERSIST", "k"));
        assertEquals(":-1\r\n", send("TTL", "k"));

        assertEquals(":0\r\n", send("EXPIRE", "nope", "10"));
        assertEquals(":1\r\n", send("EXPIRE", "k", "-1"));
        assertNull(cache.peek("k"));
    }

    @Test
    void dbsizeAndFlush() {
        send("SET", "a", "1");
        send("SET", "b", "2");
        assertEquals(":2\r\n", send("DBSIZE"));
        assertEquals("+OK\r\n", send("FLUSHALL"));
        assertEquals(":0\r\n", send("DBSIZE"));
        send("SET", "a", "1");
        assertEquals("+OK\r\n", send("FLUSHDB", "ASYNC"));
        assertEquals(":0\r\n", send("DBSIZE"));
    }

    @Test
    void infoListsStatsOnePerLine() {
        send("GET", "missing");
        String info = send("INFO");
        assertTrue(info.startsWith("$"), info);
        assertTrue(info.contains("# Stats\r\nhits:0\r\nmisses:1\r\n"), info);
    }

    @Test
    void selectOnlyAcceptsDatabaseZero() {
        assertEquals("+OK\r\n", send("SELECT", "0"));
        assertEquals("-ERR DB index is out of range\r\n", send("SELECT", "1"));
    }

    @Test
    void textProtocolVerbsWork() {
        assertEquals("+OK\r\n", send("PUT", "k", "v"));
        assertEquals(":1\r\n", send("DELETE", "k"));
        assertTrue(send("STATS").contains("puts:1"));
    }

    @Test
    void errorsUseRedisWording() {
        assertEquals("-ERR unknown command 'HELLO'\r\n", send("HELLO", "3"));
        assertEquals("-ERR wrong number of arguments for 'get' command\r\n", send("GET"));
        assertEquals("-ERR wrong number of arguments for 'get' command\r\n", send("GET", "a", "b"));
        assertTrue(channel.isActive(), "a rejected command keeps the connection open");
    }

    @Test
    void quitRepliesAndCloses() {
        assertEquals("+OK\r\n", send("QUIT"));
        assertFalse(channel.isActive());
    }

    @Test
    void malformedInputClosesTheConnection() {
        channel.writeInbound(Unpooled.copiedBuffer("*1\r\n#bad\r\n", CharsetUtil.UTF_8));
        ByteBuf out = channel.readOutbound();
        try {
            assertTrue(out.toString(CharsetUtil.UTF_8).startsWith("-ERR Protocol error: "));
        } finally {
            out.release();
        }
        assertFalse(channel.isActive());
    }

    @Test
    void pipelinedCommandsGetRepliesInOrder() {
        channel.writeInbound(Unpooled.copiedBuffer(
                "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n", CharsetUtil.UTF_8));
        StringBuilder replies = new StringBuilder();
        for (ByteBuf out; (out = channel.readOutbound()) != null; ) {
            replies.append(out.toString(CharsetUtil.UTF_8));
            out.release();
        }
        assertEquals("+OK\r\n$1\r\n1\r\n", replies.toString());
    }

    @Test
    void rateLimitApplies() {
        ServerConfig limited = ServerConfig.builder().rateLimitPerSecond(1).rateLimitBurst(1).build();
        EmbeddedChannel ch = new EmbeddedChannel(new RespDecoder(), new RespEncoder(),
                new RespCommandHandler(cache, new ServerMetrics(), limited, null, () -> 0L));
        try {
            ch.writeInbound(Unpooled.copiedBuffer(
                    "*2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$4\r\nPING\r\n",
                    CharsetUtil.UTF_8));
            StringBuilder replies = new StringBuilder();
            for (ByteBuf out; (out = ch.readOutbound()) != null; ) {
                replies.append(out.toString(CharsetUtil.UTF_8));
                out.release();
            }
            assertEquals("$-1\r\n-ERR rate limit exceeded\r\n+PONG\r\n", replies.toString());
        } finally {
            ch.finishAndReleaseAll();
        }
    }
}
