package com.cache.server.handler;

import com.cache.api.Cache;
import com.cache.policy.LRUCache;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CacheServerHandler using Netty's EmbeddedChannel.
 *
 * ROOT CAUSE OF THE ClassCastException (PooledUnsafeDirectByteBuf → String):
 *
 *   StringEncoder converts String → ByteBuf for sending over a real TCP socket.
 *   In EmbeddedChannel, there is no real socket. The ByteBuf produced by
 *   StringEncoder lands directly in the outbound queue. When you call
 *   channel.readOutbound(), you get that raw ByteBuf — NOT a String.
 *
 *   Casting it to String with (String) channel.readOutbound() throws ClassCastException.
 *
 * THE FIX:
 *   Read the outbound ByteBuf explicitly, convert it to String using
 *   buf.toString(CharsetUtil.UTF_8), then release() the ByteBuf to
 *   prevent memory leaks. This is what the send() helper now does.
 *
 * PIPELINE EXPLANATION:
 *   Inbound  (client → server): writeInbound(String)
 *     → LineBasedFrameDecoder frames on \n
 *     → StringDecoder converts ByteBuf → String
 *     → CacheServerHandler.channelRead0() receives String, calls cache,
 *       calls ctx.writeAndFlush(String response)
 *
 *   Outbound (server → client): ctx.writeAndFlush(String)
 *     → StringEncoder converts String → ByteBuf
 *     → ByteBuf sits in EmbeddedChannel's outbound queue
 *     → readOutboundAsString() reads ByteBuf, converts to String, releases ByteBuf
 *
 * NO SERVER NEEDED:
 *   EmbeddedChannel is 100% in-memory. No ports, no TCP, no network.
 *   No need to start CacheServer before running these tests.
 */
@DisplayName("CacheServerHandler Tests")
class CacheServerHandlerTest {

    // -------------------------------------------------------------------------
    // Test infrastructure
    // -------------------------------------------------------------------------

    private EmbeddedChannel channel;
    private Cache<String, String> cache;
    private ServerMetrics metrics;
    private ServerConfig config;

    @BeforeEach
    void setUp() {
        cache   = new LRUCache<>(100);
        metrics = new ServerMetrics();
        config  = ServerConfig.defaults();

        channel = new EmbeddedChannel(
                new LineBasedFrameDecoder(8192),
                new StringDecoder(CharsetUtil.UTF_8),
                new StringEncoder(CharsetUtil.UTF_8),
                new CacheServerHandler(cache, metrics, config)
        );
    }

    @AfterEach
    void tearDown() {
        // Releases all pending ByteBufs in inbound/outbound queues.
        // Without this, Netty logs memory leak warnings for every unread response.
        channel.finishAndReleaseAll();
    }

    // -------------------------------------------------------------------------
    // Core helper methods
    // -------------------------------------------------------------------------

    /**
     * Reads the next outbound message from the EmbeddedChannel as a String.
     *
     * WHY NOT just cast readOutbound() to String?
     *   StringEncoder converts the handler's String response → ByteBuf.
     *   EmbeddedChannel stores that ByteBuf in the outbound queue.
     *   readOutbound() returns Object — the actual runtime type is ByteBuf.
     *   Casting to String throws ClassCastException.
     *
     * THE RIGHT APPROACH:
     *   1. Read the outbound object — it is a ByteBuf.
     *   2. Convert ByteBuf → String using buf.toString(CharsetUtil.UTF_8).
     *   3. Call buf.release() to return the ByteBuf to Netty's pool.
     *      Skipping release() causes "LEAK: ByteBuf.release() was not called" warnings.
     *
     * @return The response String, or null if no outbound message is queued.
     */
    private String readOutboundAsString() {
        // readOutbound() returns the raw object in the outbound queue.
        // Generic type is erased at runtime — we must handle ByteBuf explicitly.
        Object outbound = channel.readOutbound();
        if (outbound == null) {
            return null;
        }

        if (outbound instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) outbound;
            try {
                // Convert the ByteBuf bytes to a String using the same charset
                // that StringEncoder used to write them.
                return buf.toString(CharsetUtil.UTF_8);
            } finally {
                // ALWAYS release in a finally block.
                // If toString() throws (it won't for UTF-8, but defensive coding),
                // the buffer is still released and we don't leak memory.
                buf.release();
            }
        }

        // If somehow a String ends up in the queue (no encoder in pipeline),
        // handle it gracefully instead of crashing.
        return outbound.toString();
    }

    /**
     * Sends a command through the full Netty pipeline and returns the String response.
     *
     * Appends "\n" so LineBasedFrameDecoder recognizes the end of the command.
     * Without "\n", the command sits buffered and channelRead0() is never called.
     *
     * @param command Command string without trailing newline.
     * @return The String response from CacheServerHandler.
     */
    private String send(String command) {
        channel.writeInbound(command + "\n");
        String response = readOutboundAsString();

        assertNotNull(response,
                "No response produced for command: [" + command + "]\n" +
                        "Verify that:\n" +
                        "  1. CacheServerHandler.channelRead0() calls ctx.writeAndFlush()\n" +
                        "  2. StringEncoder is in the pipeline\n" +
                        "  3. The command verb is recognized (not silently swallowed)"
        );

        return response;
    }

    /**
     * Sends a command and asserts the response starts with "+".
     */
    private String sendExpectSuccess(String command) {
        String response = send(command);
        assertTrue(response.startsWith("+"),
                "Expected success (+) for [" + command + "] but got: [" + response + "]");
        return response;
    }

    /**
     * Sends a command and asserts the response starts with "-ERR".
     */
    private String sendExpectError(String command) {
        String response = send(command);
        assertTrue(response.startsWith("-ERR"),
                "Expected error (-ERR) for [" + command + "] but got: [" + response + "]");
        return response;
    }

    /**
     * Discards N outbound responses without asserting.
     * Use after commands whose +OK response you don't need to check,
     * so the outbound queue is empty before the next assertion.
     */
    private void drainOutbound(int count) {
        for (int i = 0; i < count; i++) {
            readOutboundAsString(); // read and discard
        }
    }

    // -------------------------------------------------------------------------
    // 1. PING
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("PING returns +PONG\\r\\n")
    void testPing_returnsPong() {
        assertEquals("+PONG\r\n", send("PING"));
    }

    @Test
    @DisplayName("PING is case-insensitive")
    void testPing_caseInsensitive() {
        assertEquals("+PONG\r\n", send("ping"));
        assertEquals("+PONG\r\n", send("Ping"));
        assertEquals("+PONG\r\n", send("pInG"));
    }

    @Test
    @DisplayName("Multiple PINGs all return PONG")
    void testPing_multipleTimes() {
        for (int i = 0; i < 5; i++) {
            assertEquals("+PONG\r\n", send("PING"),
                    "Failed on PING iteration " + i);
        }
    }

    // -------------------------------------------------------------------------
    // 2. GET
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("GET on missing key returns -ERR key not found")
    void testGet_missingKey_returnsError() {
        String response = sendExpectError("GET nonexistent");
        assertTrue(response.contains("not found"),
                "Error should mention 'not found': " + response);
    }

    @Test
    @DisplayName("GET after PUT returns the stored value")
    void testGet_afterPut_returnsValue() {
        send("PUT name Alice");
        drainOutbound(1); // discard +OK from PUT

        String response = sendExpectSuccess("GET name");
        assertTrue(response.contains("Alice"),
                "GET should return 'Alice', got: " + response);
    }

    @Test
    @DisplayName("GET returns exact formatted response: +<value>\\r\\n")
    void testGet_exactResponseFormat() {
        send("PUT greeting hello-world");
        drainOutbound(1);

        assertEquals("+hello-world\r\n", send("GET greeting"));
    }

    @Test
    @DisplayName("GET with no key argument returns -ERR")
    void testGet_missingArgument() {
        sendExpectError("GET");
    }

    @Test
    @DisplayName("GET key lookup is case-sensitive")
    void testGet_keyCaseSensitive() {
        send("PUT MyKey value1");
        drainOutbound(1);

        // "mykey" != "MyKey"
        sendExpectError("GET mykey");
    }

    @Test
    @DisplayName("GET records hit in metrics")
    void testGet_recordsHit() {
        send("PUT key val");
        drainOutbound(1);

        send("GET key"); // cache hit
        drainOutbound(1);

        ServerMetrics.MetricsSnapshot snap = metrics.snapshot();
        assertEquals(1, snap.hits,   "Should record 1 hit");
        assertEquals(0, snap.misses, "Should record 0 misses");
    }

    @Test
    @DisplayName("GET records miss in metrics")
    void testGet_recordsMiss() {
        send("GET doesnotexist"); // cache miss
        drainOutbound(1);

        ServerMetrics.MetricsSnapshot snap = metrics.snapshot();
        assertEquals(0, snap.hits,   "Should record 0 hits");
        assertEquals(1, snap.misses, "Should record 1 miss");
    }

    // -------------------------------------------------------------------------
    // 3. PUT
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("PUT returns +OK\\r\\n")
    void testPut_returnsOK() {
        assertEquals("+OK\r\n", send("PUT key value"));
    }

    @Test
    @DisplayName("PUT with only key and no value returns -ERR")
    void testPut_missingValue() {
        sendExpectError("PUT key");
    }

    @Test
    @DisplayName("PUT with no arguments returns -ERR")
    void testPut_noArgs() {
        sendExpectError("PUT");
    }

    @Test
    @DisplayName("PUT overwrites existing key")
    void testPut_overwritesKey() {
        send("PUT name Alice");
        send("PUT name Bob");
        drainOutbound(2);

        assertTrue(send("GET name").contains("Bob"),
                "Second PUT should overwrite: expected 'Bob'");
    }

    @Test
    @DisplayName("SET is an alias for PUT")
    void testSet_aliasForPut() {
        assertEquals("+OK\r\n", send("SET key value"));
        drainOutbound(1);

        assertTrue(send("GET key").contains("value"),
                "Value stored via SET should be retrievable via GET");
    }

    @Test
    @DisplayName("PUT records puts count in metrics")
    void testPut_recordsMetrics() {
        send("PUT a 1");
        send("PUT b 2");
        send("PUT c 3");
        drainOutbound(3);

        assertEquals(3, metrics.snapshot().totalPuts);
    }

    // -------------------------------------------------------------------------
    // 4. DELETE
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("DELETE returns +OK")
    void testDelete_returnsOK() {
        send("PUT key val");
        drainOutbound(1);

        assertEquals("+OK\r\n", send("DELETE key"));
    }

    @Test
    @DisplayName("DELETE removes key — GET returns error after")
    void testDelete_removesKey() {
        send("PUT key val");
        send("DELETE key");
        drainOutbound(2);

        sendExpectError("GET key");
    }

    @Test
    @DisplayName("DELETE on nonexistent key returns +OK (idempotent)")
    void testDelete_nonexistentKey_idempotent() {
        assertEquals("+OK\r\n", send("DELETE ghostkey"));
    }

    @Test
    @DisplayName("DEL is an alias for DELETE")
    void testDel_aliasForDelete() {
        send("PUT key val");
        drainOutbound(1);

        assertEquals("+OK\r\n", send("DEL key"));
    }

    @Test
    @DisplayName("DELETE with no argument returns -ERR")
    void testDelete_missingArgument() {
        sendExpectError("DELETE");
    }

    @Test
    @DisplayName("DELETE records deletes count in metrics")
    void testDelete_recordsMetrics() {
        send("PUT k v");
        send("DELETE k");
        drainOutbound(2);

        assertEquals(1, metrics.snapshot().totalDeletes);
    }

    // -------------------------------------------------------------------------
    // 5. STATS
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("STATS returns + prefixed response")
    void testStats_returnsSuccess() {
        sendExpectSuccess("STATS");
    }

    @Test
    @DisplayName("STATS response contains all required metric fields")
    void testStats_containsRequiredFields() {
        send("PUT k v");
        send("GET k");
        send("GET miss");
        drainOutbound(3);

        String response = send("STATS");
        assertTrue(response.contains("hits:"),    "Missing 'hits:' in:    " + response);
        assertTrue(response.contains("misses:"),  "Missing 'misses:' in:  " + response);
        assertTrue(response.contains("gets:"),    "Missing 'gets:' in:    " + response);
        assertTrue(response.contains("puts:"),    "Missing 'puts:' in:    " + response);
        assertTrue(response.contains("hitRate:"), "Missing 'hitRate:' in: " + response);
        assertTrue(response.contains("p50:"),     "Missing 'p50:' in:     " + response);
        assertTrue(response.contains("p99:"),     "Missing 'p99:' in:     " + response);
    }

    @Test
    @DisplayName("STATS reflects accurate operation counts")
    void testStats_accurateCounts() {
        send("PUT a 1");  // put 1
        send("PUT b 2");  // put 2
        send("GET a");    // hit 1
        send("GET c");    // miss 1
        drainOutbound(4);

        String stats = send("STATS");
        assertTrue(stats.contains("puts:2"),   "Expected puts:2 in:   " + stats);
        assertTrue(stats.contains("hits:1"),   "Expected hits:1 in:   " + stats);
        assertTrue(stats.contains("misses:1"), "Expected misses:1 in: " + stats);
        assertTrue(stats.contains("gets:2"),   "Expected gets:2 in:   " + stats);
    }

    // -------------------------------------------------------------------------
    // 6. FLUSH
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("FLUSH returns +OK")
    void testFlush_returnsOK() {
        assertEquals("+OK\r\n", send("FLUSH"));
    }

    // -------------------------------------------------------------------------
    // 7. Unknown commands and edge cases
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Unknown command returns -ERR")
    void testUnknownCommand_returnsError() {
        sendExpectError("FOOBAR");
    }

    @Test
    @DisplayName("Unknown command increments error counter")
    void testUnknownCommand_incrementsErrors() {
        send("BADCOMMAND");
        send("ALSOBAD");
        drainOutbound(2);

        assertEquals(2, metrics.snapshot().errors);
    }

    @Test
    @DisplayName("Empty line produces no response")
    void testEmptyLine_noResponse() {
        channel.writeInbound("\n");

        // readOutboundAsString() returns null when nothing is queued
        assertNull(readOutboundAsString(),
                "Empty line should produce no response");
    }

    @Test
    @DisplayName("Whitespace-only line produces no response")
    void testWhitespaceLine_noResponse() {
        channel.writeInbound("   \n");

        assertNull(readOutboundAsString(),
                "Whitespace-only line should produce no response");
    }

    // -------------------------------------------------------------------------
    // 8. Multi-command sequences
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("PUT → GET → DELETE sequence is consistent")
    void testSequence_putGetDelete() {
        assertEquals("+OK\r\n", send("PUT user alice"));

        assertTrue(send("GET user").contains("alice"));

        assertEquals("+OK\r\n", send("DELETE user"));

        sendExpectError("GET user");
    }

    @Test
    @DisplayName("DELETE one key does not affect other keys")
    void testDelete_doesNotAffectOtherKeys() {
        send("PUT key1 val1");
        send("PUT key2 val2");
        send("PUT key3 val3");
        drainOutbound(3);

        send("DELETE key2");
        drainOutbound(1);

        assertTrue(send("GET key1").contains("val1"), "key1 should still exist");
        assertTrue(send("GET key3").contains("val3"), "key3 should still exist");
        sendExpectError("GET key2");
    }

    @Test
    @DisplayName("Multiple overwrites — GET returns final value")
    void testOverwrite_finalValueCorrect() {
        send("PUT counter 1");
        send("PUT counter 2");
        send("PUT counter 3");
        drainOutbound(3);

        assertTrue(send("GET counter").contains("3"),
                "After 3 overwrites, GET should return '3'");
    }

    // -------------------------------------------------------------------------
    // 9. Metrics accuracy
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("All metrics counters accurate under mixed operations")
    void testMetrics_allCountersAccurate() {
        send("PUT a 1");      // put 1
        send("PUT b 2");      // put 2
        send("PUT c 3");      // put 3
        send("GET a");        // hit 1
        send("GET b");        // hit 2
        send("GET z");        // miss 1
        send("DELETE a");     // delete 1
        send("BADCOMMAND");   // error 1
        drainOutbound(8);

        ServerMetrics.MetricsSnapshot snap = metrics.snapshot();

        assertEquals(3, snap.totalPuts,    "puts");
        assertEquals(3, snap.totalGets,    "gets (2 hits + 1 miss)");
        assertEquals(2, snap.hits,         "hits");
        assertEquals(1, snap.misses,       "misses");
        assertEquals(1, snap.totalDeletes, "deletes");
        assertEquals(1, snap.errors,       "errors");

        // hit rate = 2/3 = 66.67%
        assertEquals(66.67, snap.hitRate, 0.1, "hit rate");
    }

    @Test
    @DisplayName("Latency metrics: p50/p99/mean are non-negative, p99 >= p50")
    void testMetrics_latencyNonNegative() {
        for (int i = 0; i < 20; i++) {
            send("PUT key-" + i + " val-" + i);
            send("GET key-" + i);
            drainOutbound(2);
        }

        ServerMetrics.MetricsSnapshot snap = metrics.snapshot();

        assertTrue(snap.p50Ms  >= 0, "p50 must be >= 0");
        assertTrue(snap.p99Ms  >= 0, "p99 must be >= 0");
        assertTrue(snap.meanMs >= 0, "mean must be >= 0");
        assertTrue(snap.p99Ms  >= snap.p50Ms,
                "p99 must be >= p50, got p50=" + snap.p50Ms + " p99=" + snap.p99Ms);
    }

    // -------------------------------------------------------------------------
    // 10. Case insensitivity for command verbs
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("All command verbs work in lowercase")
    void testCommandVerbs_allLowercase() {
        send("put testkey testval");
        drainOutbound(1);

        assertEquals("+PONG\r\n", send("ping"));
        assertTrue(send("get testkey").contains("testval"), "lowercase get");
        assertEquals("+OK\r\n",   send("put k2 v2"));
        drainOutbound(1);
        assertEquals("+OK\r\n",   send("delete k2"));
        assertTrue(send("stats").startsWith("+"), "lowercase stats");
    }

    @Test
    @DisplayName("Mixed case command verbs work")
    void testCommandVerbs_mixedCase() {
        assertEquals("+PONG\r\n", send("Ping"));
        assertEquals("+PONG\r\n", send("pInG"));

        assertEquals("+OK\r\n", send("Put mixedkey mixedval"));
        drainOutbound(1);
        assertTrue(send("Get mixedkey").contains("mixedval"));
    }
}