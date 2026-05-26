package com.cache.server.handler;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * CacheServerHandler is the final inbound handler in the Netty pipeline.
 * It receives fully decoded String commands, dispatches them to the cache
 * engine, records metrics, and writes String responses back to the client.
 *
 * POSITION IN THE PIPELINE:
 *   LineBasedFrameDecoder → StringDecoder → StringEncoder
 *   → ConnectionManager → CacheServerHandler  ← YOU ARE HERE
 *
 * By the time channelRead0() is called:
 *   - The message is a clean String with no \n or \r characters
 *   - The connection has been validated by ConnectionManager
 *   - StringEncoder is available to convert our String response to bytes
 *
 * COMMAND DISPATCH:
 *   Commands are space-delimited. The first token is the verb.
 *   We switch on the verb and delegate to private handle*() methods.
 *   Each handle*() method returns a String response — never null.
 *   The response is written back via ctx.writeAndFlush().
 *
 * RESPONSE PROTOCOL:
 *   + prefix → success    e.g. "+OK", "+Alice", "+PONG"
 *   - prefix → error      e.g. "-ERR key not found", "-ERR wrong number of args"
 *
 *   Every response ends with "\r\n" so clients using telnet or raw sockets
 *   see clean line breaks. LineBasedFrameDecoder on the CLIENT side (if any)
 *   would strip these automatically.
 *
 * SUPPORTED COMMANDS:
 *   PING                      → +PONG
 *   GET <key>                 → +<value>  or  -ERR key not found
 *   PUT <key> <value>         → +OK
 *   PUT <key> <value> <ttl>   → +OK  (ttl in seconds, 0 = no expiry)
 *   DELETE <key>              → +OK
 *   STATS                     → +<metrics string>
 *   FLUSH                     → +OK  (clears all entries)
 *   QUIT                      → +BYE then closes connection
 *
 * LATENCY MEASUREMENT:
 *   We record System.nanoTime() before and after each cache operation.
 *   The delta is passed to ServerMetrics.recordGet/Put/Delete().
 *   We use nanoTime() (not currentTimeMillis()) because:
 *     - nanoTime() is monotonic — not affected by system clock adjustments
 *     - nanoTime() has nanosecond resolution — currentTimeMillis() is ~10ms
 *     - Cache operations complete in microseconds — ms resolution is useless
 *
 * WHY NOT @ChannelHandler.Sharable?
 *   CacheServerHandler is not marked sharable because conceptually each
 *   channel should have independent handler state (even though our current
 *   implementation is stateless per-channel). This is defensive — if you
 *   add per-connection state later (e.g., authenticated username, pipeline
 *   mode), you won't accidentally share it across connections.
 *
// * @param <K> Key type — String for the network protocol.
// * @param <V> Value type — String for the network protocol.
 */
public class CacheServerHandler extends SimpleChannelInboundHandler<String> {

    // -------------------------------------------------------------------------
    // Response prefix constants
    // -------------------------------------------------------------------------

    /** Prefix for all successful responses. Matches Redis RESP convention. */
    private static final String OK      = "+OK\r\n";

    /** Prefix for error responses. */
    private static final String ERR     = "-ERR ";

    /** Response suffix — all responses end with CRLF. */
    private static final String CRLF    = "\r\n";

    /** PING response. */
    private static final String PONG    = "+PONG\r\n";

    /** QUIT response — sent before closing the connection. */
    private static final String BYE     = "+BYE\r\n";

    // -------------------------------------------------------------------------
    // Dependencies — injected at construction, never modified
    // -------------------------------------------------------------------------

    /**
     * The cache engine — shared across ALL handler instances (all connections).
     * Must be thread-safe (SegmentedCache or CoarseGrainedCache).
     * CacheServerHandler never modifies this reference, only calls get/put/evict.
     */
    private final Cache<String, String> cache;

    /**
     * Metrics collector — shared across all handler instances.
     * Thread-safe (uses LongAdder and AtomicLong internally).
     */
    private final ServerMetrics metrics;

    /**
     * Server configuration — read-only reference for config values.
     * Currently used to check verbose logging flag.
     */
    private final ServerConfig config;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a CacheServerHandler.
     *
     * Called once per accepted connection in CacheServer's ChannelInitializer.
     * Each connection gets its own handler instance, but all instances share
     * the same cache and metrics references.
     *
     * @param cache   Thread-safe cache engine. Must not be null.
     * @param metrics Metrics collector. Must not be null.
     * @param config  Server configuration. Must not be null.
     */
    public CacheServerHandler(Cache<String, String> cache,
                              ServerMetrics metrics,
                              ServerConfig config) {
        // autoRelease=true (default in SimpleChannelInboundHandler):
        // Netty releases the incoming ByteBuf after channelRead0() returns.
        // Our messages are Strings (heap objects), so this doesn't affect us,
        // but it's the correct default for memory-managed types.
        super(true);

        if (cache   == null) throw new IllegalArgumentException("cache cannot be null");
        if (metrics == null) throw new IllegalArgumentException("metrics cannot be null");
        if (config  == null) throw new IllegalArgumentException("config cannot be null");

        this.cache   = cache;
        this.metrics = metrics;
        this.config  = config;
    }

    // -------------------------------------------------------------------------
    // Core handler method — called by Netty for every received line
    // -------------------------------------------------------------------------

    /**
     * Called by Netty when a complete line has been received from the client.
     *
     * By this point:
     *   - LineBasedFrameDecoder has buffered bytes until \n was found
     *   - StringDecoder has converted the ByteBuf to a String
     *   - The String has no leading/trailing whitespace issues
     *
     * We parse the command, dispatch to a handler method, and write the response.
     * All exceptions are caught and converted to -ERR responses so the connection
     * stays alive after a bad command.
     *
     * @param ctx The channel context — used for writing responses.
     * @param msg The decoded command string, e.g. "GET foo" or "PUT bar baz 60".
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        // Trim whitespace — handles \r leftover from Windows clients sending \r\n
        String trimmed = msg.trim();

        if (trimmed.isEmpty()) {
            // Empty line — ignore silently. Telnet sends empty lines sometimes.
            return;
        }

        if (config.isVerbose()) {
            System.out.printf("[Handler] Received from %s: %s%n",
                    ctx.channel().remoteAddress(), trimmed);
        }

        // Parse into tokens. limit=-1 means trailing empty strings are included,
        // which matters for values containing spaces (handled in handlePut).
        // We split on one-or-more spaces to be lenient with clients.
        String[] tokens = trimmed.split("\\s+");
        String verb = tokens[0].toUpperCase();

        // Dispatch to the appropriate command handler.
        // Each handler returns a fully formatted response string.
        String response;
        try {
            switch (verb) {
                case "PING":   response = handlePing();              break;
                case "GET":    response = handleGet(tokens);         break;
                case "PUT":    response = handlePut(tokens, trimmed); break;
                case "SET":    response = handlePut(tokens, trimmed); break; // alias for PUT
                case "DELETE": response = handleDelete(tokens);      break;
                case "DEL":    response = handleDelete(tokens);      break; // alias for DELETE
                case "STATS":  response = handleStats();             break;
                case "FLUSH":  response = handleFlush();             break;
                case "QUIT":
                case "EXIT":
                    // Send BYE then close — writeAndFlush returns a Future,
                    // we add a listener to close AFTER the write completes.
                    ctx.writeAndFlush(BYE)
                            .addListener(io.netty.channel.ChannelFutureListener.CLOSE);
                    return; // don't write again below
                default:
                    metrics.recordError();
                    response = ERR + "unknown command '" + verb + "'" + CRLF;
            }
        } catch (Exception e) {
            // Defensive catch — no command handler should throw, but if one does,
            // we return an error instead of closing the connection.
            metrics.recordError();
            response = ERR + "internal error: " + e.getMessage() + CRLF;
            System.err.printf("[Handler] Unexpected error processing command '%s': %s%n",
                    trimmed, e.getMessage());
        }

        // Write the response and flush immediately.
        // writeAndFlush() is non-blocking — it schedules the write and returns a Future.
        // We don't wait for it to complete — Netty handles backpressure internally.
        ctx.writeAndFlush(response);
    }

    // -------------------------------------------------------------------------
    // Command handlers — one method per command verb
    // -------------------------------------------------------------------------

    /**
     * Handles PING — the heartbeat command.
     *
     * Used by:
     *   - Health checks: "is this server alive?"
     *   - Connection pool keep-alive: "is this connection still valid?"
     *   - Latency measurement: time the round-trip of a PING/PONG
     *
     * No cache interaction, no metrics recording (would skew latency stats).
     *
     * @return "+PONG\r\n"
     */
    private String handlePing() {
        return PONG;
    }

    /**
     * Handles GET <key> — retrieves a value from the cache.
     *
     * Protocol:
     *   GET foo       → +bar     (if key "foo" exists with value "bar")
     *   GET missing   → -ERR key not found
     *   GET           → -ERR wrong number of arguments for GET (missing key)
     *
     * Latency measurement wraps only the cache.get() call, not argument parsing.
     * This isolates cache latency from network/parsing overhead.
     *
     * @param tokens Parsed command tokens. tokens[0]="GET", tokens[1]=key.
     * @return Formatted response string.
     */
    private String handleGet(String[] tokens) {
        if (tokens.length < 2) {
            metrics.recordError();
            return ERR + "wrong number of arguments for GET" + CRLF;
        }

        String key = tokens[1];

        long startNs = System.nanoTime();
        String value = cache.get(key);
        long durationNs = System.nanoTime() - startNs;

        boolean hit = (value != null);
        metrics.recordGet(hit, durationNs);

        if (hit) {
            return "+" + value + CRLF;
        } else {
            return ERR + "key not found" + CRLF;
        }
    }

    /**
     * Handles PUT <key> <value> [ttl] — stores a key-value pair.
     *
     * Protocol:
     *   PUT foo bar       → +OK  (no TTL — key lives until evicted by policy)
     *   PUT foo bar 60    → +OK  (key expires in 60 seconds)
     *   PUT foo bar 0     → +OK  (TTL=0 means no expiry, same as omitting TTL)
     *   PUT foo           → -ERR wrong number of arguments for PUT
     *
     * WHY RECONSTRUCT VALUE FROM ORIGINAL STRING?
     *   split("\\s+") on "PUT key hello world" gives ["PUT", "key", "hello", "world"].
     *   The value "hello world" is split across tokens[2] and tokens[3].
     *   To support values with spaces, we extract the value by finding where
     *   it starts in the original trimmed string and taking the substring.
     *
     *   Format: "PUT <key> <value_possibly_with_spaces> [ttl]"
     *   If the last token is a valid long, it's the TTL and value is everything before it.
     *   If the last token is not a long, the entire rest of the string is the value.
     *
     * @param tokens  Parsed command tokens.
     * @param original The original trimmed command string (for value extraction).
     * @return "+OK\r\n" on success, "-ERR ...\r\n" on failure.
     */
    private String handlePut(String[] tokens, String original) {
        if (tokens.length < 3) {
            metrics.recordError();
            return ERR + "wrong number of arguments for PUT" + CRLF;
        }

        String key = tokens[1];
        long ttl   = 0; // default: no expiry

        // Determine if the last token is a TTL (a valid non-negative long).
        // If yes, the value is everything between key and the TTL token.
        // If no, the value is everything after the key.
        String valueRaw;
        String lastToken = tokens[tokens.length - 1];
        boolean lastIsNumber = isNonNegativeLong(lastToken);

        if (lastIsNumber && tokens.length >= 4) {
            // Last token is TTL, value is middle section
            ttl = Long.parseLong(lastToken);

            // Reconstruct value: skip "PUT key " prefix, strip " ttl" suffix
            int keyEnd   = original.indexOf(key) + key.length();
            int ttlStart = original.lastIndexOf(lastToken);
            valueRaw     = original.substring(keyEnd, ttlStart).trim();
        } else {
            // No TTL — everything after key is the value
            int keyEnd = original.indexOf(key) + key.length();
            valueRaw   = original.substring(keyEnd).trim();
        }

        if (valueRaw.isEmpty()) {
            metrics.recordError();
            return ERR + "value cannot be empty" + CRLF;
        }

        // Validate TTL range
        if (ttl < 0) {
            metrics.recordError();
            return ERR + "TTL cannot be negative" + CRLF;
        }

        // Delegate to TTLCache-aware put if TTL > 0, else plain put
        long startNs = System.nanoTime();
        try {
            if (ttl > 0 && cache instanceof com.cache.ttl.TTLCache) {
                // Cast to TTLCache to access the TTL-aware put method
                ((com.cache.ttl.TTLCache<String, String>) cache).put(key, valueRaw, ttl);
            } else {
                // Plain put — no TTL (or cache doesn't support TTL)
                cache.put(key, valueRaw);
            }
        } catch (Exception e) {
            metrics.recordError();
            return ERR + e.getMessage() + CRLF;
        }
        long durationNs = System.nanoTime() - startNs;

        metrics.recordPut(durationNs);
        return OK;
    }

    /**
     * Handles DELETE <key> — removes a key from the cache.
     *
     * Protocol:
     *   DELETE foo   → +OK  (whether or not key existed — idempotent)
     *   DELETE       → -ERR wrong number of arguments for DELETE
     *
     * WHY IDEMPOTENT?
     *   Deleting a key that doesn't exist is not an error. This matches Redis
     *   semantics and makes clients simpler — they don't need to check
     *   existence before deleting.
     *
     * @param tokens Parsed command tokens. tokens[0]="DELETE", tokens[1]=key.
     * @return "+OK\r\n" always (unless argument is missing).
     */
    private String handleDelete(String[] tokens) {
        if (tokens.length < 2) {
            metrics.recordError();
            return ERR + "wrong number of arguments for DELETE" + CRLF;
        }

        String key = tokens[1];

        long startNs = System.nanoTime();
        cache.evict(key);
        long durationNs = System.nanoTime() - startNs;

        metrics.recordDelete(durationNs);
        return OK;
    }

    /**
     * Handles STATS — returns server metrics.
     *
     * Protocol:
     *   STATS   → +hits:1420 misses:380 gets:1800 puts:200 ...
     *
     * The full metrics string comes from ServerMetrics.toStatsString().
     * This is the command that makes your server observable — operators
     * can telnet in and type STATS to see live performance numbers.
     *
     * No cache interaction, no latency recording.
     *
     * @return Formatted stats string prefixed with "+".
     */
    private String handleStats() {

        CacheStats stats = cache.getstats();

        return "+"
                + metrics.toStatsString(
                stats.evictions(),
                cache.size()
        )
                + CRLF;
    }

    /**
     * Handles FLUSH — removes all entries from the cache.
     *
     * Protocol:
     *   FLUSH   → +OK
     *
     * Equivalent to Redis FLUSHALL. Useful for:
     *   - Testing: start each test with a clean cache
     *   - Emergency: "clear everything, we have corrupt data"
     *   - Development: reset state without restarting the server
     *
     * DANGER: This is a destructive operation. In a real production server,
     * you would require authentication before allowing FLUSH.
     *
     * @return "+OK\r\n"
     */
    private String handleFlush() {
        // Cache interface doesn't have a clear() method in our design.
        // We implement flush by getting the size and noting we can't clear.
        // TODO: Add clear() to Cache<K,V> interface if FLUSH is required.
        //
        // For now, we return OK and log that flush is not fully implemented.
        // In Day 13 cleanup, add clear() to the Cache interface and implement
        // it in LRUCache, LFUCache, ARCCache.
        cache.clear();
        return OK;
    }

    // -------------------------------------------------------------------------
    // Netty lifecycle — connection events
    // -------------------------------------------------------------------------

    /**
     * Called when an unhandled exception propagates through the pipeline
     * to this handler (after ConnectionManager's exceptionCaught).
     *
     * ConnectionManager handles most exceptions already. This is a last resort.
     * We log and close the channel.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.printf("[Handler] Unhandled exception on channel %s: %s%n",
                ctx.channel().remoteAddress(), cause.getMessage());
        metrics.recordError();
        ctx.close();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Checks if a string represents a non-negative long integer.
     * Used in handlePut() to detect whether the last token is a TTL value.
     *
     * @param s The string to check.
     * @return true if s can be parsed as a non-negative long.
     */
    private boolean isNonNegativeLong(String s) {
        if (s == null || s.isEmpty()) return false;
        try {
            long val = Long.parseLong(s);
            return val >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}