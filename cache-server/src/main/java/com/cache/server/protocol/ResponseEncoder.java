package com.cache.server.protocol;

import com.cache.api.CacheStats;

/**
 * ResponseEncoder formats server responses into wire-protocol strings
 * ready to be sent back to the client over TCP.
 *
 * RESPONSIBILITY:
 * This class has one job: data → formatted String.
 * It does NOT write to the network. CacheServerHandler calls
 * ResponseEncoder to format the string, then calls ctx.writeAndFlush()
 * to send it. Separation of concerns.
 *
 * PROTOCOL FORMAT:
 * Every response is a single line terminated with \r\n (CRLF).
 *
 * Success responses start with '+':   "+OK\r\n", "+Alice\r\n", "+PONG\r\n"
 * Error responses start with '-':     "-ERR key not found\r\n"
 *
 * WHY \r\n (CRLF) INSTEAD OF JUST \n?
 * RFC 5321 (SMTP), RFC 7230 (HTTP/1.1), and Redis RESP all use CRLF.
 * Telnet sends CRLF by default. Using CRLF means:
 *   - `telnet localhost 6379` works out of the box for manual testing.
 *   - nc (netcat) with line-mode also works.
 *   - LineBasedFrameDecoder on the client side strips both \r\n and \n.
 *
 * WHY '+' AND '-' PREFIX?
 * Inspired by Redis RESP (REdis Serialization Protocol).
 * Single-character prefix means clients can check response[0] without
 * parsing the full line. '+' = success, '-' = error. Binary-safe.
 *
 * DESIGN — Static methods, no state:
 * ResponseEncoder has no instance state — all methods are static.
 * This makes it a pure function namespace. CacheServerHandler calls
 * ResponseEncoder.ok(), ResponseEncoder.value("Alice"), etc.
 * No instantiation needed.
 *
 * EXTENDING THE PROTOCOL:
 * To add a new response type, add a static method here.
 * CacheServerHandler stays unchanged — it just calls the new method.
 * Open/Closed Principle: open for extension (new methods), closed for
 * modification (existing methods don't change).
 */
public final class ResponseEncoder {

    // -------------------------------------------------------------------------
    // Protocol constants
    // -------------------------------------------------------------------------

    /**
     * CRLF line terminator appended to every response.
     * Netty's StringEncoder will convert this String to bytes.
     * The client's LineBasedFrameDecoder strips this when reading.
     */
    private static final String CRLF = "\r\n";

    /** Prefix for all success responses. */
    private static final String SUCCESS_PREFIX = "+";

    /** Prefix for all error responses. */
    private static final String ERROR_PREFIX   = "-ERR ";

    // =========================================================================
    // Success responses
    // =========================================================================

    /**
     * Generic success acknowledgment. Sent for PUT, DELETE, FLUSH.
     *
     * Wire format: "+OK\r\n"
     *
     * @return The encoded OK response string.
     */
    public static String ok() {
        return SUCCESS_PREFIX + "OK" + CRLF;
    }

    /**
     * PING response. Sent in reply to PING commands.
     *
     * Wire format: "+PONG\r\n"
     *
     * WHY A SEPARATE METHOD instead of ok()?
     * PONG is semantically distinct from OK — it means "I am alive and
     * responding", not "your operation succeeded". Clients use PONG
     * specifically to detect that the server is responding to commands,
     * not just accepting TCP connections (which a dead server might still do
     * if the OS's TCP stack is up).
     *
     * @return The encoded PONG response string.
     */
    public static String pong() {
        return SUCCESS_PREFIX + "PONG" + CRLF;
    }

    /**
     * Returns a value retrieved from the cache. Sent in reply to GET.
     *
     * Wire format: "+{value}\r\n"
     * Example:     "+Alice\r\n"
     *
     * LIMITATION: values cannot contain \r\n because that would be
     * interpreted as end-of-line by the client's frame decoder.
     * Values with \r\n must be Base64-encoded before storage.
     * (This is a known protocol limitation, documented in WireProtocol.md.)
     *
     * @param value The value retrieved from the cache. Must not be null.
     * @return The encoded value response string.
     */
    public static String value(String value) {
        return SUCCESS_PREFIX + value + CRLF;
    }

    /**
     * Returns cache statistics. Sent in reply to STATS.
     *
     * Wire format: "+hits:{N} misses:{N} evictions:{N} size:{N} hitRate:{N.NN}%\r\n"
     * Example:     "+hits:1420 misses:231 evictions:88 size:256 hitRate:86.00%\r\n"
     *
     * All stats on one line — simple for clients to parse by splitting on space.
     * Each field is key:value separated by spaces.
     *
     * @param stats    The CacheStats snapshot from the cache engine.
     * @param cacheSize Current number of entries in the cache.
     * @return The encoded stats response string.
     */
    public static String stats(CacheStats stats, int cacheSize) {
        long hits      = stats.hits();
        long misses    = stats.misses();
        long evictions = stats.evictions();
        long total     = hits + misses;

        // Hit rate: 0.00% if no operations have occurred yet (avoids division by zero)
        double hitRate = total == 0 ? 0.0 : (double) hits / total * 100.0;

        return SUCCESS_PREFIX
                + "hits:"      + hits
                + " misses:"   + misses
                + " evictions:" + evictions
                + " size:"     + cacheSize
                + " hitRate:"  + String.format("%.2f", hitRate) + "%"
                + CRLF;
    }

    /**
     * Returns cache statistics using individual counter values.
     * Use this overload when you have raw counters instead of a CacheStats object.
     *
     * @param hits       Total cache hits.
     * @param misses     Total cache misses.
     * @param evictions  Total cache evictions.
     * @param size       Current number of entries.
     * @return The encoded stats response string.
     */
    public static String stats(long hits, long misses, long evictions, int size) {
        long total     = hits + misses;
        double hitRate = total == 0 ? 0.0 : (double) hits / total * 100.0;

        return SUCCESS_PREFIX
                + "hits:"       + hits
                + " misses:"    + misses
                + " evictions:" + evictions
                + " size:"      + size
                + " hitRate:"   + String.format("%.2f", hitRate) + "%"
                + CRLF;
    }

    // =========================================================================
    // Error responses
    // =========================================================================

    /**
     * Generic error response with a custom message.
     *
     * Wire format: "-ERR {message}\r\n"
     * Example:     "-ERR key not found\r\n"
     *
     * WHEN TO USE:
     * Use this for application-level errors (key not found, cache full).
     * For protocol errors (bad command), use fromProtocolException().
     *
     * @param message The error message. Should be human-readable and specific.
     *                Must not contain \r\n — if the message itself has newlines,
     *                replace them with spaces before passing here.
     * @return The encoded error response string.
     */
    public static String error(String message) {
        // Sanitize message: replace any embedded CRLF or LF to prevent
        // response injection (a malicious server could otherwise inject
        // additional lines into the response stream).
        String sanitized = message.replace("\r\n", " ").replace("\n", " ");
        return ERROR_PREFIX + sanitized + CRLF;
    }

    /**
     * Error response for a key-not-found GET result.
     *
     * Wire format: "-ERR key not found: {key}\r\n"
     *
     * We include the key in the error message so the client can confirm
     * which key was missing (useful when clients pipeline multiple GETs).
     *
     * @param key The key that was not found in the cache.
     * @return The encoded not-found error response string.
     */
    public static String keyNotFound(String key) {
        return ERROR_PREFIX + "key not found: " + key + CRLF;
    }

    /**
     * Error response built from a ProtocolException.
     *
     * Formats the exception's message as a standard error response.
     * The ErrorCode is NOT included in the wire response (it's for server-side
     * logging only). Clients see only the human-readable message.
     *
     * Wire format: "-ERR {exception.getMessage()}\r\n"
     *
     * @param e The ProtocolException thrown by CommandParser.
     * @return The encoded protocol error response string.
     */
    public static String fromProtocolException(ProtocolException e) {
        return error(e.getMessage());
    }

    /**
     * Error response for an internal server error.
     *
     * Wire format: "-ERR internal server error\r\n"
     *
     * Used in CacheServerHandler.exceptionCaught() when an unexpected
     * Throwable occurs. We don't leak internal error details to clients
     * (no stack traces, no exception class names) — those go in server logs.
     *
     * @return The encoded internal error response string.
     */
    public static String internalError() {
        return ERROR_PREFIX + "internal server error" + CRLF;
    }

    /**
     * Error response for when the server has reached maximum connections.
     *
     * Wire format: "-ERR server at capacity, try again later\r\n"
     *
     * Sent by ConnectionManager when maxConnections is exceeded.
     * After sending this, the server closes the channel.
     *
     * @return The encoded capacity error response string.
     */
    public static String serverAtCapacity() {
        return ERROR_PREFIX + "server at capacity, try again later" + CRLF;
    }

    // =========================================================================
    // Utility
    // =========================================================================

    /**
     * Checks whether a response string represents a success (starts with '+').
     *
     * Useful in client-side code and integration tests:
     *   String response = // ... read from socket
     *   if (ResponseEncoder.isSuccess(response)) { ... }
     *
     * @param response A response line received from the server.
     * @return true if the response indicates success.
     */
    public static boolean isSuccess(String response) {
        return response != null && response.startsWith(SUCCESS_PREFIX);
    }

    /**
     * Checks whether a response string represents an error (starts with '-').
     *
     * @param response A response line received from the server.
     * @return true if the response indicates an error.
     */
    public static boolean isError(String response) {
        return response != null && response.startsWith("-");
    }

    /**
     * Extracts the value from a success response string.
     * Strips the '+' prefix and trailing \r\n.
     *
     * Example: "+Alice\r\n" → "Alice"
     * Example: "+OK\r\n"   → "OK"
     *
     * Use in client code to extract the value after checking isSuccess().
     *
     * @param response A success response line (must start with '+').
     * @return The value portion of the response, without prefix or CRLF.
     * @throws IllegalArgumentException if the response is not a success response.
     */
    public static String extractValue(String response) {
        if (!isSuccess(response)) {
            throw new IllegalArgumentException(
                    "Cannot extract value from non-success response: " + response
            );
        }
        // Strip '+' prefix and trailing whitespace/CRLF
        return response.substring(1).stripTrailing();
    }

    // =========================================================================
    // Private constructor — static utility class, not instantiable
    // =========================================================================

    private ResponseEncoder() {
        throw new UnsupportedOperationException("ResponseEncoder is a static utility class");
    }
}