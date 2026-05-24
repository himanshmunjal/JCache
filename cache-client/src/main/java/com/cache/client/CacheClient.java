package com.cache.client;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * CacheClient is a single-connection Java client for the CacheServer.
 *
 * It provides a clean, typed API over the raw TCP wire protocol:
 *   cache.put("name", "Alice", 60)  →  sends "PUT name Alice 60\n"
 *   cache.get("name")               →  sends "GET name\n", returns "Alice"
 *
 * DESIGN DECISIONS:
 *
 *   Single connection per CacheClient instance.
 *   For concurrent usage across threads, wrap this in ConnectionPool,
 *   which manages a pool of CacheClient instances and hands them out safely.
 *   CacheClient itself is NOT thread-safe — one thread uses one client at a time.
 *
 *   Why NOT thread-safe?
 *   Making it thread-safe would require synchronizing every send/receive pair,
 *   which serializes all operations through one connection anyway — no benefit.
 *   The correct pattern is: pool of single-threaded clients, each owned by one
 *   thread at a time. This is exactly how HikariCP and Jedis work.
 *
 *   Blocking I/O (not NIO).
 *   The server uses Netty NIO. The client uses blocking java.net.Socket.
 *   This is intentional — the client is simple, single-threaded, and doesn't
 *   need to multiplex many connections. Blocking I/O with one thread per
 *   connection is simpler and just as fast for low-concurrency clients.
 *   For high-concurrency clients (thousands of connections), use Netty on the
 *   client side too — but that's overkill for this project.
 *
 *   Read timeout (default 5 seconds).
 *   If the server crashes mid-request, readLine() would block forever without
 *   a timeout. SO_TIMEOUT causes readLine() to throw SocketTimeoutException
 *   after the timeout elapses, allowing the caller to handle it gracefully.
 *
 * WIRE PROTOCOL (matches CacheServerHandler):
 *   Requests:  one line per command, \n terminated, space-delimited arguments
 *   Responses: one line, starts with + (success) or - (error)
 *
 *   PING           → +PONG
 *   GET key        → +value  or  -ERR key not found
 *   PUT key val    → +OK
 *   PUT key val 60 → +OK  (with 60-second TTL)
 *   DELETE key     → +OK
 *   STATS          → +hits:N misses:N evictions:N size:N
 *   FLUSH          → +OK
 *
 * USAGE:
 *   try (CacheClient client = new CacheClient("localhost", 6379)) {
 *       client.put("session:user1", "token_abc123", 3600);
 *       String token = client.get("session:user1");
 *       Map<String, String> stats = client.stats();
 *   }
 */
public class CacheClient implements Closeable {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /** Default read timeout in milliseconds. Prevents indefinite blocking. */
    private static final int DEFAULT_READ_TIMEOUT_MS = 5_000;

    /**
     * Response prefix for success. Server sends "+OK", "+value", "+PONG", etc.
     * Strip this prefix to get the actual value.
     */
    private static final char SUCCESS_PREFIX = '+';

    /**
     * Response prefix for errors. Server sends "-ERR key not found", etc.
     * Strip this prefix to get the error message.
     */
    private static final char ERROR_PREFIX   = '-';

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Remote server hostname or IP address. */
    private final String host;

    /** Remote server port. */
    private final int port;

    /** Read timeout in milliseconds. Applied to socket SO_TIMEOUT. */
    private final int readTimeoutMs;

    /**
     * The TCP connection to the server.
     * Created in connect(), closed in close().
     * null if not yet connected or after close().
     */
    private Socket socket;

    /**
     * Buffered writer for sending commands to the server.
     * PrintWriter.println() writes the command + \n in one call.
     * autoFlush=true means the buffer is flushed after every println().
     *
     * WHY autoFlush=true?
     * Without auto-flush, println() buffers the command locally.
     * The server would block waiting for a complete line that's sitting
     * in the client's buffer. autoFlush ensures every command is sent immediately.
     */
    private PrintWriter writer;

    /**
     * Buffered reader for receiving responses from the server.
     * readLine() blocks until the server sends a complete line (\n terminated).
     * Returns null if the connection is closed by the server.
     */
    private BufferedReader reader;

    /** Whether this client is currently connected. */
    private boolean connected = false;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Creates a CacheClient and immediately connects to the server.
     *
     * @param host Server hostname or IP (e.g., "localhost", "10.0.1.5").
     * @param port Server port (e.g., 6379).
     * @throws IOException if the connection cannot be established.
     */
    public CacheClient(String host, int port) throws IOException {
        this(host, port, DEFAULT_READ_TIMEOUT_MS);
    }

    /**
     * Creates a CacheClient with a custom read timeout and immediately connects.
     *
     * @param host          Server hostname or IP.
     * @param port          Server port.
     * @param readTimeoutMs Maximum milliseconds to wait for a server response.
     *                      0 means wait forever (not recommended in production).
     * @throws IOException if the connection cannot be established.
     */
    public CacheClient(String host, int port, int readTimeoutMs) throws IOException {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (readTimeoutMs < 0) {
            throw new IllegalArgumentException("readTimeoutMs cannot be negative");
        }

        this.host          = host;
        this.port          = port;
        this.readTimeoutMs = readTimeoutMs;

        connect(); // establish connection immediately on construction
    }

    // -------------------------------------------------------------------------
    // Connection lifecycle
    // -------------------------------------------------------------------------

    /**
     * Opens a TCP connection to the server and initializes reader/writer streams.
     *
     * Called automatically by the constructor. Can be called again after close()
     * to reconnect (e.g., after a server restart in long-running clients).
     *
     * @throws IOException if the connection fails.
     */
    public void connect() throws IOException {
        if (connected) {
            return; // already connected
        }

        socket = new Socket(host, port);

        // SO_TIMEOUT: if readLine() doesn't receive data within this window,
        // it throws SocketTimeoutException. Prevents indefinite blocking.
        socket.setSoTimeout(readTimeoutMs);

        // TCP_NODELAY: disable Nagle's algorithm on the client side too.
        // Each command is sent immediately, not buffered waiting for more data.
        socket.setTcpNoDelay(true);

        // autoFlush=true: every println() immediately flushes to the socket.
        writer = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())),
                true // autoFlush
        );

        reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream())
        );

        connected = true;
    }

    /**
     * Closes the connection to the server.
     * Implements Closeable so CacheClient works in try-with-resources blocks.
     *
     * After close(), this client cannot be used until connect() is called again.
     * ConnectionPool calls this when a pooled connection is unhealthy.
     */
    @Override
    public void close() {
        connected = false;

        if (writer != null) {
            writer.close();
            writer = null;
        }

        if (reader != null) {
            try { reader.close(); } catch (IOException ignored) {}
            reader = null;
        }

        if (socket != null && !socket.isClosed()) {
            try { socket.close(); } catch (IOException ignored) {}
            socket = null;
        }
    }

    // -------------------------------------------------------------------------
    // Cache operations — the public API
    // -------------------------------------------------------------------------

    /**
     * Sends a PING to the server and returns true if +PONG is received.
     * Used by ConnectionPool to check if a connection is still alive.
     *
     * @return true if the server responds with PONG, false otherwise.
     */
    public boolean ping() {
        try {
            String response = sendCommand("PING");
            return "PONG".equals(response);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Retrieves the value for the given key.
     *
     * @param key The cache key. Cannot be null or empty.
     * @return The value, or null if the key doesn't exist or has expired.
     * @throws CacheClientException if the server returns an error response.
     * @throws IOException if the network connection fails.
     */
    public String get(String key) throws IOException {
        validateKey(key);
        return sendCommand("GET " + key);
    }

    /**
     * Stores a key-value pair with no TTL (persists until evicted by policy).
     *
     * @param key   The cache key. Cannot be null or empty.
     * @param value The value to store. Cannot be null or empty.
     * @throws CacheClientException if the server returns an error response.
     * @throws IOException if the network connection fails.
     */
    public void put(String key, String value) throws IOException {
        validateKey(key);
        validateValue(value);
        sendCommand("PUT " + key + " " + value);
    }

    /**
     * Stores a key-value pair with a TTL in seconds.
     *
     * @param key        The cache key. Cannot be null or empty.
     * @param value      The value to store. Cannot be null or empty.
     * @param ttlSeconds Time-to-live in seconds. 0 means no expiry.
     * @throws IllegalArgumentException if ttlSeconds is negative.
     * @throws CacheClientException if the server returns an error response.
     * @throws IOException if the network connection fails.
     */
    public void put(String key, String value, long ttlSeconds) throws IOException {
        validateKey(key);
        validateValue(value);
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("TTL cannot be negative: " + ttlSeconds);
        }
        sendCommand("PUT " + key + " " + value + " " + ttlSeconds);
    }

    /**
     * Deletes a key from the cache.
     * No-op on the server side if the key doesn't exist.
     *
     * @param key The key to delete.
     * @throws CacheClientException if the server returns an error response.
     * @throws IOException if the network connection fails.
     */
    public void delete(String key) throws IOException {
        validateKey(key);
        sendCommand("DELETE " + key);
    }

    /**
     * Removes all entries from the cache.
     * Equivalent to Redis FLUSHALL.
     *
     * @throws CacheClientException if the server returns an error response.
     * @throws IOException if the network connection fails.
     */
    public void flush() throws IOException {
        sendCommand("FLUSH");
    }

    /**
     * Returns server statistics as a key-value map.
     *
     * The server sends a single line like:
     *   +hits:142 misses:31 evictions:8 size:256
     *
     * This method parses it into:
     *   {"hits": "142", "misses": "31", "evictions": "8", "size": "256"}
     *
     * @return Map of stat name → stat value as strings.
     * @throws CacheClientException if the server returns an error response.
     * @throws IOException if the network connection fails.
     */
    public Map<String, String> stats() throws IOException {
        String response = sendCommand("STATS");
        return parseStatsResponse(response);
    }

    // -------------------------------------------------------------------------
    // State accessors
    // -------------------------------------------------------------------------

    /**
     * Returns true if this client has an active connection to the server.
     * A client may become disconnected if the server closes the connection
     * (e.g., server restart, max connections exceeded, idle timeout).
     *
     * @return true if connected.
     */
    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed()
                && socket.isConnected() && !socket.isInputShutdown();
    }

    /** @return The server hostname this client is connected to. */
    public String getHost() { return host; }

    /** @return The server port this client is connected to. */
    public int getPort() { return port; }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Sends a raw command string to the server and reads the response.
     *
     * Protocol flow:
     *   1. writer.println(command)  →  sends "COMMAND\n" to the server
     *   2. reader.readLine()        ←  receives "+response\n" from the server
     *   3. Parse the response prefix (+ or -)
     *   4. Return the value (stripping the prefix) or throw on error
     *
     * ERROR HANDLING:
     *   If the server returns "-ERR key not found", we throw CacheClientException
     *   for errors that aren't "key not found" (those return null instead).
     *   "Key not found" is a normal cache miss, not an error — the caller
     *   expects null for misses, not an exception.
     *
     * @param command The full command string (e.g., "GET foo", "PUT k v 60").
     * @return The response value (without the + prefix), or null for cache misses.
     * @throws IOException if the connection fails or read times out.
     * @throws CacheClientException if the server returns a non-miss error.
     */
    private String sendCommand(String command) throws IOException {
        ensureConnected();

        // Send the command. println() appends \n which LineBasedFrameDecoder needs.
        writer.println(command);

        // Read exactly one line of response.
        // readLine() blocks here until the server sends \n or timeout elapses.
        String response;
        try {
            response = reader.readLine();
        } catch (SocketTimeoutException e) {
            // Server didn't respond within the timeout window.
            // Mark connection as dead — the pool will discard it.
            connected = false;
            throw new IOException("Server did not respond within " + readTimeoutMs + "ms", e);
        }

        // null means the server closed the connection.
        if (response == null) {
            connected = false;
            throw new IOException("Server closed the connection unexpectedly");
        }

        // Empty response is a protocol error — should never happen with correct server.
        if (response.isEmpty()) {
            throw new IOException("Received empty response from server");
        }

        // Parse prefix
        char prefix = response.charAt(0);
        String body = response.length() > 1 ? response.substring(1) : "";

        if (prefix == SUCCESS_PREFIX) {
            // +OK, +PONG, +Alice, etc.
            // For PING specifically: response is "+PONG", we return "PONG"
            // For GET of missing key... actually the server sends -ERR, handled below.
            // Empty body means +OK — return null (caller doesn't need "OK" string)
            return body.isEmpty() ? null : body;
        }

        if (prefix == ERROR_PREFIX) {
            // -ERR key not found → treat as cache miss, return null
            if (body.contains("key not found")) {
                return null;
            }
            // Any other error → throw so the caller knows something went wrong
            throw new CacheClientException(body);
        }

        // Unexpected prefix — protocol violation
        throw new IOException("Unexpected response from server: " + response);
    }

    /**
     * Parses the STATS response line into a Map.
     *
     * Input:  "hits:142 misses:31 evictions:8 size:256"
     * Output: {"hits":"142", "misses":"31", "evictions":"8", "size":"256"}
     *
     * @param statsLine The stats response body (after stripping the + prefix).
     * @return Map of stat name to value string.
     */
    private Map<String, String> parseStatsResponse(String statsLine) {
        Map<String, String> stats = new HashMap<>();
        if (statsLine == null || statsLine.isEmpty()) {
            return stats;
        }

        // Each stat is "key:value", stats are space-separated
        String[] pairs = statsLine.split("\\s+");
        for (String pair : pairs) {
            int colonIdx = pair.indexOf(':');
            if (colonIdx > 0 && colonIdx < pair.length() - 1) {
                String statName  = pair.substring(0, colonIdx);
                String statValue = pair.substring(colonIdx + 1);
                stats.put(statName, statValue);
            }
        }
        return stats;
    }

    /**
     * Verifies that this client is still connected before sending a command.
     * Throws IOException if disconnected — callers should handle reconnection
     * or return the client to the pool (which will discard and replace it).
     *
     * @throws IOException if not connected.
     */
    private void ensureConnected() throws IOException {
        if (!isConnected()) {
            throw new IOException(
                    "CacheClient is not connected to " + host + ":" + port +
                            ". Call connect() to reconnect."
            );
        }
    }

    /**
     * Validates that a key is non-null and non-empty.
     * Keys with spaces would break the wire protocol (space is the delimiter).
     *
     * @param key The key to validate.
     * @throws IllegalArgumentException if the key is invalid.
     */
    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (key.contains(" ")) {
            throw new IllegalArgumentException(
                    "Key cannot contain spaces (wire protocol uses space as delimiter): '" + key + "'"
            );
        }
        if (key.contains("\n") || key.contains("\r")) {
            throw new IllegalArgumentException("Key cannot contain newline characters");
        }
    }

    /**
     * Validates that a value is non-null and non-empty.
     * Values can contain spaces but not newlines (newline terminates the command).
     *
     * @param value The value to validate.
     * @throws IllegalArgumentException if the value is invalid.
     */
    private void validateValue(String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Value cannot be null or empty");
        }
        if (value.contains("\n") || value.contains("\r")) {
            throw new IllegalArgumentException("Value cannot contain newline characters");
        }
    }

    // -------------------------------------------------------------------------
    // Inner class: CacheClientException
    // -------------------------------------------------------------------------

    /**
     * Thrown when the server returns an error response (-ERR ...).
     * Distinct from IOException (network failure) — this means the server
     * received and processed the command, but the command itself was invalid.
     *
     * Example: sending "PUT" with no arguments returns "-ERR wrong number of arguments"
     * That's a CacheClientException, not an IOException.
     */
    public static class CacheClientException extends IOException {
        public CacheClientException(String message) {
            super("Server error: " + message);
        }
    }
}