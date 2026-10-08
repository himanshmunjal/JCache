package com.cache.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Blocking client for a single JCache server, over one TCP connection.
 *
 * <p>Methods are synchronized, so one instance can be shared between threads,
 * but requests are then sent one at a time. For concurrent use prefer a
 * {@link ConnectionPool}.
 *
 * <p>Keys may not contain whitespace. Values may contain spaces but not line
 * breaks, and may not start or end with whitespace because the protocol
 * would trim it.
 */
public class CacheClient implements Closeable {

    private static final int DEFAULT_TIMEOUT_MS = 5_000;
    private static final String NOT_FOUND = "key not found";

    private final String host;
    private final int port;
    private final int timeoutMs;

    private Socket socket;
    private Writer writer;
    private BufferedReader reader;
    private boolean connected;
    private long lastUsedNanos;

    /**
     * Connects with a five-second connect and read timeout.
     *
     * @param host server host
     * @param port server port
     * @throws IOException if the connection fails
     */
    public CacheClient(String host, int port) throws IOException {
        this(host, port, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Connects to the server.
     *
     * @param host      server host
     * @param port      server port
     * @param timeoutMs connect and read timeout in milliseconds; 0 waits forever
     * @throws IOException if the connection fails
     */
    public CacheClient(String host, int port, int timeoutMs) throws IOException {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs cannot be negative");
        }
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
        connect();
    }

    /**
     * Opens the connection if it is not open. Call this to reconnect after
     * an I/O error.
     *
     * @throws IOException if the connection fails
     */
    public synchronized void connect() throws IOException {
        if (connected) {
            return;
        }
        closeQuietly();
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(host, port), timeoutMs);
            s.setSoTimeout(timeoutMs);
            s.setTcpNoDelay(true);
            writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
            reader = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            s.close();
            throw e;
        }
        socket = s;
        connected = true;
        lastUsedNanos = System.nanoTime();
    }

    @Override
    public synchronized void close() {
        if (connected) {
            try {
                send("QUIT");
            } catch (IOException ignored) {
                // Closing anyway.
            }
        }
        closeQuietly();
    }

    /** @return {@code true} if the server answered PING */
    public boolean ping() {
        try {
            return "PONG".equals(send("PING"));
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Reads a value.
     *
     * @param key the key
     * @return the value, or {@code null} if the key is absent
     * @throws IOException on a connection problem or a server error
     */
    public String get(String key) throws IOException {
        validateKey(key);
        return send("GET " + key);
    }

    /**
     * Stores a value. It expires according to the server's default TTL, which
     * is "never" unless the server was configured otherwise.
     *
     * @param key   the key
     * @param value the value
     * @throws IOException on a connection problem or a server error
     */
    public void put(String key, String value) throws IOException {
        put(key, value, 0);
    }

    /**
     * Stores a value with a TTL.
     *
     * @param key        the key
     * @param value      the value
     * @param ttlSeconds time to live in seconds; 0 uses the server's default
     * @throws IOException on a connection problem or a server error
     */
    public void put(String key, String value, long ttlSeconds) throws IOException {
        validateKey(key);
        validateValue(value);
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("TTL cannot be negative: " + ttlSeconds);
        }
        // The TTL is always sent so a value ending in a number is never mistaken for one.
        send("PUT " + key + " " + value + " " + ttlSeconds);
    }

    /**
     * Removes a key. Removing an absent key is not an error.
     *
     * @param key the key
     * @throws IOException on a connection problem or a server error
     */
    public void delete(String key) throws IOException {
        validateKey(key);
        send("DELETE " + key);
    }

    /**
     * Sets a new TTL on an existing key. A TTL of 0 deletes it.
     *
     * @param key        the key
     * @param ttlSeconds the new TTL in seconds
     * @return {@code false} if the key does not exist
     * @throws IOException on a connection problem or a server error
     */
    public boolean expire(String key, long ttlSeconds) throws IOException {
        validateKey(key);
        if (ttlSeconds < 0) {
            throw new IllegalArgumentException("TTL cannot be negative: " + ttlSeconds);
        }
        return send("EXPIRE " + key + " " + ttlSeconds) != null;
    }

    /**
     * Returns the remaining TTL of a key.
     *
     * @param key the key
     * @return seconds left, {@code -1} if the key never expires, or {@code -2}
     *         if it does not exist (the same convention as Redis)
     * @throws IOException on a connection problem or a server error
     */
    public long ttl(String key) throws IOException {
        validateKey(key);
        String reply = send("TTL " + key);
        return reply == null ? -2 : Long.parseLong(reply);
    }

    /**
     * Removes a key's TTL so it never expires.
     *
     * @param key the key
     * @return {@code false} if the key does not exist
     * @throws IOException on a connection problem or a server error
     */
    public boolean persist(String key) throws IOException {
        validateKey(key);
        return send("PERSIST " + key) != null;
    }

    /**
     * Removes every key on the server.
     *
     * @throws IOException on a connection problem or a server error
     */
    public void flush() throws IOException {
        send("FLUSH");
    }

    /**
     * Fetches the server's metrics.
     *
     * @return metric name to value, for example {@code hits -> 42}
     * @throws IOException on a connection problem or a server error
     */
    public Map<String, String> stats() throws IOException {
        Map<String, String> stats = new LinkedHashMap<>();
        String reply = send("STATS");
        if (reply != null) {
            for (String pair : reply.split("\\s+")) {
                int colon = pair.indexOf(':');
                if (colon > 0 && colon < pair.length() - 1) {
                    stats.put(pair.substring(0, colon), pair.substring(colon + 1));
                }
            }
        }
        return stats;
    }

    /** @return whether the connection is open as far as the client knows */
    public synchronized boolean isConnected() {
        return connected && !socket.isClosed();
    }

    /** @return nanoseconds since the connection was last used */
    synchronized long idleNanos() {
        return System.nanoTime() - lastUsedNanos;
    }

    /** @return the server host */
    public String getHost() {
        return host;
    }

    /** @return the server port */
    public int getPort() {
        return port;
    }

    /**
     * Sends one command and reads the one-line reply.
     *
     * @return the payload of a {@code +} reply, or {@code null} for an empty
     *         payload or a "key not found" error
     */
    private synchronized String send(String command) throws IOException {
        if (!connected) {
            throw new IOException("Not connected to " + host + ":" + port + "; call connect() to reconnect");
        }
        String reply;
        try {
            writer.write(command);
            writer.write("\r\n");
            writer.flush();
            reply = reader.readLine();
        } catch (SocketTimeoutException e) {
            // The reply may still arrive later and would be read as the answer to the next request.
            closeQuietly();
            throw new IOException("No reply from " + host + ":" + port + " within " + timeoutMs + " ms", e);
        } catch (IOException e) {
            closeQuietly();
            throw e;
        }
        lastUsedNanos = System.nanoTime();
        if (reply == null) {
            closeQuietly();
            throw new IOException("Server closed the connection");
        }
        if (reply.startsWith("+")) {
            return reply.length() == 1 ? null : reply.substring(1);
        }
        if (reply.startsWith("-")) {
            String message = reply.startsWith("-ERR ") ? reply.substring(5) : reply.substring(1);
            if (message.startsWith(NOT_FOUND)) {
                return null;
            }
            throw new CacheClientException(message);
        }
        throw new IOException("Unexpected reply from server: " + reply);
    }

    private void closeQuietly() {
        connected = false;
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Nothing more can be done.
            }
            socket = null;
        }
    }

    static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        for (int i = 0; i < key.length(); i++) {
            if (Character.isWhitespace(key.charAt(i))) {
                throw new IllegalArgumentException("Key cannot contain whitespace: '" + key + "'");
            }
        }
    }

    static void validateValue(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Value cannot be null or blank");
        }
        if (value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("Value cannot contain line breaks");
        }
        if (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1))) {
            throw new IllegalArgumentException("Value cannot start or end with whitespace");
        }
    }

    /** An error reply from the server, as opposed to a connection problem. */
    public static class CacheClientException extends IOException {

        /**
         * @param message the server's error message
         */
        public CacheClientException(String message) {
            super("Server error: " + message);
        }
    }
}
