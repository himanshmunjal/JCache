package com.cache.persistence;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * One line of the append-only file.
 *
 * <p>Keys and values are Base64-encoded so that spaces, newlines and any
 * other bytes survive a round trip:
 * <pre>
 * SET &lt;key&gt; &lt;value&gt; &lt;expiryMs&gt;
 * DEL &lt;key&gt;
 * FLUSH
 * </pre>
 * {@code expiryMs} is an absolute epoch-millis timestamp, or {@code -1}.
 * Files written by versions before 1.0.2 used unencoded {@code PUT} and
 * {@code DELETE} lines; those are still accepted when reading.
 *
 * @param op       what the record does
 * @param key      the key, or {@code null} for a flush
 * @param value    the value, or {@code null} unless {@code op} is SET
 * @param expiryMs absolute expiry time, or {@code -1}
 */
record AofRecord(Op op, String key, String value, long expiryMs) {

    enum Op { SET, DEL, FLUSH }

    private static final Base64.Encoder ENCODER = Base64.getEncoder();
    private static final Base64.Decoder DECODER = Base64.getDecoder();

    static AofRecord set(String key, String value, long expiryMs) {
        return new AofRecord(Op.SET, key, value, expiryMs);
    }

    static AofRecord delete(String key) {
        return new AofRecord(Op.DEL, key, null, -1L);
    }

    static AofRecord flush() {
        return new AofRecord(Op.FLUSH, null, null, -1L);
    }

    String encode() {
        return switch (op) {
            case SET -> "SET " + encode(key) + " " + encode(value) + " " + expiryMs;
            case DEL -> "DEL " + encode(key);
            case FLUSH -> "FLUSH";
        };
    }

    /**
     * Parses one non-blank line.
     *
     * @throws IllegalArgumentException if the line is malformed
     */
    static AofRecord parse(String line) {
        String[] t = line.trim().split(" ");
        switch (t[0]) {
            case "SET":
                requireTokens(t, 4);
                return set(decode(t[1]), decode(t[2]), parseExpiry(t[3]));
            case "DEL":
                requireTokens(t, 2);
                return delete(decode(t[1]));
            case "FLUSH":
                requireTokens(t, 1);
                return flush();
            case "PUT":
                // Legacy: PUT key value expiryMs, value without spaces.
                requireTokens(t, 4);
                return set(t[1], t[2], parseExpiry(t[3]));
            case "DELETE":
                requireTokens(t, 2);
                return delete(t[1]);
            default:
                throw new IllegalArgumentException("Unknown AOF verb '" + t[0] + "'");
        }
    }

    private static void requireTokens(String[] tokens, int expected) {
        if (tokens.length != expected) {
            throw new IllegalArgumentException(
                    tokens[0] + " expects " + (expected - 1) + " arguments, got " + (tokens.length - 1));
        }
    }

    private static long parseExpiry(String token) {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad expiry '" + token + "'");
        }
    }

    private static String encode(String s) {
        return ENCODER.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String s) {
        try {
            return new String(DECODER.decode(s), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Bad Base64 '" + s + "'");
        }
    }
}
