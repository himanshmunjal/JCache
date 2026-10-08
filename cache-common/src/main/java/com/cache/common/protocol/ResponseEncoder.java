package com.cache.common.protocol;

/**
 * Formats replies of the text protocol. Every reply is one line ending in
 * CRLF: {@code +} followed by the payload for success, {@code -ERR }
 * followed by a message for failure.
 */
public final class ResponseEncoder {

    private static final String CRLF = "\r\n";
    private static final String OK = "+OK" + CRLF;
    private static final String PONG = "+PONG" + CRLF;
    private static final String BYE = "+BYE" + CRLF;
    private static final String NOT_FOUND = "-ERR key not found" + CRLF;

    private ResponseEncoder() {
    }

    /** @return {@code +OK} */
    public static String ok() {
        return OK;
    }

    /** @return {@code +PONG} */
    public static String pong() {
        return PONG;
    }

    /** @return {@code +BYE}, sent before the server closes the connection */
    public static String bye() {
        return BYE;
    }

    /**
     * Formats a successful reply carrying a payload.
     *
     * @param value the payload
     * @return {@code +value}
     */
    public static String value(String value) {
        return "+" + value + CRLF;
    }

    /** @return {@code -ERR key not found} */
    public static String keyNotFound() {
        return NOT_FOUND;
    }

    /**
     * Formats an error reply. Line breaks in the message are replaced so the
     * reply stays on one line.
     *
     * @param message the error message
     * @return {@code -ERR message}
     */
    public static String error(String message) {
        String text = message == null ? "unknown error" : message.replace('\r', ' ').replace('\n', ' ');
        return "-ERR " + text + CRLF;
    }
}
