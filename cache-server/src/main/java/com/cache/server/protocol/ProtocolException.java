package com.cache.server.protocol;

/**
 * ProtocolException is thrown when a client sends a malformed or
 * invalid command that cannot be parsed into a valid ParsedCommand.
 *
 * WHY A CHECKED EXCEPTION?
 * Protocol errors are expected, recoverable events — not programming bugs.
 * A client sending "GET" with no key is not a JVM error; it's a usage error
 * that the server should handle gracefully by sending "-ERR ..." back to the
 * client and continuing to serve other requests.
 *
 * Checked exceptions force CommandParser callers (CacheServerHandler) to
 * explicitly handle the error case — you cannot accidentally ignore it.
 * If we used RuntimeException, a developer could forget the try/catch and
 * the channel would close silently on every bad command.
 *
 * WHAT HAPPENS WHEN THIS IS THROWN:
 *   CommandParser.parse() throws ProtocolException
 *       ↓
 *   CacheServerHandler.channelRead0() catches it
 *       ↓
 *   ResponseEncoder.error(e.getMessage()) formats "-ERR ..." string
 *       ↓
 *   ctx.writeAndFlush() sends error back to client
 *       ↓
 *   Connection stays open — client can send another command
 *
 * The server NEVER closes the connection because of a ProtocolException.
 * Only genuine I/O errors or client disconnects close the channel.
 *
 * ERROR CODE DESIGN:
 * Each ProtocolException carries an ErrorCode enum value.
 * This allows CacheServerHandler to distinguish between:
 *   - UNKNOWN_COMMAND  → client sent "FLIBBERTIGIBBET key"
 *   - MISSING_ARGUMENT → client sent "GET" with no key
 *   - TOO_MANY_ARGS    → client sent "GET key extra_arg"
 *   - INVALID_TTL      → client sent "PUT key val notanumber"
 *   - EMPTY_COMMAND    → client sent an empty line or only whitespace
 *
 * For now, CacheServerHandler uses the message string directly in the
 * error response. The ErrorCode is available for future use (e.g., numeric
 * error codes for machine-readable clients, like HTTP status codes).
 */
public class ProtocolException extends Exception {

    // -------------------------------------------------------------------------
    // Error codes — categorizes the type of protocol violation
    // -------------------------------------------------------------------------

    /**
     * Enumerates the categories of protocol errors.
     * Mirrors HTTP's approach: a numeric/named code plus a human message.
     */
    public enum ErrorCode {

        /** Client sent a command verb that the server doesn't recognize. */
        UNKNOWN_COMMAND,

        /** Client sent a known command but omitted required arguments. */
        MISSING_ARGUMENT,

        /** Client sent more arguments than the command accepts. */
        TOO_MANY_ARGS,

        /**
         * Client sent a TTL value that cannot be parsed as a positive integer.
         * e.g., "PUT key value abc" or "PUT key value -5"
         */
        INVALID_TTL,

        /**
         * Client sent an empty line or a line containing only whitespace.
         * Not a fatal error — server ignores and waits for next command.
         */
        EMPTY_COMMAND,

        /**
         * Client sent a key that violates key format rules.
         * e.g., key contains spaces (which are our argument delimiter).
         * Currently unused but reserved for future key validation.
         */
        INVALID_KEY
    }

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    /** The category of this protocol error. */
    private final ErrorCode errorCode;

    /**
     * The raw input that caused this error.
     * Stored for logging and debugging — never sent back to the client
     * (avoid echoing potentially malicious input in error messages).
     */
    private final String rawInput;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Creates a ProtocolException with an error code, human-readable message,
     * and the raw input that caused the error.
     *
     * @param errorCode  Category of the protocol violation.
     * @param message    Human-readable description. Sent to the client as:
     *                   "-ERR {message}"
     * @param rawInput   The raw string that triggered this exception.
     *                   Used for server-side logging only.
     */
    public ProtocolException(ErrorCode errorCode, String message, String rawInput) {
        super(message);
        this.errorCode = errorCode;
        this.rawInput  = rawInput;
    }

    /**
     * Convenience constructor when raw input is not available or relevant
     * (e.g., EMPTY_COMMAND where there's nothing meaningful to log).
     *
     * @param errorCode Category of the protocol violation.
     * @param message   Human-readable description sent to the client.
     */
    public ProtocolException(ErrorCode errorCode, String message) {
        this(errorCode, message, "");
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    /**
     * Returns the category of this protocol error.
     * Use this in CacheServerHandler to differentiate handling logic
     * if needed (e.g., log UNKNOWN_COMMAND at WARN, EMPTY_COMMAND at DEBUG).
     *
     * @return The ErrorCode for this exception.
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Returns the raw client input that caused this exception.
     * Use for server-side logging. Do NOT echo this in client responses.
     *
     * @return The raw input string, or empty string if not applicable.
     */
    public String getRawInput() {
        return rawInput;
    }

    /**
     * Returns a formatted string suitable for server-side logging.
     * Includes the error code and raw input alongside the message.
     *
     * Example: "ProtocolException[MISSING_ARGUMENT] 'GET' with no key: GET"
     *
     * @return A detailed string for log output.
     */
    public String toLogString() {
        return String.format("ProtocolException[%s] %s | raw='%s'",
                errorCode, getMessage(), rawInput);
    }
}