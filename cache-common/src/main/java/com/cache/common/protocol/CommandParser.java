package com.cache.common.protocol;

import com.cache.common.protocol.Command.ParsedCommand;
import com.cache.common.protocol.Command.Type;

/**
 * CommandParser converts a raw text line received from a TCP client
 * into a structured ParsedCommand object.
 *
 * RESPONSIBILITY:
 * This class has exactly one job: text → ParsedCommand.
 * It does NOT touch the cache. It does NOT write to the network.
 * It ONLY validates and structures the input.
 *
 * This separation means:
 *   - CommandParser is fully testable without a running server or cache.
 *   - CacheServerHandler only deals with already-valid ParsedCommand objects.
 *   - Protocol changes happen in one place (here), not scattered in the handler.
 *
 * PARSING STRATEGY — Split on whitespace, validate structure:
 *   1. Trim the raw line.
 *   2. If empty → throw EMPTY_COMMAND (caller should ignore and wait for next line).
 *   3. Split on one-or-more whitespace characters (\s+).
 *   4. First token is the command verb → look up in Command.Type enum.
 *   5. Remaining tokens are arguments → validate count against Type.minArgs/maxArgs.
 *   6. For PUT: parse optional TTL argument as positive long.
 *   7. Return ParsedCommand with structured fields.
 *
 * WHITESPACE HANDLING:
 *   "GET   foo"   → valid, treated as "GET foo"
 *   "  PUT k v"   → valid, leading whitespace stripped
 *   "get foo"     → valid, command verbs are case-insensitive
 *   "\t\n"        → EMPTY_COMMAND
 *
 * CASE INSENSITIVITY:
 *   Command verbs are uppercased before enum lookup.
 *   Keys and values are case-sensitive (treated as opaque strings).
 *   "get Foo" → GET key="Foo" (key preserved as-is)
 *   "GET Foo" → GET key="Foo" (same result)
 *
 * VALUE WITH SPACES:
 *   Our protocol uses space as delimiter, so values cannot contain spaces.
 *   "PUT key hello world" → TOO_MANY_ARGS (not "key=key, value=hello world")
 *   This is the same limitation as Redis's inline command format.
 *   For values with spaces, clients should use Base64 encoding or quotes.
 *   (Quote handling is a potential Day 25 stretch goal.)
 *
 * THREAD SAFETY:
 *   CommandParser has no mutable state — all fields are final (none exist).
 *   The parse() method is stateless and safe to call from multiple threads.
 *   CacheServerHandler can share a single CommandParser instance, or create
 *   one per channel — either is correct.
 */
public class CommandParser {

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Parses a raw text line into a ParsedCommand.
     *
     * This is the only public method. All parsing logic flows through here.
     *
     * FLOW:
     *   parse("PUT name Alice 60")
     *     → trim        → "PUT name Alice 60"
     *     → split       → ["PUT", "name", "Alice", "60"]
     *     → verb lookup → Type.PUT
     *     → arg count   → 3 args, PUT accepts 2-3 ✓
     *     → TTL parse   → 60L ✓
     *     → return ParsedCommand{type=PUT, key="name", value="Alice", ttl=60}
     *
     * @param rawLine The raw string received from the client over TCP.
     *                May contain leading/trailing whitespace or \r from CRLF line endings.
     *                Must not be null (Netty's StringDecoder never passes null).
     *
     * @return A fully validated ParsedCommand ready for dispatch.
     *
     * @throws ProtocolException if the line is empty, the verb is unknown,
     *                           the argument count is wrong, or the TTL is invalid.
     */
    public ParsedCommand parse(String rawLine) throws ProtocolException {
        // ---- Step 1: Trim and reject empty input ----------------------------
        // Trim handles: leading spaces, trailing \r (from \r\n line endings),
        // and trailing spaces. LineBasedFrameDecoder strips \n but may leave \r.
        String trimmed = rawLine.trim();

        if (trimmed.isEmpty()) {
            throw new ProtocolException(
                    ProtocolException.ErrorCode.EMPTY_COMMAND,
                    "Empty command — send a valid command or PING",
                    rawLine
            );
        }

        // ---- Step 2: Split on whitespace ------------------------------------
        // "\\s+" matches one or more whitespace characters (space, tab, etc.)
        // This tolerates "GET  key" (double space) and "PUT\tkey\tval" (tabs).
        String[] tokens = trimmed.split("\\s+");

        // ---- Step 3: Extract and validate the command verb ------------------
        String verbString = tokens[0].toUpperCase();
        Type commandType  = lookupCommandType(verbString, rawLine);

        // ---- Step 4: Count and validate arguments ---------------------------
        // args = everything after the verb
        int argCount = tokens.length - 1;
        validateArgCount(commandType, argCount, rawLine);

        // ---- Step 5: Build the appropriate ParsedCommand --------------------
        return buildParsedCommand(commandType, tokens, rawLine);
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    /**
     * Looks up the Command.Type for a verb string.
     * Uses Enum.valueOf() which throws IllegalArgumentException for unknown names.
     * We catch that and convert to a ProtocolException with a helpful message.
     *
     * @param verb    The uppercased command verb from the client.
     * @param rawLine Original raw input for error context.
     * @return The matching Command.Type.
     * @throws ProtocolException if the verb is not recognized.
     */
    private Type lookupCommandType(String verb, String rawLine) throws ProtocolException {
        try {
            return Type.valueOf(verb);
        } catch (IllegalArgumentException e) {
            // Enum.valueOf threw — verb is not a known command.
            // Build a helpful error listing valid commands.
            throw new ProtocolException(
                    ProtocolException.ErrorCode.UNKNOWN_COMMAND,
                    "Unknown command '" + verb + "'. Valid commands: " + validCommandList(),
                    rawLine
            );
        }
    }

    /**
     * Validates that the number of arguments matches the command's requirements.
     * Uses the minArgs/maxArgs fields on Command.Type — no per-command logic here.
     *
     * @param type     The resolved command type.
     * @param argCount Number of arguments provided (not counting the verb).
     * @param rawLine  Original raw input for error context.
     * @throws ProtocolException if argCount is outside [minArgs, maxArgs].
     */
    private void validateArgCount(Type type, int argCount, String rawLine)
            throws ProtocolException {

        if (argCount < type.getMinArgs()) {
            throw new ProtocolException(
                    ProtocolException.ErrorCode.MISSING_ARGUMENT,
                    String.format("'%s' requires %s, but got %d",
                            type.name(), type.argCountDescription(), argCount),
                    rawLine
            );
        }

        if (argCount > type.getMaxArgs()) {
            throw new ProtocolException(
                    ProtocolException.ErrorCode.TOO_MANY_ARGS,
                    String.format("'%s' accepts %s, but got %d",
                            type.name(), type.argCountDescription(), argCount),
                    rawLine
            );
        }
    }

    /**
     * Builds a ParsedCommand from validated tokens.
     * By the time this method is called, we know:
     *   - commandType is valid
     *   - argCount is within [minArgs, maxArgs]
     *
     * So we don't repeat validation here — just extract and structure.
     *
     * @param type    The resolved command type.
     * @param tokens  Full token array [verb, arg1, arg2, ...].
     * @param rawLine Original raw input for error context (TTL parse errors).
     * @return A fully populated ParsedCommand.
     * @throws ProtocolException if TTL argument cannot be parsed.
     */
    private ParsedCommand buildParsedCommand(Type type, String[] tokens, String rawLine)
            throws ProtocolException {

        switch (type) {

            case PING:
            case STATS:
            case FLUSH:
                // No arguments — return simple ParsedCommand with just the type.
                return new ParsedCommand(type);

            case GET:
            case DELETE:
                // Exactly one argument: the key.
                // tokens[0] = verb, tokens[1] = key
                return new ParsedCommand(type, tokens[1]);

            case PUT:
                // tokens[0] = "PUT"
                // tokens[1] = key
                // tokens[2] = value
                // tokens[3] = ttlSeconds (optional)
                String key   = tokens[1];
                String value = tokens[2];
                long   ttl   = 0L; // default: no expiry

                if (tokens.length == 4) {
                    // TTL argument was provided — parse and validate it.
                    ttl = parseTTL(tokens[3], rawLine);
                }

                return new ParsedCommand(type, key, value, ttl);

            default:
                // This should never happen — all Type values are handled above.
                // If someone adds a new Type without updating this switch, this fires.
                throw new IllegalStateException(
                        "Unhandled command type in buildParsedCommand: " + type
                );
        }
    }

    /**
     * Parses a TTL string argument into a non-negative long.
     *
     * VALID TTL values:
     *   "0"    → no expiry (key lives until evicted by policy)
     *   "60"   → expires in 60 seconds
     *   "3600" → expires in 1 hour
     *
     * INVALID TTL values:
     *   "-1"    → negative, throws INVALID_TTL
     *   "abc"   → not a number, throws INVALID_TTL
     *   "1.5"   → not an integer, throws INVALID_TTL
     *   "99999999999999999999" → overflows long, throws INVALID_TTL
     *
     * @param ttlString The raw TTL string from the client (tokens[3]).
     * @param rawLine   Original full command line for error context.
     * @return The parsed TTL as a non-negative long.
     * @throws ProtocolException if ttlString is not a valid non-negative integer.
     */
    private long parseTTL(String ttlString, String rawLine) throws ProtocolException {
        long ttl;
        try {
            ttl = Long.parseLong(ttlString);
        } catch (NumberFormatException e) {
            throw new ProtocolException(
                    ProtocolException.ErrorCode.INVALID_TTL,
                    "TTL must be a non-negative integer (seconds), got: '" + ttlString + "'",
                    rawLine
            );
        }

        if (ttl < 0) {
            throw new ProtocolException(
                    ProtocolException.ErrorCode.INVALID_TTL,
                    "TTL must be non-negative, got: " + ttl,
                    rawLine
            );
        }

        return ttl;
    }

    /**
     * Returns a comma-separated list of all valid command verbs.
     * Used in UNKNOWN_COMMAND error messages to help clients self-correct.
     *
     * Example: "DELETE, FLUSH, GET, PING, PUT, STATS"
     * (Enum.values() order is declaration order, not alphabetical.
     *  We sort for consistency in error messages.)
     *
     * @return Comma-separated list of valid command names.
     */
    private String validCommandList() {
        StringBuilder sb = new StringBuilder();
        Type[] types = Type.values();
        for (int i = 0; i < types.length; i++) {
            sb.append(types[i].name());
            if (i < types.length - 1) sb.append(", ");
        }
        return sb.toString();
    }

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
    public static class ProtocolException extends Exception {

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
}
