package com.cache.server.protocol;

import com.cache.server.protocol.Command.ParsedCommand;
import com.cache.server.protocol.Command.Type;

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
}