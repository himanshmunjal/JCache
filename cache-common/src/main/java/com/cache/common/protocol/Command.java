package com.cache.common.protocol;

/**
 * Command defines the set of operations the cache server understands,
 * and ParsedCommand is the structured result of parsing a raw client string.
 *
 * TWO THINGS IN ONE FILE — WHY?
 * Command (enum) and ParsedCommand (inner class) are tightly coupled:
 * a ParsedCommand always contains a Command type. Keeping them in one file
 * avoids a proliferation of tiny files and makes the relationship explicit.
 * This is a common pattern in protocol implementations (e.g., HTTP method + request).
 *
 * COMMAND ENUM:
 * Each enum constant represents one verb the client can send.
 * The enum stores the expected argument count range (min/max) so
 * CommandParser can validate argument counts without per-command switch statements.
 *
 * Example valid commands (full spec in docs/WireProtocol.md):
 *   PING                  → minArgs=0, maxArgs=0
 *   GET key               → minArgs=1, maxArgs=1
 *   PUT key value         → minArgs=2, maxArgs=3  (TTL is optional 3rd arg)
 *   DELETE key            → minArgs=1, maxArgs=1
 *   STATS                 → minArgs=0, maxArgs=0
 *   FLUSH                 → minArgs=0, maxArgs=0
 *
 * PARSEDCOMMAND INNER CLASS:
 * Carries the parsed result: which command + what arguments.
 * Immutable — set once by CommandParser, never modified by CacheServerHandler.
 *
 * NAMING — why "ParsedCommand" not "Request"?
 * "Request" implies HTTP semantics. "ParsedCommand" is explicit:
 * this object IS a command THAT HAS BEEN parsed. The name describes
 * both what it is and how it was created.
 */
public class Command {

    // =========================================================================
    // Command enum
    // =========================================================================

    /**
     * The set of commands the cache server recognizes.
     *
     * Each constant stores:
     *   minArgs — minimum number of arguments required after the verb
     *   maxArgs — maximum number of arguments accepted after the verb
     *
     * CommandParser uses these to validate argument counts without
     * needing a separate validation method per command.
     *
     * DESIGN NOTE — FLUSH requires confirmation in many real systems.
     * We keep it simple here (no confirmation required) since this is
     * a developer tool, not a production Redis deployment.
     */
    public enum Type {

        /**
         * PING — health check. Server responds with PONG.
         * Syntax: PING
         * Use case: connection health checks, latency measurement.
         */
        PING(0, 0),

        /**
         * GET — retrieve a value by key.
         * Syntax: GET key
         * Response: +value  or  -ERR key not found
         */
        GET(1, 1),

        /**
         * PUT — store a key-value pair, optionally with TTL.
         * Syntax: PUT key value [ttlSeconds]
         * Response: +OK
         *
         * minArgs=2 (key + value required)
         * maxArgs=3 (optional TTL as third argument)
         * TTL of 0 or omitted = no expiry.
         */
        PUT(2, 3),

        /**
         * DELETE — remove a key from the cache.
         * Syntax: DELETE key
         * Response: +OK (even if key didn't exist — idempotent)
         *
         * WHY IDEMPOTENT?
         * Clients retry on network failures. If DELETE succeeds but
         * the response is lost, the client retries. An idempotent DELETE
         * is safe to retry — the key is gone either way.
         */
        DELETE(1, 1),

        /**
         * STATS — return cache statistics.
         * Syntax: STATS
         * Response: +hits:N misses:N evictions:N size:N hitRate:N.NN
         *
         * Numbers come from CacheMetricsCollector in cache-core.
         */
        STATS(0, 0),

        /**
         * FLUSH — remove ALL entries from the cache.
         * Syntax: FLUSH
         * Response: +OK
         *
         * Warning: destructive operation. No confirmation required.
         * Only included for dev/test convenience.
         */
        FLUSH(0, 0);

        // ------------------------------------------------------------------
        // Fields
        // ------------------------------------------------------------------

        /** Minimum number of arguments this command requires (not counting the verb). */
        private final int minArgs;

        /** Maximum number of arguments this command accepts (not counting the verb). */
        private final int maxArgs;

        // ------------------------------------------------------------------
        // Constructor
        // ------------------------------------------------------------------

        Type(int minArgs, int maxArgs) {
            this.minArgs = minArgs;
            this.maxArgs = maxArgs;
        }

        // ------------------------------------------------------------------
        // Getters
        // ------------------------------------------------------------------

        /** @return Minimum argument count for this command. */
        public int getMinArgs() { return minArgs; }

        /** @return Maximum argument count for this command. */
        public int getMaxArgs() { return maxArgs; }

        /**
         * Returns a human-readable argument count description for error messages.
         * Examples:
         *   GET  → "exactly 1 argument(s)"
         *   PUT  → "2 to 3 argument(s)"
         *   PING → "no arguments"
         *
         * @return A descriptive string for error messages.
         */
        public String argCountDescription() {
            if (minArgs == 0 && maxArgs == 0) return "no arguments";
            if (minArgs == maxArgs)            return "exactly " + minArgs + " argument(s)";
            return minArgs + " to " + maxArgs + " argument(s)";
        }
    }

    // =========================================================================
    // ParsedCommand inner class
    // =========================================================================

    /**
     * ParsedCommand is the structured result of parsing a raw client command string.
     *
     * Produced by: CommandParser.parse(String rawLine)
     * Consumed by: CacheServerHandler.channelRead0()
     *
     * IMMUTABILITY:
     * All fields are final. ParsedCommand is created once by the parser
     * and read once by the handler. No synchronization needed.
     *
     * NULL HANDLING:
     * key and value are null for commands that don't use them (PING, STATS, FLUSH).
     * ttlSeconds is 0 for commands without TTL or when TTL is not specified.
     * CacheServerHandler must check type before accessing key/value.
     */
    public static final class ParsedCommand {

        /** Which command this is. Never null. */
        private final Type type;

        /**
         * The cache key for GET, PUT, DELETE commands.
         * Null for PING, STATS, FLUSH.
         */
        private final String key;

        /**
         * The value to store for PUT commands.
         * Null for GET, DELETE, PING, STATS, FLUSH.
         */
        private final String value;

        /**
         * Time-to-live in seconds for PUT commands.
         * 0 means no expiry.
         * Always 0 for non-PUT commands.
         */
        private final long ttlSeconds;

        // ------------------------------------------------------------------
        // Constructors — one per command shape
        // ------------------------------------------------------------------

        /**
         * Constructor for commands with no arguments: PING, STATS, FLUSH.
         *
         * @param type Command type.
         */
        public ParsedCommand(Type type) {
            this(type, null, null, 0L);
        }

        /**
         * Constructor for single-key commands: GET, DELETE.
         *
         * @param type Command type.
         * @param key  The cache key.
         */
        public ParsedCommand(Type type, String key) {
            this(type, key, null, 0L);
        }

        /**
         * Constructor for PUT without TTL.
         *
         * @param type  Command type (PUT).
         * @param key   The cache key.
         * @param value The value to store.
         */
        public ParsedCommand(Type type, String key, String value) {
            this(type, key, value, 0L);
        }

        /**
         * Full constructor for PUT with TTL.
         *
         * @param type       Command type (PUT).
         * @param key        The cache key.
         * @param value      The value to store.
         * @param ttlSeconds Time-to-live in seconds. 0 means no expiry.
         */
        public ParsedCommand(Type type, String key, String value, long ttlSeconds) {
            this.type       = type;
            this.key        = key;
            this.value      = value;
            this.ttlSeconds = ttlSeconds;
        }

        // ------------------------------------------------------------------
        // Getters
        // ------------------------------------------------------------------

        /** @return The command type. Never null. */
        public Type getType() { return type; }

        /**
         * Returns the cache key.
         *
         * @return Key string, or null for keyless commands (PING, STATS, FLUSH).
         */
        public String getKey() { return key; }

        /**
         * Returns the value for PUT commands.
         *
         * @return Value string, or null for non-PUT commands.
         */
        public String getValue() { return value; }

        /**
         * Returns the TTL in seconds for PUT commands.
         * 0 means no expiry — key lives until evicted by policy.
         *
         * @return TTL in seconds, or 0 if not applicable.
         */
        public long getTtlSeconds() { return ttlSeconds; }

        // ------------------------------------------------------------------
        // Convenience predicates
        // ------------------------------------------------------------------

        /**
         * Returns true if this command has a TTL set (PUT with explicit TTL).
         *
         * @return true if ttlSeconds > 0.
         */
        public boolean hasTTL() {
            return ttlSeconds > 0;
        }

        // ------------------------------------------------------------------
        // toString — for logging and debugging
        // ------------------------------------------------------------------

        /**
         * Returns a log-safe string representation.
         * VALUE IS TRUNCATED to 32 chars to prevent log bloat from large values.
         * Key is shown in full (keys should always be short).
         *
         * @return String representation for logging.
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ParsedCommand{type=").append(type);
            if (key   != null) sb.append(", key='").append(key).append("'");
            if (value != null) {
                // Truncate long values in logs
                String displayValue = value.length() > 32
                        ? value.substring(0, 32) + "..."
                        : value;
                sb.append(", value='").append(displayValue).append("'");
            }
            if (ttlSeconds > 0) sb.append(", ttl=").append(ttlSeconds).append("s");
            sb.append("}");
            return sb.toString();
        }
    }

    // =========================================================================
    // Private constructor — this class is a namespace, not instantiated
    // =========================================================================

    /**
     * This class is a namespace container for Command.Type and ParsedCommand.
     * It should never be instantiated directly.
     */
    private Command() {
        throw new UnsupportedOperationException("Command is a namespace class");
    }
}