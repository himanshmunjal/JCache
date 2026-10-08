package com.cache.common.protocol;

import java.util.Locale;
import java.util.Map;

/** Commands of the JCache text protocol and their parsed form. */
public final class Command {

    private Command() {
    }

    /** Command verbs, with the number of arguments each one accepts. */
    public enum Type {
        /** {@code PING}: liveness check. */
        PING(0, 0),
        /** {@code GET key}. */
        GET(1, 1),
        /** {@code PUT key value [ttlSeconds]}; the value may contain spaces. */
        PUT(2, Integer.MAX_VALUE),
        /** {@code DELETE key}. */
        DELETE(1, 1),
        /** {@code EXPIRE key seconds}: set a new TTL on an existing key. */
        EXPIRE(2, 2),
        /** {@code TTL key}: remaining TTL in seconds. {@code TTL key seconds} is accepted as EXPIRE. */
        TTL(1, 2),
        /** {@code PERSIST key}: remove a key's TTL. */
        PERSIST(1, 1),
        /** {@code STATS}: server metrics. */
        STATS(0, 0),
        /** {@code FLUSH}: remove every key. */
        FLUSH(0, 0),
        /** {@code QUIT}: close the connection. */
        QUIT(0, 0);

        private static final Map<String, Type> ALIASES = Map.of(
                "SET", PUT,
                "DEL", DELETE,
                "EXIT", QUIT);

        private final int minArgs;
        private final int maxArgs;

        Type(int minArgs, int maxArgs) {
            this.minArgs = minArgs;
            this.maxArgs = maxArgs;
        }

        /** @return the minimum number of arguments */
        public int getMinArgs() {
            return minArgs;
        }

        /** @return the maximum number of arguments */
        public int getMaxArgs() {
            return maxArgs;
        }

        /**
         * Looks up a verb or one of its aliases, ignoring case.
         *
         * @param verb the verb as typed by the client
         * @return the command type, or {@code null} if the verb is unknown
         */
        public static Type fromVerb(String verb) {
            String upper = verb.toUpperCase(Locale.ROOT);
            Type alias = ALIASES.get(upper);
            if (alias != null) {
                return alias;
            }
            for (Type t : values()) {
                if (t.name().equals(upper)) {
                    return t;
                }
            }
            return null;
        }

        String describeArity() {
            if (maxArgs == 0) {
                return "no arguments";
            }
            if (minArgs == maxArgs) {
                return minArgs == 1 ? "1 argument" : minArgs + " arguments";
            }
            if (maxArgs == Integer.MAX_VALUE) {
                return "at least " + minArgs + " arguments";
            }
            return minArgs + " to " + maxArgs + " arguments";
        }
    }

    /** A validated command, ready to execute. */
    public static final class ParsedCommand {
        private final Type type;
        private final String key;
        private final String value;
        private final long ttlSeconds;

        /**
         * Creates a command with no arguments.
         *
         * @param type the command
         */
        public ParsedCommand(Type type) {
            this(type, null, null, 0L);
        }

        /**
         * Creates a command that takes only a key.
         *
         * @param type the command
         * @param key  the key
         */
        public ParsedCommand(Type type, String key) {
            this(type, key, null, 0L);
        }

        /**
         * Creates a command.
         *
         * @param type       the command
         * @param key        the key, or {@code null}
         * @param value      the value, or {@code null}
         * @param ttlSeconds TTL in seconds, 0 for none
         */
        public ParsedCommand(Type type, String key, String value, long ttlSeconds) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.ttlSeconds = ttlSeconds;
        }

        /** @return the command */
        public Type getType() {
            return type;
        }

        /** @return the key, or {@code null} */
        public String getKey() {
            return key;
        }

        /** @return the value, or {@code null} */
        public String getValue() {
            return value;
        }

        /** @return the TTL in seconds, 0 if none was given */
        public long getTtlSeconds() {
            return ttlSeconds;
        }

        /** @return whether a positive TTL was given */
        public boolean hasTTL() {
            return ttlSeconds > 0;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ParsedCommand{type=").append(type);
            if (key != null) {
                sb.append(", key='").append(key).append('\'');
            }
            if (value != null) {
                String shown = value.length() > 32 ? value.substring(0, 32) + "..." : value;
                sb.append(", value='").append(shown).append('\'');
            }
            if (ttlSeconds > 0) {
                sb.append(", ttl=").append(ttlSeconds).append('s');
            }
            return sb.append('}').toString();
        }
    }
}
