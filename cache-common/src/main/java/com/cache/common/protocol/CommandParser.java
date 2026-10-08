package com.cache.common.protocol;

import com.cache.common.protocol.Command.ParsedCommand;
import com.cache.common.protocol.Command.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses one line of the text protocol into a {@link ParsedCommand}.
 *
 * <p>Tokens are separated by whitespace and verbs are case-insensitive. The
 * value of a {@code PUT} is everything between the key and the end of the
 * line, with inner spacing preserved. If that text has more than one token
 * and the last one is a non-negative integer, the integer is taken as the
 * TTL in seconds. To store a value that itself ends in a number, add an
 * explicit TTL of 0: {@code PUT k order 42 0} stores {@code "order 42"} with
 * no expiry.
 *
 * <p>Instances are stateless and thread-safe.
 */
public class CommandParser {

    /**
     * Parses a line.
     *
     * @param rawLine the line, without its terminator
     * @return the parsed command
     * @throws ProtocolException if the line is empty, the verb is unknown or
     *                           the arguments are wrong
     */
    public ParsedCommand parse(String rawLine) throws ProtocolException {
        List<int[]> spans = tokenize(rawLine);
        if (spans.isEmpty()) {
            throw new ProtocolException(ProtocolException.ErrorCode.EMPTY_COMMAND, "empty command", rawLine);
        }

        String verb = token(rawLine, spans, 0);
        Type type = Type.fromVerb(verb);
        if (type == null) {
            throw new ProtocolException(ProtocolException.ErrorCode.UNKNOWN_COMMAND,
                    "unknown command '" + verb + "'", rawLine);
        }

        int argc = spans.size() - 1;
        if (argc < type.getMinArgs()) {
            throw new ProtocolException(ProtocolException.ErrorCode.MISSING_ARGUMENT,
                    "wrong number of arguments for " + type + ": expected " + type.describeArity()
                            + ", got " + argc, rawLine);
        }
        if (argc > type.getMaxArgs()) {
            throw new ProtocolException(ProtocolException.ErrorCode.TOO_MANY_ARGS,
                    "wrong number of arguments for " + type + ": expected " + type.describeArity()
                            + ", got " + argc, rawLine);
        }

        switch (type) {
            case PUT:
                return parsePut(rawLine, spans);
            case EXPIRE:
                return new ParsedCommand(Type.EXPIRE, token(rawLine, spans, 1), null,
                        parseTtl(token(rawLine, spans, 2), rawLine));
            case TTL:
                // "TTL key seconds" predates EXPIRE and is kept for existing clients.
                return argc == 2
                        ? new ParsedCommand(Type.EXPIRE, token(rawLine, spans, 1), null,
                                parseTtl(token(rawLine, spans, 2), rawLine))
                        : new ParsedCommand(Type.TTL, token(rawLine, spans, 1));
            default:
                return argc == 0 ? new ParsedCommand(type) : new ParsedCommand(type, token(rawLine, spans, 1));
        }
    }

    private ParsedCommand parsePut(String line, List<int[]> spans) {
        String key = token(line, spans, 1);
        int valueStart = spans.get(2)[0];
        int valueEnd = spans.get(spans.size() - 1)[1];
        long ttl = 0;

        if (spans.size() > 3) {
            String last = token(line, spans, spans.size() - 1);
            if (isNonNegativeInteger(last)) {
                ttl = Long.parseLong(last);
                valueEnd = spans.get(spans.size() - 2)[1];
            }
        }
        return new ParsedCommand(Type.PUT, key, line.substring(valueStart, valueEnd), ttl);
    }

    private static long parseTtl(String token, String rawLine) throws ProtocolException {
        if (!isNonNegativeInteger(token)) {
            throw new ProtocolException(ProtocolException.ErrorCode.INVALID_TTL,
                    "TTL must be a non-negative integer, got '" + token + "'", rawLine);
        }
        return Long.parseLong(token);
    }

    private static boolean isNonNegativeInteger(String s) {
        if (s.isEmpty() || s.length() > 18) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** Returns [start, end) offsets of each whitespace-separated token. */
    private static List<int[]> tokenize(String line) {
        List<int[]> spans = new ArrayList<>();
        int i = 0;
        int n = line.length();
        while (i < n) {
            while (i < n && Character.isWhitespace(line.charAt(i))) {
                i++;
            }
            int start = i;
            while (i < n && !Character.isWhitespace(line.charAt(i))) {
                i++;
            }
            if (i > start) {
                spans.add(new int[]{start, i});
            }
        }
        return spans;
    }

    private static String token(String line, List<int[]> spans, int index) {
        int[] span = spans.get(index);
        return line.substring(span[0], span[1]);
    }
}
