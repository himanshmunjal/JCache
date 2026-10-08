package com.cache.common.protocol;

/** A RESP reply value. Create instances with the static factory methods. */
public final class RespValue {

    /** RESP reply types supported by {@link RespEncoder}. */
    public enum Type {
        /** {@code +text} */
        SIMPLE_STRING,
        /** {@code -message} */
        ERROR,
        /** {@code :number} */
        INTEGER,
        /** {@code $length} followed by the bytes */
        BULK_STRING,
        /** {@code $-1}, the absent value */
        NULL_BULK
    }

    private static final RespValue NULL = new RespValue(Type.NULL_BULK, null, 0);

    private final Type type;
    private final String stringValue;
    private final long longValue;

    private RespValue(Type type, String stringValue, long longValue) {
        this.type = type;
        this.stringValue = stringValue;
        this.longValue = longValue;
    }

    /**
     * @param value text without CR or LF
     * @return a simple string reply
     */
    public static RespValue simpleString(String value) {
        return new RespValue(Type.SIMPLE_STRING, value, 0);
    }

    /**
     * @param message error text without CR or LF, conventionally starting with {@code ERR}
     * @return an error reply
     */
    public static RespValue error(String message) {
        return new RespValue(Type.ERROR, message, 0);
    }

    /**
     * @param value the number
     * @return an integer reply
     */
    public static RespValue integer(long value) {
        return new RespValue(Type.INTEGER, null, value);
    }

    /**
     * @param value any text; use {@link #nullBulk()} for an absent value
     * @return a bulk string reply
     */
    public static RespValue bulkString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Use RespValue.nullBulk() for an absent value");
        }
        return new RespValue(Type.BULK_STRING, value, 0);
    }

    /** @return the null bulk string reply */
    public static RespValue nullBulk() {
        return NULL;
    }

    /** @return the reply type */
    public Type getType() {
        return type;
    }

    /** @return the text of a string or error reply, otherwise {@code null} */
    public String getStringValue() {
        return stringValue;
    }

    /** @return the number of an integer reply, otherwise 0 */
    public long getLongValue() {
        return longValue;
    }

    @Override
    public String toString() {
        return switch (type) {
            case INTEGER -> "RespValue{INTEGER, " + longValue + "}";
            case NULL_BULK -> "RespValue{NULL_BULK}";
            default -> "RespValue{" + type + ", \"" + stringValue + "\"}";
        };
    }
}
