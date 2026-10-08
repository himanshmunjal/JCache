package com.cache.common.protocol;

/** Thrown by {@link CommandParser} for a line that is not a valid command. */
public class ProtocolException extends Exception {

    /** What was wrong with the line. */
    public enum ErrorCode {
        /** The verb is not a known command. */
        UNKNOWN_COMMAND,
        /** Fewer arguments than the command needs. */
        MISSING_ARGUMENT,
        /** More arguments than the command accepts. */
        TOO_MANY_ARGS,
        /** A TTL argument is not a non-negative integer. */
        INVALID_TTL,
        /** The line was blank. */
        EMPTY_COMMAND
    }

    private final ErrorCode errorCode;
    private final String rawInput;

    /**
     * Creates the exception.
     *
     * @param errorCode what was wrong
     * @param message   description suitable for sending to the client
     * @param rawInput  the offending line
     */
    public ProtocolException(ErrorCode errorCode, String message, String rawInput) {
        super(message);
        this.errorCode = errorCode;
        this.rawInput = rawInput;
    }

    /** @return what was wrong */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /** @return the offending line */
    public String getRawInput() {
        return rawInput;
    }
}
