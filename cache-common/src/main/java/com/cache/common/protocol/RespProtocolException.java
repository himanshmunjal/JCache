package com.cache.common.protocol;

/** Thrown by {@link RespDecoder} when the input is not valid RESP. */
public class RespProtocolException extends RuntimeException {

    /**
     * Creates the exception.
     *
     * @param message what was wrong with the input
     */
    public RespProtocolException(String message) {
        super(message);
    }
}
