package com.cache.server.handler;

import java.util.function.LongSupplier;

/**
 * Token bucket for one connection. Tokens refill continuously at a fixed rate
 * up to the bucket's capacity; each command takes one.
 *
 * <p>Not thread-safe. Each connection's handler runs on a single I/O thread.
 */
final class TokenBucket {

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    private final double tokensPerNano;
    private final double capacity;
    private final LongSupplier nanoClock;
    private double tokens;
    private long lastRefill;

    /**
     * Creates a full bucket.
     *
     * @param perSecond refill rate, at least 1
     * @param capacity  maximum tokens, at least 1
     * @param nanoClock time source in nanoseconds, usually {@code System::nanoTime}
     */
    TokenBucket(int perSecond, int capacity, LongSupplier nanoClock) {
        if (perSecond < 1 || capacity < 1) {
            throw new IllegalArgumentException("rate and capacity must be >= 1");
        }
        this.tokensPerNano = perSecond / NANOS_PER_SECOND;
        this.capacity = capacity;
        this.nanoClock = nanoClock;
        this.tokens = capacity;
        this.lastRefill = nanoClock.getAsLong();
    }

    /** @return whether a token was taken; {@code false} means the caller is over the limit */
    boolean tryAcquire() {
        long now = nanoClock.getAsLong();
        tokens = Math.min(capacity, tokens + (now - lastRefill) * tokensPerNano);
        lastRefill = now;
        if (tokens < 1) {
            return false;
        }
        tokens -= 1;
        return true;
    }
}
