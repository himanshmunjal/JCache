package com.cache.server.handler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TokenBucket")
class TokenBucketTest {

    private static final long SECOND = 1_000_000_000L;

    private final AtomicLong now = new AtomicLong();

    @Test
    @DisplayName("A full bucket allows a burst, then rejects")
    void burstThenReject() {
        TokenBucket bucket = new TokenBucket(10, 3, now::get);

        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertFalse(bucket.tryAcquire());
    }

    @Test
    @DisplayName("Tokens come back at the configured rate")
    void refill() {
        TokenBucket bucket = new TokenBucket(10, 1, now::get);
        assertTrue(bucket.tryAcquire());
        assertFalse(bucket.tryAcquire());

        now.addAndGet(SECOND / 20);
        assertFalse(bucket.tryAcquire(), "half a token is not enough");

        now.addAndGet(SECOND / 20);
        assertTrue(bucket.tryAcquire());
        assertFalse(bucket.tryAcquire());
    }

    @Test
    @DisplayName("Refill never exceeds the capacity")
    void capacityCaps() {
        TokenBucket bucket = new TokenBucket(100, 2, now::get);
        now.addAndGet(60 * SECOND);

        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertFalse(bucket.tryAcquire());
    }

    @Test
    @DisplayName("Rate and capacity must be positive")
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(0, 1, now::get));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(1, 0, now::get));
    }
}
