package com.example.jcache;

import com.cache.client.CacheClient.CacheClientException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimitRetryTest {

    private final List<Long> sleeps = new ArrayList<>();
    private final RateLimitRetry retry = new RateLimitRetry(4, 10, sleeps::add);

    @Test
    void retriesRateLimitWithDoublingBackoff() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result = retry.call(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new CacheClientException("rate limit exceeded");
            }
            return "ok";
        });
        assertEquals("ok", result);
        assertEquals(List.of(10L, 20L), sleeps);
        assertEquals(2, retry.retries());
    }

    @Test
    void givesUpAfterMaxAttempts() {
        AtomicInteger calls = new AtomicInteger();
        assertThrows(CacheClientException.class, () -> retry.call(() -> {
            calls.incrementAndGet();
            throw new CacheClientException("rate limit exceeded");
        }));
        assertEquals(4, calls.get());
        assertEquals(List.of(10L, 20L, 40L), sleeps);
    }

    @Test
    void otherErrorsAreNotRetried() {
        CacheClientException serverError = new CacheClientException("unknown command 'FOO'");
        IOException thrown = assertThrows(IOException.class, () -> retry.call(() -> {
            throw serverError;
        }));
        assertSame(serverError, thrown);

        assertThrows(IOException.class, () -> retry.call(() -> {
            throw new IOException("Server closed the connection");
        }));
        assertTrue(sleeps.isEmpty());
    }

    @Test
    void recognisesOnlyTheRateLimitReply() {
        assertTrue(RateLimitRetry.isRateLimited(new CacheClientException("rate limit exceeded")));
        assertFalse(RateLimitRetry.isRateLimited(new CacheClientException("max connections reached")));
        assertFalse(RateLimitRetry.isRateLimited(new IOException("rate limit exceeded")));
    }

    @Test
    void rejectsBadSettings() {
        assertThrows(IllegalArgumentException.class, () -> new RateLimitRetry(0, 10));
        assertThrows(IllegalArgumentException.class, () -> new RateLimitRetry(1, -1));
    }
}
