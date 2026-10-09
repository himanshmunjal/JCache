package com.example.jcache;

import com.cache.client.CacheClient.CacheClientException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Retries a call that the server rejected with {@code rate limit exceeded}.
 * The server does not queue rejected commands, so the client has to wait for
 * its bucket to refill and send again. Other errors are not retried.
 */
public final class RateLimitRetry {

    /** A cache call to run. */
    @FunctionalInterface
    public interface Call<T> {
        T run() throws IOException;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private static final String RATE_LIMITED = "rate limit exceeded";

    private final int maxAttempts;
    private final long initialBackoffMs;
    private final Sleeper sleeper;
    private final AtomicLong retries = new AtomicLong();

    /**
     * @param maxAttempts      attempts per call, including the first
     * @param initialBackoffMs wait before the first retry; doubles after each one
     */
    public RateLimitRetry(int maxAttempts, long initialBackoffMs) {
        this(maxAttempts, initialBackoffMs, Thread::sleep);
    }

    RateLimitRetry(int maxAttempts, long initialBackoffMs, Sleeper sleeper) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, got: " + maxAttempts);
        }
        if (initialBackoffMs < 0) {
            throw new IllegalArgumentException("initialBackoffMs cannot be negative: " + initialBackoffMs);
        }
        this.maxAttempts = maxAttempts;
        this.initialBackoffMs = initialBackoffMs;
        this.sleeper = sleeper;
    }

    /**
     * Runs the call, retrying while the server reports the rate limit.
     *
     * @return what the call returned
     * @throws IOException the last rate-limit error once attempts run out, or
     *                     any other error straight away
     */
    public <T> T call(Call<T> call) throws IOException, InterruptedException {
        long backoff = initialBackoffMs;
        for (int attempt = 1; ; attempt++) {
            try {
                return call.run();
            } catch (CacheClientException e) {
                if (!isRateLimited(e) || attempt == maxAttempts) {
                    throw e;
                }
            }
            retries.incrementAndGet();
            sleeper.sleep(backoff);
            backoff *= 2;
        }
    }

    /** @return how many retries this instance has made */
    public long retries() {
        return retries.get();
    }

    /** @return whether the error is the server's rate-limit rejection */
    public static boolean isRateLimited(IOException e) {
        return e instanceof CacheClientException && e.getMessage() != null && e.getMessage().contains(RATE_LIMITED);
    }
}
