package com.cache.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheServerTest {

    @Test
    void startupMessageShowsRateLimit() {
        String limited = CacheServer.startupMessage(6379,
                ServerConfig.builder().rateLimitPerSecond(5).rateLimitBurst(3).build());
        assertTrue(limited.contains("rateLimit=5/s burst 3"), limited);

        String sameBurst = CacheServer.startupMessage(6379, ServerConfig.builder().rateLimitPerSecond(100).build());
        assertTrue(sameBurst.contains("rateLimit=100/s burst 100"), sameBurst);

        String unlimited = CacheServer.startupMessage(6379, ServerConfig.defaults());
        assertTrue(unlimited.contains("rateLimit=off"), unlimited);
    }
}
