package com.cache.server;

import com.cache.api.CachePolicyType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ServerConfig")
class ServerConfigTest {

    @Test
    @DisplayName("Environment variables override defaults")
    void environment() {
        ServerConfig config = ServerConfig.builder()
                .applyEnvironment(Map.of(
                        "JCACHE_PORT", "7000",
                        "JCACHE_POLICY", "arc",
                        "JCACHE_SEGMENTS", "32",
                        "JCACHE_DEFAULT_TTL", "60",
                        "JCACHE_PERSISTENCE_ENABLED", "true",
                        "JCACHE_SNAPSHOT_PATH", "/data"))
                .build();

        assertEquals(7000, config.getPort());
        assertEquals(CachePolicyType.ARC, config.getEvictionPolicy());
        assertEquals(32, config.getSegments());
        assertEquals(60, config.getDefaultTtlSeconds());
        assertTrue(config.isPersistenceEnabled());
        assertEquals("/data", config.getSnapshotPath());
        assertEquals(ServerConfig.DEFAULT_CACHE_CAPACITY, config.getCacheCapacity());
    }

    @Test
    @DisplayName("Properties are applied, and environment variables win over them")
    void precedence(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("jcache.properties");
        Files.writeString(file, "server.port=7001\ncache.capacity=500\n");

        ServerConfig config = ServerConfig.builder()
                .applyProperties(file.toString())
                .applyEnvironment(Map.of("JCACHE_PORT", "7002"))
                .build();

        assertEquals(7002, config.getPort());
        assertEquals(500, config.getCacheCapacity());
    }

    @Test
    @DisplayName("Invalid values fail with the name of the setting")
    void invalidValue() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ServerConfig.builder().applyEnvironment(Map.of("JCACHE_CAPACITY", "lots")));
        assertTrue(e.getMessage().contains("JCACHE_CAPACITY"), e.getMessage());

        assertThrows(IllegalArgumentException.class,
                () -> ServerConfig.builder().applyEnvironment(Map.of("JCACHE_POLICY", "FIFO")));
        assertThrows(IllegalArgumentException.class, () -> ServerConfig.builder().segments(12));
    }

    @Test
    @DisplayName("Command-line flags are parsed")
    void commandLine() {
        ServerConfig config = CacheServer.parseArgs(new String[]{
                "--port", "7003", "--policy", "lfu", "--capacity", "42", "--default-ttl", "5", "--persist"});

        assertEquals(7003, config.getPort());
        assertEquals(CachePolicyType.LFU, config.getEvictionPolicy());
        assertEquals(42, config.getCacheCapacity());
        assertEquals(5, config.getDefaultTtlSeconds());
        assertTrue(config.isPersistenceEnabled());
    }

    @Test
    @DisplayName("Unknown flags and missing values are rejected; --help returns null")
    void commandLineErrors() {
        assertThrows(IllegalArgumentException.class, () -> CacheServer.parseArgs(new String[]{"--nope"}));
        assertThrows(IllegalArgumentException.class, () -> CacheServer.parseArgs(new String[]{"--port"}));
        assertNull(CacheServer.parseArgs(new String[]{"--help"}));
    }

    @Test
    @DisplayName("Rate limit is off by default and the burst defaults to the rate")
    void rateLimit(@TempDir Path dir) throws IOException {
        ServerConfig defaults = ServerConfig.defaults();
        assertEquals(0, defaults.getRateLimitPerSecond());

        ServerConfig fromEnv = ServerConfig.builder()
                .applyEnvironment(Map.of("JCACHE_RATE_LIMIT", "500"))
                .build();
        assertEquals(500, fromEnv.getRateLimitPerSecond());
        assertEquals(500, fromEnv.getRateLimitBurst());

        Path file = dir.resolve("jcache.properties");
        Files.writeString(file, "server.rate.limit=100\nserver.rate.limit.burst=250\n");
        ServerConfig fromFile = ServerConfig.fromProperties(file.toString());
        assertEquals(100, fromFile.getRateLimitPerSecond());
        assertEquals(250, fromFile.getRateLimitBurst());

        ServerConfig fromArgs = CacheServer.parseArgs(new String[]{"--rate-limit", "20", "--rate-limit-burst", "40"});
        assertEquals(20, fromArgs.getRateLimitPerSecond());
        assertEquals(40, fromArgs.getRateLimitBurst());

        assertThrows(IllegalArgumentException.class, () -> ServerConfig.builder().rateLimitPerSecond(-1));
        assertThrows(IllegalArgumentException.class, () -> ServerConfig.builder().rateLimitBurst(-1));
    }

    @Test
    @DisplayName("toBuilder() round-trips every setting")
    void toBuilder() {
        ServerConfig original = ServerConfig.builder()
                .port(1234).cacheCapacity(77).segments(4).defaultTtlSeconds(9).verbose(true)
                .rateLimitPerSecond(50).rateLimitBurst(80).build();
        assertEquals(original.toString(), original.toBuilder().build().toString());
    }
}
