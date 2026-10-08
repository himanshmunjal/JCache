package com.cache.server;

import com.cache.client.CacheClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Server restart with persistence")
class PersistenceRestartTest {

    private static CacheServer start(Path dataDir) {
        CacheServer server = new CacheServer(ServerConfig.builder()
                .port(0)
                .persistenceEnabled(true)
                .snapshotPath(dataDir.toString())
                .snapshotIntervalMs(0)
                .build());
        server.startAsync();
        return server;
    }

    @Test
    @DisplayName("Data, including values with spaces and TTLs, survives a restart")
    void dataSurvivesRestart(@TempDir Path dataDir) throws IOException {
        CacheServer first = start(dataDir);
        try (CacheClient client = new CacheClient("localhost", first.getPort())) {
            client.put("user:1", "Alice Smith");
            client.put("session", "token", 3600);
            client.put("gone", "x");
            client.delete("gone");
        }
        first.shutdown();

        CacheServer second = start(dataDir);
        try (CacheClient client = new CacheClient("localhost", second.getPort())) {
            assertEquals("Alice Smith", client.get("user:1"));
            assertEquals("token", client.get("session"));
            long ttl = client.ttl("session");
            assertTrue(ttl > 3500 && ttl <= 3600, "ttl: " + ttl);
            assertNull(client.get("gone"));
        } finally {
            second.shutdown();
        }
    }

    @Test
    @DisplayName("Writes after the last snapshot are replayed from the AOF after a crash")
    void aofReplayAfterCrash(@TempDir Path dataDir) throws IOException {
        CacheServer first = start(dataDir);
        try (CacheClient client = new CacheClient("localhost", first.getPort())) {
            client.put("a", "1");
            client.flush();
            client.put("b", "2");
        }
        // No shutdown(): simulate a crash by starting a second server on the same directory.
        CacheServer second = start(dataDir);
        try (CacheClient client = new CacheClient("localhost", second.getPort())) {
            assertNull(client.get("a"));
            assertEquals("2", client.get("b"));
        } finally {
            second.shutdown();
            first.shutdown();
        }
    }
}
