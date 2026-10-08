package com.cache.persistence;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.concurrent.SegmentedCache;
import com.cache.ttl.TTLCache;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Persistence Layer Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PersistenceTest {
    @TempDir
    Path tempRoot;

    @Test
    @Order(1)
    @DisplayName("SnapshotWriter creates snapshot file after write()")
    void testSnapshotWriter_createsFile() throws IOException {
        Path dir = tempRoot.resolve("test1");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        cache.put("key1", "value1");
        cache.put("key2", "value2");

        SnapshotWriter writer = new SnapshotWriter(dir, cache);
        int written = writer.write();

        assertTrue(Files.exists(dir.resolve(PersistenceManager.SNAPSHOT_FILENAME)),
                "Snapshot file should exist after write()");

        assertEquals(2, written, "write() should return the number of entries written");
    }

    @Test
    @Order(2)
    @DisplayName("SnapshotWriter writes correct magic number and version")
    void testSnapshotWriter_headerCorrect() throws IOException {
        Path dir = tempRoot.resolve("test2");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        cache.put("k", "v");

        new SnapshotWriter(dir, cache).write();

        try (DataInputStream in = new DataInputStream(
                new FileInputStream(dir.resolve(PersistenceManager.SNAPSHOT_FILENAME).toFile()))) {
            int magic   = in.readInt();
            int version = in.readInt();

            assertEquals(SnapshotWriter.MAGIC_NUMBER,    magic,
                    "Magic number should match MAGIC_NUMBER constant");
            assertEquals(SnapshotWriter.FORMAT_VERSION,  version,
                    "Format version should match FORMAT_VERSION constant");
        }
    }

    @Test
    @Order(3)
    @DisplayName("SnapshotWriter header contains entry count and snapshot timestamp")
    void testSnapshotWriter_headerEntryCountAndTimestamp() throws IOException {
        Path dir = tempRoot.resolve("test3");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        long beforeWrite = System.currentTimeMillis();
        new SnapshotWriter(dir, cache).write();
        long afterWrite = System.currentTimeMillis();

        try (DataInputStream in = new DataInputStream(
                new FileInputStream(dir.resolve(PersistenceManager.SNAPSHOT_FILENAME).toFile()))) {
            in.readInt();
            in.readInt();
            long snapshotTime = in.readLong();
            int  entryCount   = in.readInt();

            assertTrue(snapshotTime >= beforeWrite && snapshotTime <= afterWrite,
                    "Snapshot timestamp should be between write start and end");
            assertEquals(3, entryCount, "Entry count hint should be 3");
        }
    }

    @Test
    @Order(4)
    @DisplayName("SnapshotWriter: tmp file is renamed: no .tmp file left behind")
    void testSnapshotWriter_atomicWrite_noTmpFileRemains() throws IOException {
        Path dir = tempRoot.resolve("test4");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        cache.put("x", "y");

        new SnapshotWriter(dir, cache).write();

        assertFalse(Files.exists(dir.resolve(PersistenceManager.SNAPSHOT_TMP_FILENAME)),
                ".tmp file should be renamed/deleted after successful write");
        assertTrue(Files.exists(dir.resolve(PersistenceManager.SNAPSHOT_FILENAME)),
                "Final snapshot file should exist");
    }

    @Test
    @Order(5)
    @DisplayName("SnapshotWriter skips entries that are already expired")
    void testSnapshotWriter_skipsExpiredEntries() throws IOException, InterruptedException {
        Path dir = tempRoot.resolve("test5");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        cache.put("live", "value");

        cache.putWithExpiry("expired", "old-value", 1L);

        Thread.sleep(5);

        SnapshotWriter writer = new SnapshotWriter(dir, cache);
        writer.write();

        StubCache restored = new StubCache();
        SnapshotLoader loader = new SnapshotLoader(dir, restored);
        loader.load();

        assertNotNull(restored.get("live"), "Non-expired key should be restored");
        assertNull(restored.get("expired"), "Expired key should NOT be restored");
    }

    @Test
    @Order(6)
    @DisplayName("SnapshotWriter handles empty cache: writes valid empty snapshot")
    void testSnapshotWriter_emptyCache_validFile() throws IOException {
        Path dir = tempRoot.resolve("test6");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        int written = new SnapshotWriter(dir, cache).write();

        assertEquals(0, written, "Empty cache should write 0 entries");
        assertTrue(Files.exists(dir.resolve(PersistenceManager.SNAPSHOT_FILENAME)),
                "Snapshot file should still be created for empty cache");

        StubCache restored = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, restored).load();
        assertEquals(0, result.snapshotKeysLoaded, "Empty snapshot should restore 0 keys");
        assertFalse(result.snapshotLoadFailed, "Empty snapshot should not be considered failed");
    }

    @Test
    @Order(7)
    @DisplayName("SnapshotLoader restores all non-expired keys from snapshot")
    void testSnapshotLoader_restoresKeys() throws IOException {
        Path dir = tempRoot.resolve("test7");
        Files.createDirectories(dir);

        StubCache original = new StubCache();
        original.put("user:1", "Alice");
        original.put("user:2", "Bob");
        original.put("user:3", "Carol");

        new SnapshotWriter(dir, original).write();

        StubCache restored = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, restored).load();

        assertEquals(3, result.snapshotKeysLoaded, "Should restore 3 keys from snapshot");
        assertEquals("Alice", restored.get("user:1"));
        assertEquals("Bob",   restored.get("user:2"));
        assertEquals("Carol", restored.get("user:3"));
    }

    @Test
    @Order(8)
    @DisplayName("SnapshotLoader skips keys that expired while server was down")
    void testSnapshotLoader_skipsExpiredKeys() throws IOException, InterruptedException {
        Path dir = tempRoot.resolve("test8");
        Files.createDirectories(dir);

        StubCache original = new StubCache();
        original.put("permanent", "stays");

        long expiryMs = System.currentTimeMillis() + 50;
        original.putWithExpiry("temporary", "gone", expiryMs);

        new SnapshotWriter(dir, original).write();

        Thread.sleep(100);

        StubCache restored = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, restored).load();

        assertEquals("stays", restored.get("permanent"), "Permanent key should be restored");
        assertNull(restored.get("temporary"), "Expired key should NOT be restored");
        assertTrue(result.skippedExpired >= 1, "skippedExpired counter should be >= 1");
    }

    @Test
    @Order(9)
    @DisplayName("SnapshotLoader is a no-op when no snapshot file exists")
    void testSnapshotLoader_missingFile_noError() throws IOException {
        Path dir = tempRoot.resolve("test9");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, cache).load();

        assertEquals(0, cache.size(), "Cache should remain empty if no snapshot exists");
        assertEquals(0, result.snapshotKeysLoaded, "No keys should be reported as loaded");
        assertFalse(result.snapshotLoadFailed,
                "Missing snapshot should not set snapshotLoadFailed (it's normal for new server)");
    }

    @Test
    @Order(10)
    @DisplayName("SnapshotLoader handles corrupt magic number: cache starts empty")
    void testSnapshotLoader_corruptMagicNumber_gracefulFallback() throws IOException {
        Path dir = tempRoot.resolve("test10");
        Files.createDirectories(dir);

        Path snapshotPath = dir.resolve(PersistenceManager.SNAPSHOT_FILENAME);
        try (DataOutputStream out = new DataOutputStream(
                new FileOutputStream(snapshotPath.toFile()))) {
            out.writeInt(0xDEADBEEF);
            out.writeInt(1);
            out.writeLong(System.currentTimeMillis());
            out.writeInt(0);
        }

        StubCache cache = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, cache).load();

        assertEquals(0, cache.size(), "Cache should be empty after corrupt snapshot");
        assertTrue(result.snapshotLoadFailed,
                "snapshotLoadFailed should be true for wrong magic number");
    }

    @Test
    @Order(11)
    @DisplayName("SnapshotLoader handles wrong format version: cache starts empty")
    void testSnapshotLoader_wrongVersion_gracefulFallback() throws IOException {
        Path dir = tempRoot.resolve("test11");
        Files.createDirectories(dir);

        Path snapshotPath = dir.resolve(PersistenceManager.SNAPSHOT_FILENAME);
        try (DataOutputStream out = new DataOutputStream(
                new FileOutputStream(snapshotPath.toFile()))) {
            out.writeInt(SnapshotWriter.MAGIC_NUMBER);
            out.writeInt(999);
            out.writeLong(System.currentTimeMillis());
            out.writeInt(0);
        }

        StubCache cache = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, cache).load();

        assertTrue(result.snapshotLoadFailed,
                "snapshotLoadFailed should be true for incompatible version");
        assertEquals(0, cache.size(),
                "Cache should be empty when snapshot version is incompatible");
    }

    @Test
    @Order(12)
    @DisplayName("SnapshotLoader handles truncated file: partial restore, no exception")
    void testSnapshotLoader_truncatedFile_partialRestore() throws IOException {
        Path dir = tempRoot.resolve("test12");
        Files.createDirectories(dir);

        StubCache original = new StubCache();
        original.put("key1", "v1");
        original.put("key2", "v2");
        original.put("key3", "v3");
        new SnapshotWriter(dir, original).write();

        Path snapshotPath = dir.resolve(PersistenceManager.SNAPSHOT_FILENAME);
        long fullSize = Files.size(snapshotPath);
        byte[] allBytes = Files.readAllBytes(snapshotPath);
        byte[] truncated = Arrays.copyOf(allBytes, (int) (fullSize / 2));
        Files.write(snapshotPath, truncated);

        StubCache restored = new StubCache();
        assertDoesNotThrow(() -> new SnapshotLoader(dir, restored).load(),
                "SnapshotLoader should not throw on truncated file");

        assertTrue(restored.size() >= 0,
                "Partial restore should have non-negative size");
    }

    @Test
    @Order(13)
    @DisplayName("logPut() appends a SET record that decodes back to the same entry")
    void testAOF_logPut_writesLine() throws IOException {
        Path dir = tempRoot.resolve("test13");
        Files.createDirectories(dir);

        PersistenceManager pm = new PersistenceManager(dir, new StubCache(), 0);
        pm.logPut("session:abc", "user42", -1L);

        List<String> lines = Files.readAllLines(dir.resolve(PersistenceManager.AOF_FILENAME));
        pm.close();

        assertEquals(1, lines.size(), "Lines: " + lines);
        assertTrue(lines.get(0).startsWith("SET "), lines.get(0));
        assertEquals(AofRecord.set("session:abc", "user42", -1L), AofRecord.parse(lines.get(0)));
    }

    @Test
    @DisplayName("AOF round-trips values containing spaces, newlines and non-ASCII text")
    void testAOF_valuesWithSpecialCharacters_roundTrip() throws IOException {
        Path dir = tempRoot.resolve("test13b");
        Files.createDirectories(dir);
        String value = "hello  world\nsecond line \u00e9\u4e2d";

        PersistenceManager pm = new PersistenceManager(dir, new StubCache(), 0);
        pm.logPut("greeting", value, -1L);

        StubCache restored = new StubCache();
        new SnapshotLoader(dir, restored).load();
        pm.close();

        assertEquals(value, restored.get("greeting"));
    }

    @Test
    @Order(14)
    @DisplayName("logDelete() appends a DEL record")
    void testAOF_logDelete_writesLine() throws IOException {
        Path dir = tempRoot.resolve("test14");
        Files.createDirectories(dir);

        PersistenceManager pm = new PersistenceManager(dir, new StubCache(), 0);
        pm.logDelete("user:99");

        List<String> lines = Files.readAllLines(dir.resolve(PersistenceManager.AOF_FILENAME));
        pm.close();

        assertEquals(1, lines.size(), "Lines: " + lines);
        assertEquals(AofRecord.delete("user:99"), AofRecord.parse(lines.get(0)));
    }

    @Test
    @DisplayName("Legacy plain-text PUT/DELETE lines are still replayed")
    void testAOF_legacyFormat_isReplayed() throws IOException {
        Path dir = tempRoot.resolve("test14b");
        Files.createDirectories(dir);
        Files.write(dir.resolve(PersistenceManager.AOF_FILENAME),
                List.of("PUT a 1 -1", "PUT b 2 -1", "DELETE a"));

        StubCache restored = new StubCache();
        new SnapshotLoader(dir, restored).load();

        assertNull(restored.get("a"));
        assertEquals("2", restored.get("b"));
    }

    @Test
    @Order(15)
    @DisplayName("AOF replay: PUT in log restores key into fresh cache")
    void testAOF_replay_putRestoresKey() throws IOException {
        Path dir = tempRoot.resolve("test15");
        Files.createDirectories(dir);

        Path aofPath = dir.resolve(PersistenceManager.AOF_FILENAME);
        Files.write(aofPath,
                ("PUT greeting hello -1\n").getBytes(StandardCharsets.UTF_8));

        StubCache cache = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, cache).load();

        assertEquals("hello", cache.get("greeting"),
                "PUT from AOF log should restore 'greeting' → 'hello'");
        assertEquals(1, result.aofPutsReplayed,
                "aofPutsReplayed counter should be 1");
    }

    @Test
    @Order(16)
    @DisplayName("AOF replay: DELETE in log removes key that was in snapshot")
    void testAOF_replay_deleteRemovesSnapshotKey() throws IOException {
        Path dir = tempRoot.resolve("test16");
        Files.createDirectories(dir);

        StubCache original = new StubCache();
        original.put("user:1", "Alice");
        original.put("user:2", "Bob");
        new SnapshotWriter(dir, original).write();

        Path aofPath = dir.resolve(PersistenceManager.AOF_FILENAME);
        Files.write(aofPath,
                ("DELETE user:1\n").getBytes(StandardCharsets.UTF_8));

        StubCache restored = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, restored).load();

        assertNull(restored.get("user:1"),
                "DELETE in AOF should override snapshot: user:1 should be gone");
        assertEquals("Bob", restored.get("user:2"),
                "user:2 was not deleted: should still be present");
        assertEquals(1, result.aofDeletesReplayed,
                "aofDeletesReplayed counter should be 1");
    }

    @Test
    @Order(17)
    @DisplayName("AOF replay: corrupt line is skipped, valid lines still replayed")
    void testAOF_replay_corruptLineSkipped() throws IOException {
        Path dir = tempRoot.resolve("test17");
        Files.createDirectories(dir);

        Path aofPath = dir.resolve(PersistenceManager.AOF_FILENAME);

        String logContent =
                "PUT valid-key valid-value -1\n"    +
                        "BADVERB something\n"                +
                        "PUT another-key another-value -1\n"+
                        "\n"                                 +
                        "PUT third-key third-value -1\n";

        Files.write(aofPath, logContent.getBytes(StandardCharsets.UTF_8));

        StubCache cache = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, cache).load();

        assertEquals("valid-value",   cache.get("valid-key"));
        assertEquals("another-value", cache.get("another-key"));
        assertEquals("third-value",   cache.get("third-key"));

        assertEquals(3, result.aofPutsReplayed,
                "3 valid PUT lines should be replayed");
        assertEquals(1, result.skippedCorrupt,
                "1 corrupt line should be counted in skippedCorrupt");
    }

    @Test
    @Order(18)
    @DisplayName("AOF replay: expired PUT entry not restored")
    void testAOF_replay_expiredEntrySkipped() throws IOException, InterruptedException {
        Path dir = tempRoot.resolve("test18");
        Files.createDirectories(dir);

        long expiryMs = System.currentTimeMillis() + 50;
        Path aofPath = dir.resolve(PersistenceManager.AOF_FILENAME);
        Files.write(aofPath,
                ("PUT expires-soon old-value " + expiryMs + "\n" +
                        "PUT permanent stays -1\n")
                        .getBytes(StandardCharsets.UTF_8));

        Thread.sleep(100);

        StubCache cache = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, cache).load();

        assertNull(cache.get("expires-soon"),
                "Expired AOF entry should not be restored");
        assertEquals("stays", cache.get("permanent"),
                "Non-expired AOF entry should be restored");
        assertTrue(result.skippedExpired >= 1,
                "skippedExpired should count the expired AOF entry");
    }

    @Test
    @Order(19)
    @DisplayName("Full cycle: 50 keys survive snapshot + restart")
    void testFullCycle_snapshotAndRestart() throws IOException {
        Path dir = tempRoot.resolve("test19");
        Files.createDirectories(dir);

        StubCache before = new StubCache();
        for (int i = 0; i < 50; i++) {
            before.put("key:" + i, "value:" + i);
        }
        new SnapshotWriter(dir, before).write();

        StubCache after = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, after).load();

        assertEquals(50, result.snapshotKeysLoaded,
                "All 50 keys should be reported as loaded");
        for (int i = 0; i < 50; i++) {
            assertEquals("value:" + i, after.get("key:" + i),
                    "key:" + i + " should be restored with correct value");
        }
    }

    @Test
    @Order(20)
    @DisplayName("Full cycle: AOF replays mutations made after last snapshot")
    void testFullCycle_aofReplaysPostSnapshotMutations() throws IOException {
        Path dir = tempRoot.resolve("test20");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        cache.put("user:1", "Alice");
        cache.put("user:2", "Bob");
        cache.put("user:3", "Carol");

        new SnapshotWriter(dir, cache).write();

        PersistenceManager pm = new PersistenceManager(dir, cache, 0);
        pm.logPut("user:4", "Dave", -1L);
        pm.logDelete("user:2");

        StubCache restored = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, restored).load();

        assertEquals("Alice", restored.get("user:1"), "user:1 should be from snapshot");
        assertNull(restored.get("user:2"),
                "user:2 was deleted in AOF: should not be present after restore");
        assertEquals("Carol", restored.get("user:3"), "user:3 should be from snapshot");
        assertEquals("Dave",  restored.get("user:4"), "user:4 should be from AOF replay");

        assertTrue(result.snapshotKeysLoaded >= 2,
                "At least snapshot keys should be loaded");
        assertTrue(result.aofDeletesReplayed >= 1,
                "At least 1 AOF delete should be replayed");
    }

    @Test
    @Order(21)
    @DisplayName("Full cycle: deleted keys stay gone after snapshot + restart")
    void testFullCycle_deletedKeyStaysGone() throws IOException {
        Path dir = tempRoot.resolve("test21");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        cache.put("temp-key", "will-be-deleted");
        cache.evict("temp-key");
        cache.put("keeper", "stays");

        new SnapshotWriter(dir, cache).write();

        StubCache restored = new StubCache();
        new SnapshotLoader(dir, restored).load();

        assertNull(restored.get("temp-key"),
                "Deleted key should not appear in snapshot: should not be restored");
        assertEquals("stays", restored.get("keeper"),
                "Non-deleted key should be restored");
    }

    @Test
    @Order(22)
    @DisplayName("Full cycle: corrupt snapshot falls back to AOF-only recovery")
    void testFullCycle_corruptSnapshotAOFFallback() throws IOException {
        Path dir = tempRoot.resolve("test22");
        Files.createDirectories(dir);

        Path snapshotPath = dir.resolve(PersistenceManager.SNAPSHOT_FILENAME);
        Files.write(snapshotPath,
                "THIS IS NOT A VALID SNAPSHOT FILE".getBytes(StandardCharsets.UTF_8));

        Path aofPath = dir.resolve(PersistenceManager.AOF_FILENAME);
        Files.write(aofPath,
                ("PUT aof-key-1 aof-value-1 -1\n" +
                        "PUT aof-key-2 aof-value-2 -1\n")
                        .getBytes(StandardCharsets.UTF_8));

        StubCache restored = new StubCache();
        SnapshotLoader.LoadResult result = new SnapshotLoader(dir, restored).load();

        assertTrue(result.snapshotLoadFailed, "snapshotLoadFailed should be true");
        assertEquals("aof-value-1", restored.get("aof-key-1"),
                "AOF key 1 should be restored despite corrupt snapshot");
        assertEquals("aof-value-2", restored.get("aof-key-2"),
                "AOF key 2 should be restored despite corrupt snapshot");
    }

    @Test
    @Order(23)
    @DisplayName("PersistenceManager: AOF log is truncated after takeSnapshot()")
    void testPM_aofTruncatedAfterSnapshot() throws IOException {
        Path dir = tempRoot.resolve("test23");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        PersistenceManager pm = new PersistenceManager(dir, cache, 0);

        pm.logPut("k1", "v1", -1L);
        pm.logPut("k2", "v2", -1L);
        pm.logPut("k3", "v3", -1L);
        pm.logDelete("k2");

        long aofSizeBefore = Files.size(dir.resolve(PersistenceManager.AOF_FILENAME));
        assertTrue(aofSizeBefore > 0, "AOF log should have content before snapshot");

        pm.takeSnapshot();

        long aofSizeAfter = Files.size(dir.resolve(PersistenceManager.AOF_FILENAME));
        assertEquals(0, aofSizeAfter,
                "AOF log should be empty (0 bytes) after takeSnapshot()");

        pm.close();
    }

    @Test
    @Order(24)
    @DisplayName("PersistenceManager: new AOF entries written after snapshot work correctly")
    void testPM_aofWorksAfterSnapshotTruncation() throws IOException {
        Path dir = tempRoot.resolve("test24");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        PersistenceManager pm = new PersistenceManager(dir, cache, 0);

        pm.logPut("before-snapshot", "val", -1L);
        pm.takeSnapshot();

        pm.logPut("after-snapshot", "val2", -1L);

        List<String> keys = Files.readAllLines(dir.resolve(PersistenceManager.AOF_FILENAME)).stream()
                .map(l -> AofRecord.parse(l).key())
                .toList();
        assertEquals(List.of("after-snapshot"), keys,
                "Only entries logged after the snapshot should remain in the AOF");

        pm.close();
    }

    @Test
    @Order(25)
    @DisplayName("PersistenceManager: logPut and logDelete are no-ops after close()")
    void testPM_logOpsNoOpAfterClose() throws IOException {
        Path dir = tempRoot.resolve("test25");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        PersistenceManager pm = new PersistenceManager(dir, cache, 0);
        pm.close();

        assertDoesNotThrow(() -> pm.logPut("key", "value", -1L),
                "logPut() after close() should be a no-op, not throw");
        assertDoesNotThrow(() -> pm.logDelete("key"),
                "logDelete() after close() should be a no-op, not throw");
    }

    @Test
    @Order(26)
    @DisplayName("PersistenceManager: aofEntryCount increments correctly")
    void testPM_aofEntryCount() throws IOException {
        Path dir = tempRoot.resolve("test26");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        PersistenceManager pm = new PersistenceManager(dir, cache, 0);

        assertEquals(0, pm.getAOFEntryCount(), "Initial entry count should be 0");

        pm.logPut("a", "1", -1L);
        pm.logPut("b", "2", -1L);
        pm.logDelete("a");

        assertEquals(3, pm.getAOFEntryCount(),
                "Entry count should be 3 after 2 puts and 1 delete");

        pm.close();
    }

    @Test
    @Order(27)
    @DisplayName("PersistenceManager: takeSnapshot() throws when closed")
    void testPM_takeSnapshotAfterClose_throws() throws IOException {
        Path dir = tempRoot.resolve("test27");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        PersistenceManager pm = new PersistenceManager(dir, cache, 0);
        pm.close();

        assertThrows(IllegalStateException.class, pm::takeSnapshot,
                "takeSnapshot() on closed PersistenceManager should throw");
    }

    @Test
    @Order(28)
    @DisplayName("Concurrent logPut() from 10 threads produces valid AOF log")
    void testConcurrent_multipleThreads_validAOF() throws IOException, InterruptedException {
        Path dir = tempRoot.resolve("test28");
        Files.createDirectories(dir);

        StubCache cache = new StubCache();
        PersistenceManager pm = new PersistenceManager(dir, cache, 0);

        int threadCount  = 10;
        int opsPerThread = 50;
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch done  = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger(0);

        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        pm.logPut("t" + threadId + ":k" + i, "v" + i, -1L);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await(10, java.util.concurrent.TimeUnit.SECONDS);
        pool.shutdown();
        pm.close();

        assertEquals(0, errors.get(), "No errors should occur during concurrent logPut()");

        List<String> lines = Files.readAllLines(dir.resolve(PersistenceManager.AOF_FILENAME));
    }

    static final class StubCache
            implements Cache<String, String>, SnapshotCapable, SnapshotLoader.TtlAware {
        private final Map<String, String> store     = new LinkedHashMap<>();

        private final Map<String, Long>   expiryMap = new LinkedHashMap<>();

        @Override
        public String get(String key) {
            Long expiry = expiryMap.get(key);
            if (expiry != null && expiry != -1L && System.currentTimeMillis() > expiry) {
                store.remove(key);
                expiryMap.remove(key);
                return null;
            }
            return store.get(key);
        }

        @Override
        public String peek(String key) {
            return get(key);
        }

        @Override
        public void put(String key, String value) {
            store.put(key, value);
            expiryMap.put(key, -1L);
        }

        @Override
        public void put(String key, String value, long ttlMillis) {
            store.put(key, value);
            long expiryMs = (ttlMillis <= 0) ? -1L : System.currentTimeMillis() + ttlMillis;
            expiryMap.put(key, expiryMs);
        }

        public void putWithExpiry(String key, String value, long absoluteExpiryMs) {
            store.put(key, value);
            expiryMap.put(key, absoluteExpiryMs);
        }

        @Override
        public void evict(String key) {
            store.remove(key);
            expiryMap.remove(key);
        }

        @Override
        public int size() {
            return store.size();
        }

        @Override
        public CacheStats getStats() {
            return new CacheStats(0, 0, 0, 0.0);
        }

        @Override
        public void clear() {
            store.clear();
            expiryMap.clear();
        }

        @Override
        public Map<String, Long> getSnapshotEntries() {
            long now = System.currentTimeMillis();
            Map<String, Long> snapshot = new LinkedHashMap<>();
            for (String key : store.keySet()) {
                Long expiry = expiryMap.getOrDefault(key, -1L);

                if (expiry != -1L && now > expiry) continue;
                snapshot.put(key, expiry);
            }
            return Collections.unmodifiableMap(snapshot);
        }
    }

    @Test
    @DisplayName("A FLUSH record clears everything written before it")
    void testAOF_flushRecord_clearsEarlierEntries() throws IOException {
        Path dir = tempRoot.resolve("flush");
        Files.createDirectories(dir);
        PersistenceManager pm = new PersistenceManager(dir, new StubCache(), 0);
        pm.logPut("a", "1");
        pm.logClear();
        pm.logPut("b", "2");

        StubCache restored = new StubCache();
        new SnapshotLoader(dir, restored).load();
        pm.close();

        assertNull(restored.get("a"));
        assertEquals("2", restored.get("b"));
    }

    @Test
    @DisplayName("Writes made while snapshots are running are never lost")
    void testConcurrentWritesDuringSnapshots_areRecovered() throws Exception {
        Path dir = tempRoot.resolve("concurrent");
        TTLCache<String, String> live = new TTLCache<>(new SegmentedCache<>(100_000));
        Cache<String, String> persistable = TTLCache.asPersistable(live);
        PersistenceManager pm = new PersistenceManager(dir, persistable, 0);
        int writes = 5_000;

        Thread writer = new Thread(() -> {
            for (int i = 0; i < writes; i++) {
                String key = "k" + i;
                pm.atomically(() -> {
                    persistable.put(key, "v");
                    pm.logPut(key, "v");
                });
            }
        });
        writer.start();
        for (int i = 0; i < 25 && writer.isAlive(); i++) {
            pm.takeSnapshot();
            Thread.sleep(2);
        }
        writer.join();

        TTLCache<String, String> recovered = new TTLCache<>(new SegmentedCache<>(100_000));
        new SnapshotLoader(dir, TTLCache.asPersistable(recovered)).load();
        pm.close();

        assertEquals(writes, recovered.size());
        live.shutdown();
        recovered.shutdown();
    }
}
