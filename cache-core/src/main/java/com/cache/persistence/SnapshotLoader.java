package com.cache.persistence;

import com.cache.api.Cache;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Restores a cache on startup: loads the latest snapshot, then replays the
 * append-only file on top of it, since the AOF only contains changes made
 * after that snapshot.
 *
 * <p>Entries whose expiry passed while the server was down are skipped.
 * Recovery is best-effort: a corrupt snapshot is ignored (the AOF is still
 * replayed), and a corrupt AOF line is skipped. Both cases are reported in
 * the returned {@link LoadResult}.
 */
public class SnapshotLoader {

    private static final Logger log = Logger.getLogger(SnapshotLoader.class.getName());

    private static final int MAX_KEY_BYTES = 1 << 20;
    private static final int MAX_VALUE_BYTES = 100 << 20;

    private final Path baseDir;
    private final Cache<String, String> cache;

    /**
     * Creates a loader.
     *
     * @param baseDir directory holding the snapshot and AOF files
     * @param cache   cache to restore into; should be empty. Implement
     *                {@link TtlAware} so that TTLs survive the restart.
     */
    public SnapshotLoader(Path baseDir, Cache<String, String> cache) {
        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir cannot be null");
        }
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        this.baseDir = baseDir;
        this.cache = cache;
    }

    /**
     * Runs the recovery. Call once, before the cache starts serving requests.
     *
     * @return counts of what was restored and skipped
     */
    public LoadResult load() {
        LoadResult result = new LoadResult();
        loadSnapshot(result);
        replayAof(result);
        log.fine(() -> "Recovery finished: " + result + ", cacheSize=" + cache.size());
        return result;
    }

    private void loadSnapshot(LoadResult result) {
        Path path = baseDir.resolve(PersistenceManager.SNAPSHOT_FILENAME);
        if (!Files.exists(path)) {
            return;
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            int magic = in.readInt();
            int version = in.readInt();
            in.readLong(); // snapshot timestamp, informational only
            int count = in.readInt();
            if (magic != SnapshotWriter.MAGIC_NUMBER) {
                log.warning(String.format("Ignoring %s: bad magic number 0x%08X", path, magic));
                result.snapshotLoadFailed = true;
                return;
            }
            if (version != SnapshotWriter.FORMAT_VERSION) {
                log.warning("Ignoring " + path + ": unsupported format version " + version);
                result.snapshotLoadFailed = true;
                return;
            }
            for (int i = 0; i < count; i++) {
                String key = readString(in, MAX_KEY_BYTES);
                String value = readString(in, MAX_VALUE_BYTES);
                long expiryMs = in.readLong();
                restore(key, value, expiryMs, result);
                result.snapshotKeysLoaded++;
            }
        } catch (EOFException e) {
            // Older writers could report more entries in the header than they wrote.
            log.warning("Snapshot " + path + " ended early; restored " + result.snapshotKeysLoaded + " entries");
        } catch (IOException e) {
            log.warning("Could not read snapshot " + path + ": " + e.getMessage());
            result.snapshotLoadFailed = true;
            result.skippedCorrupt++;
        }
    }

    private void replayAof(LoadResult result) {
        Path path = baseDir.resolve(PersistenceManager.AOF_FILENAME);
        if (!Files.exists(path)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    apply(AofRecord.parse(line), result);
                } catch (IllegalArgumentException e) {
                    log.warning("Skipping AOF line " + lineNumber + ": " + e.getMessage());
                    result.skippedCorrupt++;
                }
            }
        } catch (IOException e) {
            log.warning("Could not read AOF " + path + ": " + e.getMessage());
        }
    }

    private void apply(AofRecord record, LoadResult result) {
        switch (record.op()) {
            case SET -> {
                restore(record.key(), record.value(), record.expiryMs(), result);
                result.aofPutsReplayed++;
            }
            case DEL -> {
                cache.evict(record.key());
                result.aofDeletesReplayed++;
            }
            case FLUSH -> cache.clear();
        }
    }

    private void restore(String key, String value, long expiryMs, LoadResult result) {
        if (expiryMs == -1L) {
            cache.put(key, value);
            return;
        }
        long remaining = expiryMs - System.currentTimeMillis();
        if (remaining <= 0) {
            result.skippedExpired++;
            // A later AOF record may have expired a key the snapshot still had.
            cache.evict(key);
        } else if (cache instanceof TtlAware ttlAware) {
            ttlAware.put(key, value, remaining);
        } else {
            log.fine(() -> "Restoring '" + key + "' without its TTL; cache is not TtlAware");
            cache.put(key, value);
        }
    }

    private static String readString(DataInputStream in, int maxBytes) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxBytes) {
            throw new IOException("Implausible string length " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Lets the loader restore an entry together with its remaining TTL. */
    public interface TtlAware {

        /**
         * Stores a value that expires after {@code ttlMillis}.
         *
         * @param key       the key
         * @param value     the value
         * @param ttlMillis time to live in milliseconds, always positive
         */
        void put(String key, String value, long ttlMillis);
    }

    /** Outcome of {@link #load()}. */
    public static final class LoadResult {
        /** Entries read from the snapshot. */
        public int snapshotKeysLoaded;
        /** SET records replayed from the AOF. */
        public int aofPutsReplayed;
        /** DEL records replayed from the AOF. */
        public int aofDeletesReplayed;
        /** Entries skipped because they had expired. */
        public int skippedExpired;
        /** Unreadable AOF lines or snapshot entries. */
        public int skippedCorrupt;
        /** True if a snapshot existed but could not be used. */
        public boolean snapshotLoadFailed;

        @Override
        public String toString() {
            return String.format("LoadResult{snapshot=%d, aofPuts=%d, aofDeletes=%d, skippedExpired=%d, "
                            + "skippedCorrupt=%d, snapshotFailed=%b}",
                    snapshotKeysLoaded, aofPutsReplayed, aofDeletesReplayed,
                    skippedExpired, skippedCorrupt, snapshotLoadFailed);
        }
    }
}
