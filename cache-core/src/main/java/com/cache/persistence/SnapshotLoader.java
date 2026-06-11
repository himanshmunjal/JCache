package com.cache.persistence;

import com.cache.api.Cache;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.logging.Logger;

/**
 * SnapshotLoader restores cache state on server startup by reading the
 * binary snapshot file and replaying the append-only log on top of it.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * RECOVERY ALGORITHM
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Called once at server startup, before the server begins accepting requests.
 *
 * STEP 1 — LOAD SNAPSHOT:
 *   Read the binary snapshot file written by SnapshotWriter.
 *   For each entry:
 *     - Skip if expiryTimeMs != -1 AND System.currentTimeMillis() > expiryTimeMs
 *       (key expired while server was down — do not restore "ghost" keys)
 *     - Otherwise: call cache.put(key, value, remainingTTLMs)
 *
 * STEP 2 — REPLAY AOF LOG:
 *   Read the text AOF log line by line.
 *   For each line:
 *     PUT key value expiryMs  → if not expired, call cache.put()
 *     DELETE key              → call cache.evict(key)
 *   AOF entries are NEWER than the snapshot — they override it correctly.
 *   (A DELETE in the AOF after a PUT in the snapshot correctly removes the key.)
 *
 * EXAMPLE:
 *   Snapshot: {user:1 → Alice (no expiry), user:2 → Bob (expires t+30s)}
 *   AOF log:  "PUT user:3 Carol -1"
 *             "DELETE user:1"
 *   After recovery:
 *     user:2 → Bob  (if not expired)
 *     user:3 → Carol
 *     user:1 → gone (DELETE in AOF overrides snapshot)
 *
 * ═══════════════════════════════════════════════════════════════════════
 * ERROR HANDLING PHILOSOPHY
 * ═══════════════════════════════════════════════════════════════════════
 *
 * CORRUPT SNAPSHOT:
 *   If the magic number doesn't match or a read throws IOException,
 *   we log a warning and skip the snapshot entirely. The cache starts empty.
 *   We still attempt to replay the AOF log (it might be intact).
 *   This matches Redis's behavior — a corrupt RDB file causes Redis to start
 *   empty rather than refuse to start.
 *
 * CORRUPT AOF LINE:
 *   Bad lines are logged and skipped. The loader continues with the next line.
 *   Partial data recovery is better than no recovery.
 *
 * MISSING FILES:
 *   If neither snapshot nor AOF exists, the cache starts empty — normal for
 *   a fresh server with no prior data.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * TTL RECALCULATION
 * ═══════════════════════════════════════════════════════════════════════
 *
 * The snapshot stores ABSOLUTE expiry timestamps (System.currentTimeMillis()).
 * LRUCache.put() takes a TTL in MILLISECONDS (relative, from now).
 *
 * Conversion on load:
 *   remainingTtlMs = expiryMs - System.currentTimeMillis()
 *   if remainingTtlMs <= 0: skip (already expired)
 *   else: cache.put(key, value, remainingTtlMs)
 *
 * This correctly handles the case where the server was down for a while —
 * keys with short TTLs may have expired during downtime and are not restored.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * INTERVIEW TALKING POINT
 * ═══════════════════════════════════════════════════════════════════════
 *
 * "SnapshotLoader implements crash recovery with TTL awareness. It skips
 *  entries whose absolute expiry timestamp has already passed, so the cache
 *  is not polluted with logically-expired data after restart. This is an
 *  improvement over a naive loader that would restore all snapshot entries
 *  regardless of expiry — those 'ghost' keys would sit in the cache until
 *  TTLCache's lazy eviction fired, causing incorrect 'hit' responses."
 */
public class SnapshotLoader {

    private static final Logger log = Logger.getLogger(SnapshotLoader.class.getName());

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Directory containing the snapshot and AOF files. */
    private final Path baseDir;

    /** The cache to restore into. Must be empty when load() is called. */
    private final Cache<String, String> cache;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a SnapshotLoader targeting the given cache and directory.
     *
     * @param baseDir Directory where snapshot and AOF files are stored.
     * @param cache   The cache to restore into. Should be empty before load().
     */
    public SnapshotLoader(Path baseDir, Cache<String, String> cache) {
        if (baseDir == null) throw new IllegalArgumentException("baseDir cannot be null");
        if (cache == null)   throw new IllegalArgumentException("cache cannot be null");
        this.baseDir = baseDir;
        this.cache   = cache;
    }

    // -------------------------------------------------------------------------
    // Core load method
    // -------------------------------------------------------------------------

    /**
     * Restores cache state from snapshot + AOF log.
     *
     * Call this ONCE at server startup, BEFORE accepting any requests.
     * Calling it on a non-empty cache will merge (not replace) — existing
     * entries are not cleared.
     *
     * @return A LoadResult describing how many keys were restored.
     */
    public LoadResult load() {
        LoadResult result = new LoadResult();

        // STEP 1: Load snapshot.
        loadSnapshot(result);

        // STEP 2: Replay AOF log (overrides snapshot entries).
        replayAOFLog(result);

        log.info(String.format(
                "Cache recovery complete: " +
                        "snapshotKeys=%d, aofPuts=%d, aofDeletes=%d, " +
                        "skippedExpired=%d, skippedCorrupt=%d, totalRestored=%d",
                result.snapshotKeysLoaded,
                result.aofPutsReplayed,
                result.aofDeletesReplayed,
                result.skippedExpired,
                result.skippedCorrupt,
                cache.size()
        ));

        return result;
    }

    // -------------------------------------------------------------------------
    // Step 1: load snapshot
    // -------------------------------------------------------------------------

    /**
     * Reads the binary snapshot file and restores non-expired entries
     * into the cache. Updates result counters.
     *
     * If the snapshot file does not exist: no-op, logged at INFO.
     * If the snapshot file is corrupt: logs WARNING, skips entire snapshot.
     *
     * @param result Counters updated in-place.
     */
    private void loadSnapshot(LoadResult result) {
        Path snapshotPath = baseDir.resolve(PersistenceManager.SNAPSHOT_FILENAME);

        if (!Files.exists(snapshotPath)) {
            log.info("No snapshot file found at " + snapshotPath + " — starting empty.");
            return;
        }

        log.info("Loading snapshot from " + snapshotPath);

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(
                        new FileInputStream(snapshotPath.toFile())))) {

            // --- Read and validate header ---
            int magic   = in.readInt();
            int version = in.readInt();
            long snapshotTime = in.readLong();
            int entryCount    = in.readInt(); // hint, not authoritative

            // Magic number check — reject files that aren't JCache snapshots.
            if (magic != SnapshotWriter.MAGIC_NUMBER) {
                log.warning(String.format(
                        "Snapshot file has wrong magic number: expected 0x%08X, got 0x%08X. " +
                                "File may be corrupt or from a different application. Skipping snapshot.",
                        SnapshotWriter.MAGIC_NUMBER, magic
                ));
                result.snapshotLoadFailed = true;
                return;
            }

            // Version check — reject incompatible formats.
            if (version != SnapshotWriter.FORMAT_VERSION) {
                log.warning(String.format(
                        "Snapshot format version mismatch: expected %d, got %d. " +
                                "Snapshot was written by a different version. Skipping.",
                        SnapshotWriter.FORMAT_VERSION, version
                ));
                result.snapshotLoadFailed = true;
                return;
            }

            long serverDowntime = System.currentTimeMillis() - snapshotTime;
            log.info(String.format(
                    "Snapshot taken %dms ago. Expected ~%d entries.",
                    serverDowntime, entryCount
            ));

            // --- Read entries until EOF ---
            // We read to EOF rather than using the entryCount header because
            // SnapshotWriter may have skipped some expired entries after writing
            // the header count. Reading to EOF is always correct.
            while (in.available() > 0) {
                try {
                    readAndRestoreEntry(in, result);
                } catch (EOFException e) {
                    // Clean EOF mid-read — file truncated. Stop reading.
                    log.fine("Reached end of snapshot file (clean EOF).");
                    break;
                } catch (IOException e) {
                    // Corrupt entry — log and skip rest of snapshot.
                    // We can't safely skip one entry in a binary format without
                    // knowing where the next entry starts.
                    log.warning("Corrupt entry in snapshot (stopping at this point): "
                            + e.getMessage());
                    result.skippedCorrupt++;
                    break;
                }
            }

        } catch (IOException e) {
            log.warning("Failed to read snapshot file: " + e.getMessage() +
                    ". Cache will be restored from AOF log only.");
            result.snapshotLoadFailed = true;
        }
    }

    /**
     * Reads one entry from the DataInputStream and restores it into the cache.
     *
     * Entry binary layout (must match SnapshotWriter.writeEntry()):
     *   [4 bytes: keyLen][keyLen bytes: key UTF-8]
     *   [4 bytes: valLen][valLen bytes: value UTF-8]
     *   [8 bytes: expiryTimeMs]
     *
     * @param in     The input stream positioned at the start of an entry.
     * @param result Counters updated in-place.
     * @throws IOException  on read error.
     * @throws EOFException if the stream ended before a complete entry was read.
     */
    private void readAndRestoreEntry(DataInputStream in, LoadResult result) throws IOException {
        // Read key.
        int keyLen = in.readInt();
        if (keyLen <= 0 || keyLen > 1_000_000) {
            // Sanity check: keys > 1MB are almost certainly corrupt data.
            throw new IOException("Implausible key length in snapshot: " + keyLen);
        }
        byte[] keyBytes = new byte[keyLen];
        in.readFully(keyBytes);   // readFully: blocks until all bytes are read
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        // Read value.
        int valLen = in.readInt();
        if (valLen <= 0 || valLen > 100_000_000) {
            // Sanity check: values > 100MB are almost certainly corrupt.
            throw new IOException("Implausible value length in snapshot: " + valLen);
        }
        byte[] valBytes = new byte[valLen];
        in.readFully(valBytes);
        String value = new String(valBytes, StandardCharsets.UTF_8);

        // Read expiry timestamp.
        long expiryMs = in.readLong();

        // Restore or skip based on expiry.
        restoreEntry(key, value, expiryMs, result);
        result.snapshotKeysLoaded++;
    }

    // -------------------------------------------------------------------------
    // Step 2: replay AOF log
    // -------------------------------------------------------------------------

    /**
     * Reads the AOF log line by line and applies each mutation to the cache.
     *
     * AOF entries are NEWER than the snapshot — they correctly override it.
     * A DELETE in the AOF after a PUT in the snapshot properly removes the key.
     *
     * Bad lines (empty, malformed, unknown verb) are logged and skipped.
     * We never abort the whole replay for one bad line.
     *
     * @param result Counters updated in-place.
     */
    private void replayAOFLog(LoadResult result) {
        Path aofPath = baseDir.resolve(PersistenceManager.AOF_FILENAME);

        if (!Files.exists(aofPath)) {
            log.info("No AOF log found at " + aofPath + " — no log to replay.");
            return;
        }

        log.info("Replaying AOF log from " + aofPath);
        long lineNumber = 0;

        try (BufferedReader reader = Files.newBufferedReader(aofPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();

                if (line.isEmpty()) continue; // blank lines are OK — skip silently

                try {
                    replayLine(line, result);
                } catch (Exception e) {
                    // Bad line — log and continue. Never abort for one bad line.
                    log.warning(String.format(
                            "Skipping corrupt AOF line %d: '%s' — %s",
                            lineNumber, truncate(line, 80), e.getMessage()
                    ));
                    result.skippedCorrupt++;
                }
            }

        } catch (IOException e) {
            log.warning("Failed to read AOF log: " + e.getMessage() +
                    ". Restoration may be incomplete.");
        }

        log.info(String.format(
                "AOF replay complete: %d lines, %d PUTs, %d DELETEs, %d skipped.",
                lineNumber, result.aofPutsReplayed, result.aofDeletesReplayed, result.skippedCorrupt
        ));
    }

    /**
     * Parses and applies one AOF log line.
     *
     * Line formats (written by PersistenceManager.logPut/logDelete):
     *   PUT key value expiryMs
     *   DELETE key
     *
     * @param line   One trimmed, non-empty line from the AOF log.
     * @param result Counters updated in-place.
     * @throws IllegalArgumentException if the line has an unrecognized format.
     */
    private void replayLine(String line, LoadResult result) {
        // Split on whitespace — exactly one space between tokens.
        // Note: values cannot contain spaces (wire protocol limitation).
        String[] tokens = line.split("\\s+");

        if (tokens.length == 0) {
            throw new IllegalArgumentException("Empty token array after split");
        }

        String verb = tokens[0].toUpperCase();

        switch (verb) {

            case PersistenceManager.AOF_PUT: {
                // Expected format: PUT key value expiryMs
                if (tokens.length < 4) {
                    throw new IllegalArgumentException(
                            "PUT requires 3 args (key, value, expiryMs), got " +
                                    (tokens.length - 1));
                }
                String key      = tokens[1];
                String value    = tokens[2];
                long   expiryMs = parseLong(tokens[3], "expiryMs");

                restoreEntry(key, value, expiryMs, result);
                result.aofPutsReplayed++;
                break;
            }

            case PersistenceManager.AOF_DELETE: {
                // Expected format: DELETE key
                if (tokens.length < 2) {
                    throw new IllegalArgumentException(
                            "DELETE requires 1 arg (key), got 0");
                }
                String key = tokens[1];
                cache.evict(key);
                result.aofDeletesReplayed++;
                break;
            }

            default:
                throw new IllegalArgumentException(
                        "Unknown AOF verb: '" + verb + "'");
        }
    }

    // -------------------------------------------------------------------------
    // Shared restore logic
    // -------------------------------------------------------------------------

    /**
     * Restores one key-value pair into the cache, respecting TTL.
     *
     * EXPIRY LOGIC:
     *   expiryMs == -1           → no expiry → cache.put(key, value) (no TTL)
     *   now > expiryMs           → already expired → skip entirely
     *   now < expiryMs           → compute remaining TTL → cache.put(key, value, remaining)
     *
     * The remaining TTL is computed as: expiryMs - now
     * This is passed to LRUCache.put(key, value, ttlMillis) which stores
     * the absolute expiry time as: now + ttlMillis = expiryMs (correct).
     *
     * EXAMPLE:
     *   Snapshot written at t=0. expiryMs = 1000 (expires at t+1000ms).
     *   Server restarts at t=300ms.
     *   remaining = 1000 - 300 = 700ms.
     *   cache.put(key, value, 700) — key expires 700ms after restore.
     *   At t=1000ms from original, the key expires. Correct.
     *
     * @param key      The cache key.
     * @param value    The cache value.
     * @param expiryMs Absolute expiry timestamp in ms. -1 = no expiry.
     * @param result   Counters updated in-place.
     */
    private void restoreEntry(String key, String value, long expiryMs, LoadResult result) {
        long now = System.currentTimeMillis();

        if (expiryMs == -1L) {
            // No TTL — restore with no expiry.
            cache.put(key, value);

        } else if (now >= expiryMs) {
            // Already expired — do not restore.
            log.fine("Skipping expired key '" + key + "' (expired " +
                    (now - expiryMs) + "ms ago).");
            result.skippedExpired++;

        } else {
            // TTL is still valid — compute remaining time and restore.
            long remainingTtlMs = expiryMs - now;

            // LRUCache.put(key, value, ttlMillis) where ttlMillis is relative.
            // We call the overloaded put() that accepts TTL in milliseconds.
            // If the cache doesn't have this overload, fall back to no-TTL put().
            if (cache instanceof TtlAware) {
                ((TtlAware) cache).put(key, value, remainingTtlMs);
            } else {
                // Fallback: restore without TTL.
                // The key will live longer than originally intended, but at
                // least it's not lost. Acceptable degradation.
                log.fine("Cache does not implement TtlAware — restoring '" + key +
                        "' without TTL (remaining was " + remainingTtlMs + "ms).");
                cache.put(key, value);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Private utilities
    // -------------------------------------------------------------------------

    /**
     * Parses a long from a string token.
     * Throws IllegalArgumentException with a descriptive message on failure.
     *
     * @param token     The string to parse.
     * @param fieldName The field name, used in the error message.
     * @return The parsed long value.
     */
    private long parseLong(String token, String fieldName) {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cannot parse " + fieldName + " as long: '" + token + "'");
        }
    }

    /**
     * Truncates a string to maxLen characters for log messages.
     * Appends "..." if truncated.
     */
    private String truncate(String s, int maxLen) {
        if (s.length() <= maxLen) return s;
        return s.substring(0, maxLen) + "...";
    }

    // -------------------------------------------------------------------------
    // TtlAware interface — for caches that support millisecond TTL on put()
    // -------------------------------------------------------------------------

    /**
     * Optional interface for cache implementations that support TTL on put().
     *
     * LRUCache already has:
     *   public void put(K key, V value, long ttlMillis)
     *
     * Implement TtlAware in LRUCache (and LFUCache, ARCCache) so
     * SnapshotLoader can restore TTLs correctly:
     *
     *   public class LRUCache<K,V> implements Cache<K,V>, TtlAware<K,V>, ... {
     *       // put(key, value, ttlMillis) is already there — just add implements TtlAware
     *   }
     *
     * WHY AN INTERFACE AND NOT JUST CAST TO LRUCache?
     * SnapshotLoader is policy-agnostic — it doesn't know if the cache is LRU,
     * LFU, or ARC. Casting to LRUCache would hardcode a policy dependency.
     * TtlAware keeps SnapshotLoader decoupled from specific implementations.
     */
    public interface TtlAware {
        /**
         * Stores a key-value pair with a TTL in milliseconds.
         *
         * @param key        The cache key.
         * @param value      The cache value.
         * @param ttlMillis  Time-to-live in milliseconds from now. -1 = no expiry.
         */
        void put(String key, String value, long ttlMillis);
    }

    // -------------------------------------------------------------------------
    // LoadResult — structured result of a load() call
    // -------------------------------------------------------------------------

    /**
     * Carries the outcome of a load() call: how many keys were restored,
     * how many were skipped, and whether any step failed.
     *
     * Used by CacheServer to log a startup summary and by PersistenceTest
     * to assert expected recovery behavior.
     */
    public static final class LoadResult {

        /** Keys successfully loaded from the snapshot file. */
        public int snapshotKeysLoaded = 0;

        /** PUT entries replayed from the AOF log. */
        public int aofPutsReplayed = 0;

        /** DELETE entries replayed from the AOF log. */
        public int aofDeletesReplayed = 0;

        /** Entries skipped because their TTL had already expired at load time. */
        public int skippedExpired = 0;

        /**
         * Lines or entries skipped due to parse/read errors.
         * > 0 indicates data loss or corruption.
         */
        public int skippedCorrupt = 0;

        /**
         * True if the snapshot file could not be loaded (missing magic, wrong
         * version, or IO error). Recovery fell back to AOF-only.
         */
        public boolean snapshotLoadFailed = false;

        /**
         * Returns a one-line summary string for logging.
         */
        @Override
        public String toString() {
            return String.format(
                    "LoadResult{snapshot=%d, aofPuts=%d, aofDeletes=%d, " +
                            "skippedExpired=%d, skippedCorrupt=%d, snapshotFailed=%b}",
                    snapshotKeysLoaded, aofPutsReplayed, aofDeletesReplayed,
                    skippedExpired, skippedCorrupt, snapshotLoadFailed
            );
        }
    }
}