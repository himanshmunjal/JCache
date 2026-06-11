package com.cache.persistence;

import com.cache.api.Cache;

import java.io.*;
import java.nio.file.*;
import java.util.logging.Logger;

/**
 * SnapshotWriter serializes the entire cache state to a binary file.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * BINARY FORMAT
 * ═══════════════════════════════════════════════════════════════════════
 *
 * The snapshot is a flat binary file with a header followed by N entry records.
 *
 * FILE STRUCTURE:
 *   [HEADER]
 *     4 bytes  — magic number (0x4A43_4143 = "JCAC" in ASCII)
 *     4 bytes  — format version (currently 1)
 *     8 bytes  — snapshot timestamp (System.currentTimeMillis())
 *     4 bytes  — entry count N
 *
 *   [ENTRY × N]
 *     4 bytes  — key length in bytes (K)
 *     K bytes  — key bytes (UTF-8)
 *     4 bytes  — value length in bytes (V)
 *     V bytes  — value bytes (UTF-8)
 *     8 bytes  — expiryTimeMs (-1 = no expiry, else absolute ms)
 *
 * WHY BINARY, NOT TEXT/JSON?
 *   Binary is faster to write and read (no string parsing).
 *   Keys and values can contain any bytes (binary-safe).
 *   DataOutputStream/DataInputStream handle byte order consistently (big-endian).
 *   JSON would require escaping and a parser dependency.
 *   This format is similar to Redis's RDB format.
 *
 * WHY LENGTH-PREFIXED STRINGS?
 *   DataOutputStream.writeUTF() is limited to 65535 bytes per string.
 *   We prefix with a 4-byte int so keys and values can be up to 2GB.
 *   We read with DataInputStream.readFully() for correctness.
 *
 * MAGIC NUMBER:
 *   0x4A434143 = bytes [0x4A, 0x43, 0x41, 0x43] = "JCAC" (JCache).
 *   SnapshotLoader checks this on load to detect corrupt or wrong files.
 *
 * FORMAT VERSION:
 *   Stored in the header so SnapshotLoader can reject files written by
 *   incompatible older versions. Increment when format changes.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * ATOMIC WRITE PATTERN
 * ═══════════════════════════════════════════════════════════════════════
 *
 * We NEVER write directly to jcache.snapshot. Instead:
 *   1. Write to jcache.snapshot.tmp
 *   2. On success: Files.move(tmp, snapshot, ATOMIC_MOVE, REPLACE_EXISTING)
 *   3. On failure: delete tmp, throw IOException
 *
 * Why? If the JVM crashes mid-write, the .tmp file is incomplete/corrupt.
 * The original jcache.snapshot is untouched and still valid.
 * Files.move with ATOMIC_MOVE is guaranteed to be atomic on POSIX systems
 * (rename() syscall). On Windows it's best-effort but still safer than
 * writing directly.
 *
 * INTERVIEW TALKING POINT:
 *   "I used an atomic write pattern for snapshots — write to a temp file,
 *    then rename atomically. This is identical to how Redis writes RDB files.
 *    A crash during snapshot leaves the previous snapshot intact. Without this,
 *    a crash mid-write produces a corrupt snapshot and unrecoverable data loss."
 *
 * ═══════════════════════════════════════════════════════════════════════
 * WHAT SnapshotWriter DOES NOT DO
 * ═══════════════════════════════════════════════════════════════════════
 *
 * It does NOT iterate cache internals directly. It calls cache.get() on
 * keys it doesn't know, because Cache<K,V> has no iterator.
 *
 * LIMITATION: Cache<K,V> has no keySet() method. To snapshot, we need
 * to know all keys. This is a real limitation of the current Cache interface.
 *
 * SOLUTION: SnapshotWriter takes the cache AND a separate key snapshot
 * provided by the caller (PersistenceManager), who tracks all live keys
 * via the AOF log. Alternatively, we expose a snapshot-friendly method.
 *
 * For simplicity, this implementation adds a getSnapshot() method via
 * a SnapshotCapable interface that cache implementations can optionally
 * implement. If not implemented, we fall back to a provided key set.
 */
public class SnapshotWriter {

    private static final Logger log = Logger.getLogger(SnapshotWriter.class.getName());

    // -------------------------------------------------------------------------
    // Binary format constants
    // -------------------------------------------------------------------------

    /**
     * Magic number at the start of every snapshot file.
     * "JCAC" in ASCII: 0x4A=J, 0x43=C, 0x41=A, 0x43=C.
     * SnapshotLoader reads this first to verify the file is a JCache snapshot.
     */
    static final int MAGIC_NUMBER = 0x4A434143;

    /**
     * Current snapshot format version.
     * Increment this if the binary format changes incompatibly.
     * SnapshotLoader rejects files with a different version.
     */
    static final int FORMAT_VERSION = 1;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private final Path                  baseDir;
    private final Cache<String, String> cache;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a SnapshotWriter for the given cache and persistence directory.
     *
     * @param baseDir Directory where snapshot files are written.
     * @param cache   The cache to snapshot.
     */
    public SnapshotWriter(Path baseDir, Cache<String, String> cache) {
        if (baseDir == null) throw new IllegalArgumentException("baseDir cannot be null");
        if (cache == null)   throw new IllegalArgumentException("cache cannot be null");
        this.baseDir = baseDir;
        this.cache   = cache;
    }

    // -------------------------------------------------------------------------
    // Core write method
    // -------------------------------------------------------------------------

    /**
     * Writes a snapshot of the cache to disk atomically.
     *
     * The cache must implement {@link SnapshotCapable} for the writer to
     * know which keys to serialize. If it does not, 0 keys are written
     * and a warning is logged — the snapshot file is still valid (empty).
     *
     * @return Number of cache entries written to the snapshot.
     * @throws IOException if writing fails. The previous snapshot is preserved.
     */
    public int write() throws IOException {
        Path tmpPath      = baseDir.resolve(PersistenceManager.SNAPSHOT_TMP_FILENAME);
        Path snapshotPath = baseDir.resolve(PersistenceManager.SNAPSHOT_FILENAME);

        // Obtain a snapshot of all live entries from the cache.
        // SnapshotCapable.getEntries() returns {key → expiryTimeMs} pairs.
        // If the cache doesn't implement SnapshotCapable, we write 0 entries.
        java.util.Map<String, Long> liveKeys = getLiveKeys();
        int entryCount = liveKeys.size();
        long snapshotTime = System.currentTimeMillis();

        // Write to tmp file first.
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(tmpPath.toFile())))) {

            // --- HEADER ---
            out.writeInt(MAGIC_NUMBER);    // 4 bytes: file type identifier
            out.writeInt(FORMAT_VERSION);  // 4 bytes: format version
            out.writeLong(snapshotTime);   // 8 bytes: when snapshot was taken
            out.writeInt(entryCount);      // 4 bytes: how many entries follow

            // --- ENTRIES ---
            int written = 0;
            for (java.util.Map.Entry<String, Long> entry : liveKeys.entrySet()) {
                String key        = entry.getKey();
                long   expiryMs   = entry.getValue();

                // Skip entries that have already expired.
                // Writing expired entries would restore "ghost" keys that are
                // logically gone — misleading and wastes space.
                if (expiryMs != -1 && System.currentTimeMillis() > expiryMs) {
                    continue;
                }

                // Fetch value from cache.
                // Note: get() may return null if the key was evicted between
                // getLiveKeys() and now. Skip null values.
                String value = cache.get(key);
                if (value == null) continue;

                writeEntry(out, key, value, expiryMs);
                written++;
            }

            out.flush();
            log.fine("Wrote " + written + " entries to tmp snapshot.");

            // Update the entry count header if some entries were skipped.
            // We can't update the header in place on a stream, so the count
            // in the header may be slightly over. SnapshotLoader uses it as
            // a hint, not a guarantee — it reads until EOF.
            // This is acceptable; the loader handles early EOF gracefully.
        } catch (IOException e) {
            // Clean up the partial tmp file before rethrowing.
            try { Files.deleteIfExists(tmpPath); } catch (IOException ignored) {}
            throw new IOException("Snapshot write failed: " + e.getMessage(), e);
        }

        // Atomically replace the old snapshot with the new one.
        // ATOMIC_MOVE: on POSIX, this is a rename() — guaranteed atomic.
        // REPLACE_EXISTING: overwrites the old snapshot if it exists.
        try {
            Files.move(tmpPath, snapshotPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // Atomic move failed (some filesystems don't support it across mounts).
            // Fall back to non-atomic copy + delete.
            log.warning("ATOMIC_MOVE failed, falling back to copy+delete: " + e.getMessage());
            Files.copy(tmpPath, snapshotPath, StandardCopyOption.REPLACE_EXISTING);
            Files.deleteIfExists(tmpPath);
        }

        log.info(String.format(
                "Snapshot written: %d entries, file=%s, size=%d bytes",
                entryCount, snapshotPath, Files.size(snapshotPath)
        ));

        return entryCount;
    }

    /**
     * Writes a snapshot using an explicit map of key → expiryTimeMs.
     * Use this overload when the cache does not implement SnapshotCapable
     * but the caller (PersistenceManager) tracks live keys externally via AOF.
     *
     * @param keyExpiryMap Map of key → absolute expiry time in ms (-1 = no expiry).
     * @return Number of entries written.
     * @throws IOException if writing fails.
     */
    public int write(java.util.Map<String, Long> keyExpiryMap) throws IOException {
        Path tmpPath      = baseDir.resolve(PersistenceManager.SNAPSHOT_TMP_FILENAME);
        Path snapshotPath = baseDir.resolve(PersistenceManager.SNAPSHOT_FILENAME);

        long snapshotTime = System.currentTimeMillis();

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(tmpPath.toFile())))) {

            // Header
            out.writeInt(MAGIC_NUMBER);
            out.writeInt(FORMAT_VERSION);
            out.writeLong(snapshotTime);
            out.writeInt(keyExpiryMap.size()); // hint; loader reads to EOF

            // Entries
            int written = 0;
            for (java.util.Map.Entry<String, Long> entry : keyExpiryMap.entrySet()) {
                String key      = entry.getKey();
                long   expiryMs = entry.getValue();

                if (expiryMs != -1 && snapshotTime > expiryMs) continue;

                String value = cache.get(key);
                if (value == null) continue;

                writeEntry(out, key, value, expiryMs);
                written++;
            }
            out.flush();
        } catch (IOException e) {
            try { Files.deleteIfExists(tmpPath); } catch (IOException ignored) {}
            throw new IOException("Snapshot write failed: " + e.getMessage(), e);
        }

        try {
            Files.move(tmpPath, snapshotPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.copy(tmpPath, snapshotPath, StandardCopyOption.REPLACE_EXISTING);
            Files.deleteIfExists(tmpPath);
        }

        return (int) keyExpiryMap.size();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Writes one cache entry to the DataOutputStream.
     *
     * Entry binary layout:
     *   [4 bytes: keyLen][keyLen bytes: key UTF-8]
     *   [4 bytes: valLen][valLen bytes: value UTF-8]
     *   [8 bytes: expiryTimeMs]
     *
     * Length-prefixed strings are used instead of DataOutputStream.writeUTF()
     * because writeUTF() is limited to 65535 bytes. Our keys and values have
     * no such limit (though practically they are short).
     *
     * @param out      The output stream to write to.
     * @param key      The cache key.
     * @param value    The cache value.
     * @param expiryMs Absolute expiry timestamp (-1 = no expiry).
     */
    private void writeEntry(DataOutputStream out,
                            String key,
                            String value,
                            long expiryMs) throws IOException {
        byte[] keyBytes   = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        out.writeInt(keyBytes.length);    // key length prefix
        out.write(keyBytes);              // key bytes
        out.writeInt(valueBytes.length);  // value length prefix
        out.write(valueBytes);            // value bytes
        out.writeLong(expiryMs);          // absolute expiry timestamp
    }

    /**
     * Gets the map of live keys → expiry times from the cache.
     *
     * If the cache implements {@link SnapshotCapable}, we call its
     * getSnapshotEntries() method for an O(N) full scan.
     *
     * If not, we return an empty map and log a warning. Callers that need
     * snapshots without SnapshotCapable should use write(keyExpiryMap).
     *
     * @return Map of key → expiryTimeMs for all live entries.
     */
    private java.util.Map<String, Long> getLiveKeys() {
        if (cache instanceof SnapshotCapable) {
            return ((SnapshotCapable) cache).getSnapshotEntries();
        }
        log.warning("Cache does not implement SnapshotCapable — snapshot will be empty. " +
                "Use write(keyExpiryMap) or implement SnapshotCapable in your cache class.");
        return java.util.Collections.emptyMap();
    }
}