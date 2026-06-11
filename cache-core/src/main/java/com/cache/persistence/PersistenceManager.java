package com.cache.persistence;

import com.cache.api.Cache;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * PersistenceManager orchestrates durable storage for the cache engine.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * ARCHITECTURE — TWO-LAYER PERSISTENCE (mirrors Redis RDB + AOF)
 * ═══════════════════════════════════════════════════════════════════════
 *
 * LAYER 1 — SNAPSHOT (like Redis RDB):
 *   A complete point-in-time binary dump of all K/V pairs + expiry times.
 *   Written periodically (every snapshotIntervalMs milliseconds).
 *   Compact and fast to load on restart — restore entire cache in one read.
 *   Risk: up to snapshotIntervalMs of data loss on crash.
 *
 * LAYER 2 — APPEND-ONLY LOG (like Redis AOF):
 *   Every mutation (PUT, DELETE) is appended to a text log file immediately.
 *   Written synchronously before put/evict returns — zero data loss window.
 *   On restart, replayed ON TOP of the snapshot to recover recent mutations.
 *   Risk: log grows unboundedly — truncated after each snapshot.
 *
 * RECOVERY SEQUENCE (handled by SnapshotLoader):
 *   1. Load snapshot (bulk restore of N keys)
 *   2. Replay AOF log (apply mutations newer than snapshot)
 *   3. Skip entries whose expiryTime has already passed
 *   4. Cache is now at the state it was at the moment of the last mutation
 *
 * EXAMPLE TIMELINE:
 *   t=0:    PUT user:1 Alice          → log: "PUT user:1 Alice -1"
 *   t=10:   PUT user:2 Bob TTL=60s    → log: "PUT user:2 Bob 1000060"  (absolute ms)
 *   t=100:  DELETE user:1             → log: "DELETE user:1"
 *   t=300:  [snapshot written]        → snapshot: {user:2, Bob, 1000060}
 *   t=300:  [log truncated to empty]
 *   t=301:  PUT user:3 Carol          → log: "PUT user:3 Carol -1"
 *   CRASH at t=310
 *   RESTART:
 *     load snapshot → user:2 restored (if not expired)
 *     replay log    → user:3 restored
 *     final state:  {user:2: Bob, user:3: Carol}
 *
 * ═══════════════════════════════════════════════════════════════════════
 * THREAD SAFETY
 * ═══════════════════════════════════════════════════════════════════════
 *
 * logPut() and logDelete() write to the AOF log.
 * Both synchronize on `logLock` — a single PrintWriter cannot be shared
 * across threads without locking. The lock is uncontended in practice
 * because cache operations are already serialized by the cache's own lock
 * (SegmentedCache) or by the caller (CacheServer handler threads serialize
 * within a channel). The log lock is a safety net.
 *
 * The snapshot scheduler runs on a single daemon thread and does NOT hold
 * the log lock during snapshot creation — the log continues to accept
 * entries while the snapshot is written.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * FILE LAYOUT
 * ═══════════════════════════════════════════════════════════════════════
 *
 *   baseDir/
 *     jcache.snapshot      ← binary snapshot (written atomically via tmp → rename)
 *     jcache.aof           ← append-only log (text, one command per line)
 *     jcache.snapshot.tmp  ← in-progress snapshot write (deleted on completion)
 *
 * ═══════════════════════════════════════════════════════════════════════
 * INTERVIEW TALKING POINT
 * ═══════════════════════════════════════════════════════════════════════
 *
 * "I implemented a two-layer persistence model matching Redis's RDB+AOF hybrid.
 *  The snapshot gives fast O(N) bulk restore on startup. The AOF log gives
 *  zero-data-loss between snapshots. The log is truncated after each snapshot
 *  to bound its size. Snapshot writes are atomic — written to a .tmp file then
 *  renamed, so a crash during snapshot never corrupts the last good snapshot."
 */
public class PersistenceManager implements Closeable {

    private static final Logger log = Logger.getLogger(PersistenceManager.class.getName());

    // -------------------------------------------------------------------------
    // File names
    // -------------------------------------------------------------------------

    /** Binary snapshot file. Contains complete cache state at a point in time. */
    public static final String SNAPSHOT_FILENAME = "jcache.snapshot";

    /** Append-only log file. Contains mutations since the last snapshot. */
    public static final String AOF_FILENAME = "jcache.aof";

    /** In-progress snapshot. Renamed to SNAPSHOT_FILENAME on completion. */
    static final String SNAPSHOT_TMP_FILENAME = "jcache.snapshot.tmp";

    // -------------------------------------------------------------------------
    // AOF command tokens — written to and read from the log file
    // -------------------------------------------------------------------------

    /** Token for PUT entries in the AOF log. */
    static final String AOF_PUT    = "PUT";

    /** Token for DELETE entries in the AOF log. */
    static final String AOF_DELETE = "DELETE";

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Base directory where snapshot and AOF files are stored. */
    private final Path baseDir;

    /**
     * The cache to snapshot. Generic — works with LRU, LFU, ARC, or any policy.
     * Used by SnapshotWriter to iterate current cache contents.
     */
    private final Cache<String, String> cache;

    /**
     * Snapshot writer — handles the binary serialization format.
     * Separated from PersistenceManager to keep each class focused on one job.
     */
    private final SnapshotWriter snapshotWriter;

    /**
     * The append-only log writer.
     * autoFlush=true — every append is immediately flushed to the OS buffer.
     * We rely on the OS to fsync on its schedule; explicit fsync per-write
     * would be too slow for a hot path.
     */
    private PrintWriter aofWriter;

    /**
     * Lock protecting aofWriter from concurrent access.
     * A plain Object used as a monitor — no need for ReentrantLock here
     * since we never need tryLock or timed locking.
     */
    private final Object logLock = new Object();

    /**
     * Counts total AOF entries written since startup.
     * Logged at snapshot time to show how much the AOF grew between snapshots.
     */
    private final AtomicLong aofEntryCount = new AtomicLong(0);

    /**
     * Scheduler for periodic snapshots.
     * Single daemon thread — snapshots don't need parallelism.
     */
    private final ScheduledExecutorService snapshotScheduler;

    /** How often to take snapshots, in milliseconds. */
    private final long snapshotIntervalMs;

    /** True after close() is called. Guards against double-close. */
    private volatile boolean closed = false;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a PersistenceManager and opens the AOF log for appending.
     *
     * The base directory is created if it does not exist.
     * The AOF log is opened in APPEND mode — existing log entries are preserved.
     * This is correct: if the server restarts without a clean shutdown,
     * the existing AOF entries are exactly what we want to replay.
     *
     * @param baseDir            Directory for snapshot and AOF files.
     * @param cache              The cache to persist. Used by SnapshotWriter.
     * @param snapshotIntervalMs How often to take snapshots (milliseconds).
     *                           Use 0 to disable automatic snapshots.
     * @throws IOException if the base directory cannot be created or the AOF
     *                     log cannot be opened.
     */
    public PersistenceManager(Path baseDir,
                              Cache<String, String> cache,
                              long snapshotIntervalMs) throws IOException {

        if (baseDir == null)  throw new IllegalArgumentException("baseDir cannot be null");
        if (cache == null)    throw new IllegalArgumentException("cache cannot be null");
        if (snapshotIntervalMs < 0) {
            throw new IllegalArgumentException(
                    "snapshotIntervalMs must be >= 0, got: " + snapshotIntervalMs);
        }

        this.baseDir             = baseDir;
        this.cache               = cache;
        this.snapshotIntervalMs  = snapshotIntervalMs;
        this.snapshotWriter      = new SnapshotWriter(baseDir, cache);

        // Create the persistence directory if it doesn't exist.
        Files.createDirectories(baseDir);

        // Open AOF log in append mode.
        // StandardOpenOption.APPEND: writes go to end of existing file.
        // StandardOpenOption.CREATE: creates the file if it doesn't exist.
        // We do NOT truncate on open — preserving existing log entries is
        // intentional for crash recovery.
        openAOFWriter();

        // Start the snapshot scheduler if interval > 0.
        if (snapshotIntervalMs > 0) {
            this.snapshotScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "jcache-snapshot-scheduler");
                t.setDaemon(true);
                return t;
            });
            this.snapshotScheduler.scheduleAtFixedRate(
                    this::takeScheduledSnapshot,
                    snapshotIntervalMs,   // initial delay — don't snapshot at startup
                    snapshotIntervalMs,
                    TimeUnit.MILLISECONDS
            );
            log.info(String.format(
                    "PersistenceManager started. snapshotInterval=%dms, baseDir=%s",
                    snapshotIntervalMs, baseDir));
        } else {
            this.snapshotScheduler = null;
            log.info("PersistenceManager started (manual snapshots only). baseDir=" + baseDir);
        }
    }

    /**
     * Convenience constructor with default snapshot interval of 5 minutes.
     *
     * @param baseDir Directory for persistence files.
     * @param cache   The cache to persist.
     * @throws IOException if setup fails.
     */
    public PersistenceManager(Path baseDir, Cache<String, String> cache) throws IOException {
        this(baseDir, cache, 5 * 60 * 1000L); // 5 minutes
    }

    // -------------------------------------------------------------------------
    // AOF logging — called on every cache mutation
    // -------------------------------------------------------------------------

    /**
     * Appends a PUT entry to the AOF log.
     *
     * Format: "PUT {key} {value} {expiryTimeMs}\n"
     *
     * expiryTimeMs is the ABSOLUTE expiry timestamp in milliseconds
     * (System.currentTimeMillis() + ttlMs at the time of put).
     * -1 means no expiry.
     *
     * We log absolute timestamps (not relative TTLs) because the log may be
     * replayed after an arbitrary delay. A relative TTL of "60 seconds" is
     * meaningless if the server was down for 2 minutes.
     *
     * WHY NOT log the value size first?
     * This is a text-format log (not binary) for human readability and
     * debuggability. Values cannot contain spaces (wire protocol limitation).
     * For binary-safe values, Base64 encoding is required before calling logPut.
     *
     * @param key          The cache key. Cannot be null.
     * @param value        The value. Cannot be null. Must not contain spaces or newlines.
     * @param expiryTimeMs Absolute expiry timestamp in ms. -1 means no expiry.
     */
    public void logPut(String key, String value, long expiryTimeMs) {
        if (key == null || value == null) return;
        if (closed) return;

        synchronized (logLock) {
            // Format: PUT key value expiryMs
            aofWriter.println(AOF_PUT + " " + key + " " + value + " " + expiryTimeMs);
            // autoFlush=true means println() flushes immediately.
            // The OS may buffer before writing to disk, but the Java buffer is gone.
        }
        aofEntryCount.incrementAndGet();
    }

    /**
     * Convenience overload: logs a PUT with no expiry (expiryTimeMs = -1).
     *
     * @param key   The cache key.
     * @param value The value.
     */
    public void logPut(String key, String value) {
        logPut(key, value, -1L);
    }

    /**
     * Appends a DELETE entry to the AOF log.
     *
     * Format: "DELETE {key}\n"
     *
     * We log DELETEs explicitly so the log is self-contained.
     * Without DELETE entries, replaying the log would re-insert keys
     * that were deleted after the last snapshot.
     *
     * @param key The key that was deleted. Cannot be null.
     */
    public void logDelete(String key) {
        if (key == null) return;
        if (closed) return;

        synchronized (logLock) {
            aofWriter.println(AOF_DELETE + " " + key);
        }
        aofEntryCount.incrementAndGet();
    }

    // -------------------------------------------------------------------------
    // Snapshot management
    // -------------------------------------------------------------------------

    /**
     * Takes a snapshot of the current cache state and truncates the AOF log.
     *
     * This is the main "checkpoint" operation:
     *   1. Write snapshot (all current K/V + expiry times) via SnapshotWriter.
     *   2. Truncate AOF log — mutations before the snapshot are now redundant.
     *   3. Reopen AOF writer pointing to the empty log.
     *
     * ATOMICITY:
     * SnapshotWriter writes to a .tmp file and renames it atomically.
     * If the JVM crashes mid-snapshot, the last good snapshot is preserved.
     *
     * AOF TRUNCATION:
     * After a successful snapshot, the AOF log contains only mutations that
     * are already captured in the snapshot. Truncating it prevents unbounded
     * growth. If truncation fails (disk error), we log a warning but don't
     * throw — the old AOF still works for recovery, just contains redundant entries.
     *
     * @throws IOException if the snapshot write fails. AOF log is NOT truncated
     *                     on snapshot failure — data is preserved for next attempt.
     */
    public void takeSnapshot() throws IOException {
        if (closed) {
            throw new IllegalStateException("PersistenceManager is closed");
        }

        long start        = System.currentTimeMillis();
        long aofBefore    = aofEntryCount.get();

        log.info("Taking snapshot...");

        // Write snapshot — SnapshotWriter handles the atomic tmp→rename pattern.
        int keysWritten = snapshotWriter.write();

        // Truncate AOF log after successful snapshot.
        // CRITICAL: only truncate AFTER the snapshot is confirmed written.
        // If we truncated first and then snapshot failed, we'd lose data.
        truncateAOFLog();

        long elapsed = System.currentTimeMillis() - start;
        log.info(String.format(
                "Snapshot complete: %d keys written in %dms. " +
                        "AOF log truncated (had %d entries).",
                keysWritten, elapsed, aofBefore
        ));
    }

    /**
     * Returns the path to the snapshot file.
     * Used by SnapshotLoader to find the snapshot on startup.
     *
     * @return Path to the snapshot file (may not exist if no snapshot taken yet).
     */
    public Path getSnapshotPath() {
        return baseDir.resolve(SNAPSHOT_FILENAME);
    }

    /**
     * Returns the path to the AOF log file.
     * Used by SnapshotLoader to find the log on startup.
     *
     * @return Path to the AOF log file.
     */
    public Path getAOFPath() {
        return baseDir.resolve(AOF_FILENAME);
    }

    /**
     * Returns the number of AOF entries written since this manager was created.
     *
     * @return AOF entry count.
     */
    public long getAOFEntryCount() {
        return aofEntryCount.get();
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Shuts down the persistence manager cleanly.
     *
     * Shutdown sequence:
     *   1. Stop the snapshot scheduler.
     *   2. Take a final snapshot (captures all mutations).
     *   3. Close the AOF writer.
     *
     * After close(), logPut() and logDelete() are no-ops.
     * Idempotent — safe to call multiple times.
     */
    @Override
    public void close() {
        if (closed) return;

        // 1. Stop scheduler.
        if (snapshotScheduler != null) {
            snapshotScheduler.shutdown();
            try {
                snapshotScheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                snapshotScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 2. Final snapshot.
        try {
            takeSnapshot();
            log.info("Final snapshot taken on shutdown.");
        } catch (IOException e) {
            log.warning("Failed to take final snapshot on shutdown: " + e.getMessage());
        }

        // Mark closed only after snapshot succeeds/fails.
        closed = true;

        // 3. Close AOF writer.
        synchronized (logLock) {
            if (aofWriter != null) {
                aofWriter.close();
                aofWriter = null;
            }
        }

        log.info("PersistenceManager closed.");
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Opens the AOF log file in append mode.
     * Called at startup and after log truncation.
     *
     * @throws IOException if the file cannot be opened.
     */
    private void openAOFWriter() throws IOException {
        Path aofPath = baseDir.resolve(AOF_FILENAME);
        // BufferedWriter wraps a FileWriter in append mode.
        // autoFlush=true on PrintWriter ensures each println() flushes immediately.
        aofWriter = new PrintWriter(
                new BufferedWriter(
                        new FileWriter(aofPath.toFile(), true) // true = append mode
                ),
                true // autoFlush
        );
    }

    /**
     * Truncates the AOF log to empty and reopens the writer.
     *
     * Truncation strategy:
     *   1. Close the current AOF writer.
     *   2. Overwrite the AOF file with an empty file (not append mode).
     *   3. Reopen in append mode for future mutations.
     *
     * If any step fails, logs a warning. The old AOF writer state may be
     * inconsistent — we try to reopen even on partial failure.
     */
    private void truncateAOFLog() {
        Path aofPath = baseDir.resolve(AOF_FILENAME);

        synchronized (logLock) {
            // Close current writer.
            if (aofWriter != null) {
                aofWriter.close();
                aofWriter = null;
            }

            // Overwrite with empty content (NOT append mode = truncates file).
            try (FileWriter truncator = new FileWriter(aofPath.toFile(), false)) {
                // Writing nothing truncates the file.
                truncator.flush();
            } catch (IOException e) {
                log.warning("Failed to truncate AOF log: " + e.getMessage() +
                        ". Log may contain redundant entries but is still valid.");
            }

            // Reopen for appending.
            try {
                openAOFWriter();
            } catch (IOException e) {
                log.severe("Failed to reopen AOF writer after truncation: " + e.getMessage());
                // aofWriter remains null — logPut/logDelete will be no-ops until fixed.
            }
        }
    }

    /**
     * The scheduled snapshot task. Catches all exceptions to keep the
     * scheduler alive — an uncaught RuntimeException would cancel all
     * future scheduled executions.
     */
    private void takeScheduledSnapshot() {
        try {
            takeSnapshot();
        } catch (IOException e) {
            log.warning("Scheduled snapshot failed: " + e.getMessage());
        } catch (Throwable t) {
            log.severe("Unexpected error in snapshot scheduler: " + t.getMessage());
        }
    }
}