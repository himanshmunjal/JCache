package com.cache.persistence;

import com.cache.api.Cache;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Durable storage for a string cache, modelled on Redis's RDB + AOF.
 *
 * <ul>
 *   <li>Every write is appended to {@value #AOF_FILENAME} as it happens.</li>
 *   <li>Periodically the whole cache is written to {@value #SNAPSHOT_FILENAME}
 *       and the AOF is truncated, which keeps the AOF short.</li>
 *   <li>On startup {@link SnapshotLoader} loads the snapshot and replays the AOF.</li>
 * </ul>
 *
 * <p>AOF writes are flushed to the operating system after every record but
 * not fsynced, so a power loss can drop the last few writes. A process crash
 * cannot.
 *
 * <p>A snapshot holds the AOF lock while it runs. Writers that log during a
 * snapshot wait for it to finish and then append to the fresh AOF, so no
 * write is lost between the snapshot and the truncation.
 */
public class PersistenceManager implements Closeable {

    private static final Logger log = Logger.getLogger(PersistenceManager.class.getName());

    /** Snapshot file name inside the data directory. */
    public static final String SNAPSHOT_FILENAME = "jcache.snapshot";
    /** Append-only file name inside the data directory. */
    public static final String AOF_FILENAME = "jcache.aof";
    static final String SNAPSHOT_TMP_FILENAME = "jcache.snapshot.tmp";

    /** Default interval between automatic snapshots: five minutes. */
    public static final long DEFAULT_SNAPSHOT_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

    private final Path baseDir;
    private final SnapshotWriter snapshotWriter;
    private final Object aofLock = new Object();
    private final AtomicLong aofEntries = new AtomicLong();
    private final ScheduledExecutorService scheduler;
    private BufferedWriter aof;
    private volatile boolean closed;

    /**
     * Opens the AOF for appending and, if {@code snapshotIntervalMs > 0},
     * starts taking snapshots on that interval.
     *
     * @param baseDir            data directory; created if missing
     * @param cache              the cache to snapshot
     * @param snapshotIntervalMs interval between snapshots, or 0 for manual snapshots only
     * @throws IOException if the directory or the AOF cannot be opened
     */
    public PersistenceManager(Path baseDir, Cache<String, String> cache, long snapshotIntervalMs) throws IOException {
        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir cannot be null");
        }
        if (cache == null) {
            throw new IllegalArgumentException("cache cannot be null");
        }
        if (snapshotIntervalMs < 0) {
            throw new IllegalArgumentException("snapshotIntervalMs must be >= 0, got: " + snapshotIntervalMs);
        }
        this.baseDir = baseDir;
        this.snapshotWriter = new SnapshotWriter(baseDir, cache);
        Files.createDirectories(baseDir);
        this.aof = openAof(StandardOpenOption.APPEND);

        if (snapshotIntervalMs > 0) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "jcache-snapshot");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(this::scheduledSnapshot,
                    snapshotIntervalMs, snapshotIntervalMs, TimeUnit.MILLISECONDS);
        } else {
            scheduler = null;
        }
    }

    /**
     * Same as {@code PersistenceManager(baseDir, cache, DEFAULT_SNAPSHOT_INTERVAL_MS)}.
     *
     * @param baseDir data directory
     * @param cache   the cache to snapshot
     * @throws IOException if the directory or the AOF cannot be opened
     */
    public PersistenceManager(Path baseDir, Cache<String, String> cache) throws IOException {
        this(baseDir, cache, DEFAULT_SNAPSHOT_INTERVAL_MS);
    }

    /**
     * Records a write.
     *
     * @param key          the key
     * @param value        the value
     * @param expiryTimeMs absolute expiry time in epoch millis, or {@code -1}
     */
    public void logPut(String key, String value, long expiryTimeMs) {
        if (key != null && value != null) {
            append(AofRecord.set(key, value, expiryTimeMs));
        }
    }

    /**
     * Records a write with no expiry.
     *
     * @param key   the key
     * @param value the value
     */
    public void logPut(String key, String value) {
        logPut(key, value, -1L);
    }

    /**
     * Records a removal.
     *
     * @param key the removed key
     */
    public void logDelete(String key) {
        if (key != null) {
            append(AofRecord.delete(key));
        }
    }

    /**
     * Records that every key was removed.
     */
    public void logClear() {
        append(AofRecord.flush());
    }

    /**
     * Runs {@code mutation} while holding the AOF lock. Wrapping a cache write
     * and its {@code log*} call in this method guarantees that concurrent writes
     * to the same key reach the AOF in the order they were applied.
     *
     * @param mutation the cache update and the matching log call
     */
    public void atomically(Runnable mutation) {
        synchronized (aofLock) {
            mutation.run();
        }
    }

    /**
     * Writes a snapshot and truncates the AOF.
     *
     * @throws IOException if the snapshot could not be written; the AOF is
     *                     kept in that case so nothing is lost
     */
    public void takeSnapshot() throws IOException {
        if (closed) {
            throw new IllegalStateException("PersistenceManager is closed");
        }
        synchronized (aofLock) {
            long start = System.nanoTime();
            int written = snapshotWriter.write();
            aof.close();
            aof = openAof(StandardOpenOption.TRUNCATE_EXISTING);
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            log.fine(() -> "Snapshot of " + written + " keys written in " + ms + " ms");
        }
    }

    /** @return path of the snapshot file */
    public Path getSnapshotPath() {
        return baseDir.resolve(SNAPSHOT_FILENAME);
    }

    /** @return path of the append-only file */
    public Path getAOFPath() {
        return baseDir.resolve(AOF_FILENAME);
    }

    /** @return number of records appended since this manager was created */
    public long getAOFEntryCount() {
        return aofEntries.get();
    }

    /**
     * Stops the scheduler, writes a final snapshot and closes the AOF.
     * Later log calls are ignored. Safe to call more than once.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            takeSnapshot();
        } catch (IOException | UncheckedIOException e) {
            log.log(Level.WARNING, "Final snapshot failed; the AOF is kept for recovery", e);
        }
        synchronized (aofLock) {
            closed = true;
            try {
                aof.close();
            } catch (IOException e) {
                log.log(Level.WARNING, "Could not close AOF", e);
            }
        }
    }

    private void append(AofRecord record) {
        synchronized (aofLock) {
            if (closed) {
                return;
            }
            try {
                aof.write(record.encode());
                aof.newLine();
                aof.flush();
                aofEntries.incrementAndGet();
            } catch (IOException e) {
                throw new UncheckedIOException("Could not append to " + getAOFPath(), e);
            }
        }
    }

    private BufferedWriter openAof(StandardOpenOption mode) throws IOException {
        return Files.newBufferedWriter(getAOFPath(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, mode);
    }

    private void scheduledSnapshot() {
        try {
            takeSnapshot();
        } catch (Exception e) {
            // Must not escape, or the scheduler cancels every future snapshot.
            log.log(Level.WARNING, "Scheduled snapshot failed", e);
        }
    }
}
