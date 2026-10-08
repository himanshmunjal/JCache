package com.cache.persistence;

import com.cache.api.Cache;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Writes the whole cache to a binary snapshot file.
 *
 * <p>Layout (all integers big-endian):
 * <pre>
 * header: int magic ("JCAC"), int version, long timestamp, int entryCount
 * entry:  int keyLen, byte[keyLen] key, int valueLen, byte[valueLen] value, long expiryMs
 * </pre>
 * Strings are UTF-8 and length-prefixed with a 4-byte int, so they are not
 * limited to the 64 KB that {@code DataOutputStream.writeUTF} allows.
 *
 * <p>The file is written to a temporary name and then renamed over the old
 * snapshot, so a crash mid-write never destroys the previous good snapshot.
 */
public class SnapshotWriter {

    private static final Logger log = Logger.getLogger(SnapshotWriter.class.getName());

    /** "JCAC" in ASCII. */
    static final int MAGIC_NUMBER = 0x4A434143;
    static final int FORMAT_VERSION = 1;

    private final Path baseDir;
    private final Cache<String, String> cache;

    /**
     * Creates a writer.
     *
     * @param baseDir directory the snapshot is written to
     * @param cache   the cache to snapshot; it must implement {@link SnapshotCapable}
     *                for any entries to be written
     */
    public SnapshotWriter(Path baseDir, Cache<String, String> cache) {
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
     * Writes a snapshot of the cache's current contents.
     *
     * @return number of entries written
     * @throws IOException if the snapshot could not be written; the previous
     *                     snapshot is left untouched in that case
     */
    public int write() throws IOException {
        List<Entry> entries = collectLiveEntries();
        Path tmp = baseDir.resolve(PersistenceManager.SNAPSHOT_TMP_FILENAME);
        Path target = baseDir.resolve(PersistenceManager.SNAPSHOT_FILENAME);

        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            out.writeInt(MAGIC_NUMBER);
            out.writeInt(FORMAT_VERSION);
            out.writeLong(System.currentTimeMillis());
            out.writeInt(entries.size());
            for (Entry e : entries) {
                writeString(out, e.key);
                writeString(out, e.value);
                out.writeLong(e.expiryMs);
            }
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw new IOException("Snapshot write failed: " + e.getMessage(), e);
        }

        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            log.warning("Atomic rename not supported here, falling back to a plain move");
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
        log.fine(() -> "Wrote " + entries.size() + " entries to " + target);
        return entries.size();
    }

    private List<Entry> collectLiveEntries() {
        if (!(cache instanceof SnapshotCapable snapshotCapable)) {
            log.warning("Cache does not implement SnapshotCapable; writing an empty snapshot");
            return List.of();
        }
        long now = System.currentTimeMillis();
        List<Entry> entries = new ArrayList<>();
        for (Map.Entry<String, Long> e : snapshotCapable.getSnapshotEntries().entrySet()) {
            long expiry = e.getValue();
            if (expiry != -1L && expiry <= now) {
                continue;
            }
            // peek() rather than get(): taking a snapshot must not count as an access.
            String value = cache.peek(e.getKey());
            if (value != null) {
                entries.add(new Entry(e.getKey(), value, expiry));
            }
        }
        return entries;
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private record Entry(String key, String value, long expiryMs) {
    }
}
