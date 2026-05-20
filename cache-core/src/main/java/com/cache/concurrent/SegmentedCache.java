package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.policy.LRUCache;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SegmentedCache — Thread-safe cache using per-segment locking.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * THE PROBLEM WITH COARSE-GRAINED LOCKING
 * ─────────────────────────────────────────────────────────────────────────────
 * CoarseGrainedCache has ONE lock for the entire cache.
 * Under high write contention, all threads queue up for that one lock.
 * Thread A writing "user:1" blocks Thread B writing "user:2" — even though
 * they're touching completely independent keys.
 *
 * This is the "hot lock" problem. The lock becomes the bottleneck, not the data.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * THE SEGMENTED LOCKING SOLUTION
 * ─────────────────────────────────────────────────────────────────────────────
 * Split the cache into N independent segments.
 * Each segment has its OWN lock and its OWN underlying cache.
 *
 * A key is assigned to a segment by: segment = hash(key) % N
 *
 * Now Thread A writing "user:1" and Thread B writing "user:2" only conflict
 * if both keys hash to the same segment. With N=16 segments, the probability
 * of collision is 1/16 = 6.25% — a massive reduction in contention.
 *
 * This is EXACTLY how ConcurrentHashMap worked in Java 7 (before it switched
 * to CAS-based lock-free approach in Java 8). You're implementing a classic.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * CAPACITY DISTRIBUTION
 * ─────────────────────────────────────────────────────────────────────────────
 * Total capacity is split evenly across segments:
 *   segmentCapacity = ceil(totalCapacity / numSegments)
 *
 * Implication: effective max capacity = segmentCapacity * numSegments
 * which may be slightly more than the requested capacity due to rounding.
 * This is acceptable — the total is within one segment-capacity of the target.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * CHOOSING N (NUMBER OF SEGMENTS)
 * ─────────────────────────────────────────────────────────────────────────────
 * Rule of thumb: N ≈ 2x the number of CPU cores you expect to saturate.
 * Default of 16 is a good starting point for 4-8 core machines.
 * Powers of 2 allow bitwise AND optimisation for segment selection:
 *   segment = hash & (N-1)  instead of  hash % N
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * TRADE-OFFS vs CoarseGrainedCache
 * ─────────────────────────────────────────────────────────────────────────────
 * BETTER:
 *   - Dramatically less lock contention under concurrent writes.
 *   - Throughput scales with number of segments (up to hardware limit).
 *
 * WORSE:
 *   - size() requires summing across all segments (N lock acquisitions).
 *   - getStats() similarly requires aggregating N stats objects.
 *   - More memory overhead (N separate cache structures + N locks).
 *   - If one segment receives all traffic (hot key problem), you're back to
 *     single-lock performance for that key.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * DESIGN NOTES
 * ─────────────────────────────────────────────────────────────────────────────
 * Each segment holds:
 *   - A Cache<K,V> delegate (typically LRUCache)
 *   - A ReentrantReadWriteLock
 *
 * The Segment inner class encapsulates both, keeping the code clean.
 *
 * @param <K> Key type. Must implement equals() and hashCode() correctly.
 * @param <V> Value type.
 */
public class SegmentedCache<K, V> implements Cache<K, V> {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /**
     * Default number of segments.
     * 16 is the classic ConcurrentHashMap default — enough for most workloads.
     * Must be a power of 2 to allow bitwise optimisation in segmentFor().
     */
    public static final int DEFAULT_NUM_SEGMENTS = 16;

    // -------------------------------------------------------------------------
    // Inner class: Segment
    // -------------------------------------------------------------------------

    /**
     * A single segment — one independently-locked partition of the cache.
     * Each segment is essentially a mini CoarseGrainedCache with its own
     * lock and its own delegate cache.
     *
     * We make this a private static inner class so the outer class can
     * access it cleanly without exposing it to external code.
     */
    private static class Segment<K, V> {

        /** The underlying cache for keys in this segment. */
        final Cache<K, V> cache;

        /**
         * This segment's exclusive lock.
         * Using fair=false here for maximum throughput — at the segment level,
         * starvation is less of a concern because any given key only contends
         * with keys that hash to the same segment (a small fraction of all threads).
         */
        final ReadWriteLock lock = new ReentrantReadWriteLock(false /* unfair for throughput */);

        Segment(int capacity) {
            // Each segment gets its own LRUCache with per-segment capacity.
            // Using LRUCache as the default eviction policy per segment.
            // In a more advanced version, this could be injected via a factory.
            this.cache = new LRUCache<>(capacity);
        }
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** The array of segments. Length is always a power of 2. */
    private final Segment<K, V>[] segments;

    /** Number of segments — cached to avoid repeated array length lookups. */
    private final int numSegments;

    /**
     * Mask for fast segment selection via bitwise AND.
     * segmentIndex = hash & mask  (equivalent to hash % numSegments, but faster)
     * Only works because numSegments is a power of 2.
     */
    private final int mask;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Creates a SegmentedCache with the default number of segments (16).
     *
     * @param totalCapacity Maximum total number of entries across all segments.
     *                      Distributed evenly: each segment holds totalCapacity/16 entries.
     */
    public SegmentedCache(int totalCapacity) {
        this(totalCapacity, DEFAULT_NUM_SEGMENTS);
    }

    /**
     * Creates a SegmentedCache with a custom number of segments.
     *
     * @param totalCapacity  Maximum total entries. Must be > 0.
     * @param numSegments    Number of partitions. Must be a positive power of 2.
     *                       Recommended: 2x your expected concurrent writer count.
     */
    @SuppressWarnings("unchecked")
    public SegmentedCache(int totalCapacity, int numSegments) {
        if (totalCapacity <= 0) {
            throw new IllegalArgumentException("totalCapacity must be > 0, got: " + totalCapacity);
        }
        if (numSegments <= 0 || (numSegments & (numSegments - 1)) != 0) {
            // Bitwise check: (n & n-1) == 0 iff n is a power of 2.
            throw new IllegalArgumentException(
                    "numSegments must be a positive power of 2, got: " + numSegments
            );
        }

        this.numSegments = numSegments;
        this.mask        = numSegments - 1; // e.g., 16 segments → mask = 0b1111 = 15

        // Calculate per-segment capacity.
        // ceil division so total effective capacity >= requested capacity.
        int segmentCapacity = (int) Math.ceil((double) totalCapacity / numSegments);
        segmentCapacity     = Math.max(1, segmentCapacity); // minimum 1 per segment

        // Initialise all segments.
        // Unchecked cast is unavoidable with Java generics + arrays.
        this.segments = new Segment[numSegments];
        for (int i = 0; i < numSegments; i++) {
            segments[i] = new Segment<>(segmentCapacity);
        }
    }

    // -------------------------------------------------------------------------
    // Cache<K,V> interface
    // -------------------------------------------------------------------------

    /**
     * Returns the value for the given key, or null if not present.
     *
     * Steps:
     *   1. Hash the key → determine which segment owns it.
     *   2. Acquire that segment's READ lock.
     *   3. Delegate get() to that segment's cache.
     *   4. Release the read lock.
     *
     * Only the owning segment's lock is acquired — all other segments
     * are completely unaffected by this operation.
     *
     * @param key The key to look up.
     * @return The value, or null if absent.
     */
    @Override
    public V get(K key) {
        Segment<K, V> segment = segmentFor(key);
        segment.lock.readLock().lock();
        try {
            return segment.cache.get(key);
        } finally {
            segment.lock.readLock().unlock();
        }
    }

    /**
     * Stores a key-value pair in the appropriate segment.
     *
     * Only the segment responsible for this key is locked.
     * All other segments continue serving requests uninterrupted.
     *
     * @param key   The key. Cannot be null.
     * @param value The value. Cannot be null.
     */
    @Override
    public void put(K key, V value) {
        Segment<K, V> segment = segmentFor(key);
        segment.lock.writeLock().lock();
        try {
            segment.cache.put(key, value);
        } finally {
            segment.lock.writeLock().unlock();
        }
    }

    /**
     * Removes the key from the appropriate segment.
     *
     * @param key The key to remove. Safe no-op if key doesn't exist.
     */
    @Override
    public void evict(K key) {
        Segment<K, V> segment = segmentFor(key);
        segment.lock.writeLock().lock();
        try {
            segment.cache.evict(key);
        } finally {
            segment.lock.writeLock().unlock();
        }
    }

    /**
     * Returns the total number of entries across ALL segments.
     *
     * Cost: O(N) — must acquire read lock on every segment and sum their sizes.
     * This is the main performance downside of segmented locking vs a single
     * global counter. For read-heavy workloads this is called rarely, so it's fine.
     *
     * Alternative: maintain a global AtomicInteger counter, updated on every
     * put/evict. Faster for size() but adds CAS overhead to every mutation.
     * We keep it simple here.
     *
     * @return Total entries across all segments.
     */
    @Override
    public int size() {
        int total = 0;
        for (Segment<K, V> segment : segments) {
            segment.lock.readLock().lock();
            try {
                total += segment.cache.size();
            } finally {
                segment.lock.readLock().unlock();
            }
        }
        return total;
    }

    /**
     * Returns aggregated stats across all segments.
     *
     * Creates a new CacheStats by summing hits, misses, and evictions
     * from every segment's stats. The result is a snapshot — it may
     * not be perfectly consistent across all segments (a write to segment 3
     * could occur between reading segment 2 and segment 4's stats), but
     * for monitoring purposes this level of consistency is acceptable.
     *
     * @return Aggregated CacheStats across all segments.
     */
    @Override
    public CacheStats getstats() {
        long totalHits      = 0;
        long totalMisses    = 0;
        long totalEvictions = 0;

        for (Segment<K, V> segment : segments) {
            segment.lock.readLock().lock();
            try {
                CacheStats s = segment.cache.getstats();

                totalHits      += s.hits();
                totalMisses    += s.misses();
                totalEvictions += s.evictions();

            } finally {
                segment.lock.readLock().unlock();
            }
        }

        double hitRate =
                (totalHits + totalMisses) == 0
                        ? 0.0
                        : (double) totalHits / (totalHits + totalMisses);

        return new CacheStats(
                totalHits,
                totalMisses,
                totalEvictions,
                hitRate
        );
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Determines which segment is responsible for the given key.
     *
     * Algorithm:
     *   1. Get the key's hashCode().
     *   2. Apply a secondary hash (Wang/Jenkins style) to improve distribution.
     *      Java's HashMap uses this because poor hashCode() implementations
     *      (e.g., all keys return multiples of 16) would cluster into
     *      the same segment, defeating the purpose of segmentation.
     *   3. AND with the mask to get the segment index.
     *
     * The secondary hash is the same technique used by Java 7's ConcurrentHashMap.
     *
     * @param key The key to route.
     * @return The segment responsible for this key.
     */
    private Segment<K, V> segmentFor(K key) {
        int h = key.hashCode();

        // Secondary hash — spreads poor hash distributions across segments.
        // Without this, keys with hashCode() that are multiples of numSegments
        // would all route to segment 0.
        h ^= (h >>> 20) ^ (h >>> 12);
        h ^= (h >>> 7)  ^ (h >>> 4);

        // Bitwise AND with mask — equivalent to h % numSegments, but faster.
        // Math.abs handles negative hashCodes.
        int index = (h & mask);
        // Ensure non-negative (handles edge case where h is Integer.MIN_VALUE)
        index = index < 0 ? index + numSegments : index;

        return segments[index];
    }

    // -------------------------------------------------------------------------
    // Diagnostic methods (useful for benchmarking and testing)
    // -------------------------------------------------------------------------

    /**
     * Returns the number of segments this cache is divided into.
     * Useful in tests to verify configuration.
     */
    public int getNumSegments() {
        return numSegments;
    }

    /**
     * Returns the size of a specific segment by index.
     * Useful in tests to verify even key distribution across segments.
     *
     * @param segmentIndex Index from 0 to numSegments-1.
     * @return Number of entries in that segment.
     */
    public int getSegmentSize(int segmentIndex) {
        if (segmentIndex < 0 || segmentIndex >= numSegments) {
            throw new IllegalArgumentException(
                    "Segment index out of range: " + segmentIndex + " (numSegments=" + numSegments + ")"
            );
        }
        Segment<K, V> segment = segments[segmentIndex];
        segment.lock.readLock().lock();
        try {
            return segment.cache.size();
        } finally {
            segment.lock.readLock().unlock();
        }
    }

    /**
     * Returns which segment index a given key belongs to.
     * Useful in tests to verify routing consistency.
     *
     * @param key The key to check.
     * @return Segment index (0 to numSegments-1).
     */
    public int getSegmentIndexFor(K key) {
        int h = key.hashCode();
        h ^= (h >>> 20) ^ (h >>> 12);
        h ^= (h >>> 7)  ^ (h >>> 4);
        int index = h & mask;
        return index < 0 ? index + numSegments : index;
    }
}