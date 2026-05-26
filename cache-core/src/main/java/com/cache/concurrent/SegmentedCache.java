//package com.cache.concurrent;
//
//import com.cache.api.Cache;
//import com.cache.api.CacheStats;
//import com.cache.policy.LRUCache;
//
//import java.util.concurrent.locks.ReadWriteLock;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//
///**
// * SegmentedCache — Thread-safe cache using per-segment locking.
// *
// * ─────────────────────────────────────────────────────────────────────────────
// * THE PROBLEM WITH COARSE-GRAINED LOCKING
// * ─────────────────────────────────────────────────────────────────────────────
// * CoarseGrainedCache has ONE lock for the entire cache.
// * Under high write contention, all threads queue up for that one lock.
// * Thread A writing "user:1" blocks Thread B writing "user:2" — even though
// * they're touching completely independent keys.
// *
// * This is the "hot lock" problem. The lock becomes the bottleneck, not the data.
// *
// * ─────────────────────────────────────────────────────────────────────────────
// * THE SEGMENTED LOCKING SOLUTION
// * ─────────────────────────────────────────────────────────────────────────────
// * Split the cache into N independent segments.
// * Each segment has its OWN lock and its OWN underlying cache.
// *
// * A key is assigned to a segment by: segment = hash(key) % N
// *
// * Now Thread A writing "user:1" and Thread B writing "user:2" only conflict
// * if both keys hash to the same segment. With N=16 segments, the probability
// * of collision is 1/16 = 6.25% — a massive reduction in contention.
// *
// * This is EXACTLY how ConcurrentHashMap worked in Java 7 (before it switched
// * to CAS-based lock-free approach in Java 8). You're implementing a classic.
// *
// * ─────────────────────────────────────────────────────────────────────────────
// * CAPACITY DISTRIBUTION
// * ─────────────────────────────────────────────────────────────────────────────
// * Total capacity is split evenly across segments:
// *   segmentCapacity = ceil(totalCapacity / numSegments)
// *
// * Implication: effective max capacity = segmentCapacity * numSegments
// * which may be slightly more than the requested capacity due to rounding.
// * This is acceptable — the total is within one segment-capacity of the target.
// *
// * ─────────────────────────────────────────────────────────────────────────────
// * CHOOSING N (NUMBER OF SEGMENTS)
// * ─────────────────────────────────────────────────────────────────────────────
// * Rule of thumb: N ≈ 2x the number of CPU cores you expect to saturate.
// * Default of 16 is a good starting point for 4-8 core machines.
// * Powers of 2 allow bitwise AND optimisation for segment selection:
// *   segment = hash & (N-1)  instead of  hash % N
// *
// * ─────────────────────────────────────────────────────────────────────────────
// * TRADE-OFFS vs CoarseGrainedCache
// * ─────────────────────────────────────────────────────────────────────────────
// * BETTER:
// *   - Dramatically less lock contention under concurrent writes.
// *   - Throughput scales with number of segments (up to hardware limit).
// *
// * WORSE:
// *   - size() requires summing across all segments (N lock acquisitions).
// *   - getStats() similarly requires aggregating N stats objects.
// *   - More memory overhead (N separate cache structures + N locks).
// *   - If one segment receives all traffic (hot key problem), you're back to
// *     single-lock performance for that key.
// *
// * ─────────────────────────────────────────────────────────────────────────────
// * DESIGN NOTES
// * ─────────────────────────────────────────────────────────────────────────────
// * Each segment holds:
// *   - A Cache<K,V> delegate (typically LRUCache)
// *   - A ReentrantReadWriteLock
// *
// * The Segment inner class encapsulates both, keeping the code clean.
// *
// * @param <K> Key type. Must implement equals() and hashCode() correctly.
// * @param <V> Value type.
// */
//public class SegmentedCache<K, V> implements Cache<K, V> {
//
//    // -------------------------------------------------------------------------
//    // Constants
//    // -------------------------------------------------------------------------
//
//    /**
//     * Default number of segments.
//     * 16 is the classic ConcurrentHashMap default — enough for most workloads.
//     * Must be a power of 2 to allow bitwise optimisation in segmentFor().
//     */
//    public static final int DEFAULT_NUM_SEGMENTS = 16;
//
//    // -------------------------------------------------------------------------
//    // Inner class: Segment
//    // -------------------------------------------------------------------------
//
//    /**
//     * A single segment — one independently-locked partition of the cache.
//     * Each segment is essentially a mini CoarseGrainedCache with its own
//     * lock and its own delegate cache.
//     *
//     * We make this a private static inner class so the outer class can
//     * access it cleanly without exposing it to external code.
//     */
//    private static class Segment<K, V> {
//
//        /** The underlying cache for keys in this segment. */
//        final Cache<K, V> cache;
//
//        /**
//         * This segment's exclusive lock.
//         * Using fair=false here for maximum throughput — at the segment level,
//         * starvation is less of a concern because any given key only contends
//         * with keys that hash to the same segment (a small fraction of all threads).
//         */
//        final ReadWriteLock lock = new ReentrantReadWriteLock(false /* unfair for throughput */);
//
//        Segment(int capacity) {
//            // Each segment gets its own LRUCache with per-segment capacity.
//            // Using LRUCache as the default eviction policy per segment.
//            // In a more advanced version, this could be injected via a factory.
//            this.cache = new LRUCache<>(capacity);
//        }
//    }
//
//    // -------------------------------------------------------------------------
//    // State
//    // -------------------------------------------------------------------------
//
//    /** The array of segments. Length is always a power of 2. */
//    private final Segment<K, V>[] segments;
//
//    /** Number of segments — cached to avoid repeated array length lookups. */
//    private final int numSegments;
//
//    /**
//     * Mask for fast segment selection via bitwise AND.
//     * segmentIndex = hash & mask  (equivalent to hash % numSegments, but faster)
//     * Only works because numSegments is a power of 2.
//     */
//    private final int mask;
//
//    // -------------------------------------------------------------------------
//    // Constructors
//    // -------------------------------------------------------------------------
//
//    /**
//     * Creates a SegmentedCache with the default number of segments (16).
//     *
//     * @param totalCapacity Maximum total number of entries across all segments.
//     *                      Distributed evenly: each segment holds totalCapacity/16 entries.
//     */
//    public SegmentedCache(int totalCapacity) {
//        this(totalCapacity, DEFAULT_NUM_SEGMENTS);
//    }
//
//    /**
//     * Creates a SegmentedCache with a custom number of segments.
//     *
//     * @param totalCapacity  Maximum total entries. Must be > 0.
//     * @param numSegments    Number of partitions. Must be a positive power of 2.
//     *                       Recommended: 2x your expected concurrent writer count.
//     */
//    @SuppressWarnings("unchecked")
//    public SegmentedCache(int totalCapacity, int numSegments) {
//        if (totalCapacity <= 0) {
//            throw new IllegalArgumentException("totalCapacity must be > 0, got: " + totalCapacity);
//        }
//        if (numSegments <= 0 || (numSegments & (numSegments - 1)) != 0) {
//            // Bitwise check: (n & n-1) == 0 iff n is a power of 2.
//            throw new IllegalArgumentException(
//                    "numSegments must be a positive power of 2, got: " + numSegments
//            );
//        }
//
//        this.numSegments = numSegments;
//        this.mask        = numSegments - 1; // e.g., 16 segments → mask = 0b1111 = 15
//
//        // Calculate per-segment capacity.
//        // ceil division so total effective capacity >= requested capacity.
//        int segmentCapacity = (int) Math.ceil((double) totalCapacity / numSegments);
//        segmentCapacity     = Math.max(1, segmentCapacity); // minimum 1 per segment
//
//        // Initialise all segments.
//        // Unchecked cast is unavoidable with Java generics + arrays.
//        this.segments = new Segment[numSegments];
//        for (int i = 0; i < numSegments; i++) {
//            segments[i] = new Segment<>(segmentCapacity);
//        }
//    }
//
//    // -------------------------------------------------------------------------
//    // Cache<K,V> interface
//    // -------------------------------------------------------------------------
//
//    /**
//     * Returns the value for the given key, or null if not present.
//     *
//     * Steps:
//     *   1. Hash the key → determine which segment owns it.
//     *   2. Acquire that segment's READ lock.
//     *   3. Delegate get() to that segment's cache.
//     *   4. Release the read lock.
//     *
//     * Only the owning segment's lock is acquired — all other segments
//     * are completely unaffected by this operation.
//     *
//     * @param key The key to look up.
//     * @return The value, or null if absent.
//     */
//    @Override
//    public V get(K key) {
//        Segment<K, V> segment = segmentFor(key);
//        segment.lock.readLock().lock();
//        try {
//            return segment.cache.get(key);
//        } finally {
//            segment.lock.readLock().unlock();
//        }
//    }
//
//    /**
//     * Stores a key-value pair in the appropriate segment.
//     *
//     * Only the segment responsible for this key is locked.
//     * All other segments continue serving requests uninterrupted.
//     *
//     * @param key   The key. Cannot be null.
//     * @param value The value. Cannot be null.
//     */
//    @Override
//    public void put(K key, V value) {
//        Segment<K, V> segment = segmentFor(key);
//        segment.lock.writeLock().lock();
//        try {
//            segment.cache.put(key, value);
//        } finally {
//            segment.lock.writeLock().unlock();
//        }
//    }
//
//    /**
//     * Removes the key from the appropriate segment.
//     *
//     * @param key The key to remove. Safe no-op if key doesn't exist.
//     */
//    @Override
//    public void evict(K key) {
//        Segment<K, V> segment = segmentFor(key);
//        segment.lock.writeLock().lock();
//        try {
//            segment.cache.evict(key);
//        } finally {
//            segment.lock.writeLock().unlock();
//        }
//    }
//
//    /**
//     * Returns the total number of entries across ALL segments.
//     *
//     * Cost: O(N) — must acquire read lock on every segment and sum their sizes.
//     * This is the main performance downside of segmented locking vs a single
//     * global counter. For read-heavy workloads this is called rarely, so it's fine.
//     *
//     * Alternative: maintain a global AtomicInteger counter, updated on every
//     * put/evict. Faster for size() but adds CAS overhead to every mutation.
//     * We keep it simple here.
//     *
//     * @return Total entries across all segments.
//     */
//    @Override
//    public int size() {
//        int total = 0;
//        for (Segment<K, V> segment : segments) {
//            segment.lock.readLock().lock();
//            try {
//                total += segment.cache.size();
//            } finally {
//                segment.lock.readLock().unlock();
//            }
//        }
//        return total;
//    }
//
//    /**
//     * Returns aggregated stats across all segments.
//     *
//     * Creates a new CacheStats by summing hits, misses, and evictions
//     * from every segment's stats. The result is a snapshot — it may
//     * not be perfectly consistent across all segments (a write to segment 3
//     * could occur between reading segment 2 and segment 4's stats), but
//     * for monitoring purposes this level of consistency is acceptable.
//     *
//     * @return Aggregated CacheStats across all segments.
//     */
//    @Override
//    public CacheStats getstats() {
//        long totalHits      = 0;
//        long totalMisses    = 0;
//        long totalEvictions = 0;
//
//        for (Segment<K, V> segment : segments) {
//            segment.lock.readLock().lock();
//            try {
//                CacheStats s = segment.cache.getstats();
//
//                totalHits      += s.hits();
//                totalMisses    += s.misses();
//                totalEvictions += s.evictions();
//
//            } finally {
//                segment.lock.readLock().unlock();
//            }
//        }
//
//        double hitRate =
//                (totalHits + totalMisses) == 0
//                        ? 0.0
//                        : (double) totalHits / (totalHits + totalMisses);
//
//        return new CacheStats(
//                totalHits,
//                totalMisses,
//                totalEvictions,
//                hitRate
//        );
//    }
//
//    // -------------------------------------------------------------------------
//    // Private helpers
//    // -------------------------------------------------------------------------
//
//    /**
//     * Determines which segment is responsible for the given key.
//     *
//     * Algorithm:
//     *   1. Get the key's hashCode().
//     *   2. Apply a secondary hash (Wang/Jenkins style) to improve distribution.
//     *      Java's HashMap uses this because poor hashCode() implementations
//     *      (e.g., all keys return multiples of 16) would cluster into
//     *      the same segment, defeating the purpose of segmentation.
//     *   3. AND with the mask to get the segment index.
//     *
//     * The secondary hash is the same technique used by Java 7's ConcurrentHashMap.
//     *
//     * @param key The key to route.
//     * @return The segment responsible for this key.
//     */
//    private Segment<K, V> segmentFor(K key) {
//        int h = key.hashCode();
//
//        // Secondary hash — spreads poor hash distributions across segments.
//        // Without this, keys with hashCode() that are multiples of numSegments
//        // would all route to segment 0.
//        h ^= (h >>> 20) ^ (h >>> 12);
//        h ^= (h >>> 7)  ^ (h >>> 4);
//
//        // Bitwise AND with mask — equivalent to h % numSegments, but faster.
//        // Math.abs handles negative hashCodes.
//        int index = (h & mask);
//        // Ensure non-negative (handles edge case where h is Integer.MIN_VALUE)
//        index = index < 0 ? index + numSegments : index;
//
//        return segments[index];
//    }
//
//    // -------------------------------------------------------------------------
//    // Diagnostic methods (useful for benchmarking and testing)
//    // -------------------------------------------------------------------------
//
//    /**
//     * Returns the number of segments this cache is divided into.
//     * Useful in tests to verify configuration.
//     */
//    public int getNumSegments() {
//        return numSegments;
//    }
//
//    /**
//     * Returns the size of a specific segment by index.
//     * Useful in tests to verify even key distribution across segments.
//     *
//     * @param segmentIndex Index from 0 to numSegments-1.
//     * @return Number of entries in that segment.
//     */
//    public int getSegmentSize(int segmentIndex) {
//        if (segmentIndex < 0 || segmentIndex >= numSegments) {
//            throw new IllegalArgumentException(
//                    "Segment index out of range: " + segmentIndex + " (numSegments=" + numSegments + ")"
//            );
//        }
//        Segment<K, V> segment = segments[segmentIndex];
//        segment.lock.readLock().lock();
//        try {
//            return segment.cache.size();
//        } finally {
//            segment.lock.readLock().unlock();
//        }
//    }
//
//    /**
//     * Returns which segment index a given key belongs to.
//     * Useful in tests to verify routing consistency.
//     *
//     * @param key The key to check.
//     * @return Segment index (0 to numSegments-1).
//     */
//    public int getSegmentIndexFor(K key) {
//        int h = key.hashCode();
//        h ^= (h >>> 20) ^ (h >>> 12);
//        h ^= (h >>> 7)  ^ (h >>> 4);
//        int index = h & mask;
//        return index < 0 ? index + numSegments : index;
//    }
//}

package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.policy.ARCCache;
import com.cache.policy.LFUCache;
import com.cache.policy.LRUCache;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
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
 * ─────────────────────────────────────────────────────────────────────────────
 * THE SEGMENTED LOCKING SOLUTION
 * ─────────────────────────────────────────────────────────────────────────────
 * Split the cache into N independent segments.
 * Each segment has its OWN lock and its OWN underlying cache.
 * A key is assigned to a segment by: segment = hash(key) & mask
 *
 * With N=16 segments, collision probability = 1/16 = 6.25% — massive
 * reduction in contention vs a single global lock.
 *
 * This is how ConcurrentHashMap worked in Java 7.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * POLICY SUPPORT — WHAT CHANGED FROM ORIGINAL
 * ─────────────────────────────────────────────────────────────────────────────
 * Original: each segment always created a new LRUCache internally.
 * Problem:  CacheServer could not inject LFU or ARC — the policy was ignored.
 *
 * Fix: added a PolicyType enum and a constructor that accepts it.
 * Each segment now creates the correct policy type based on the enum.
 *
 * WHY ENUM INSTEAD OF PASSING A CACHE DELEGATE?
 * SegmentedCache creates ONE cache per segment (numSegments total).
 * If we accepted a single Cache<K,V> delegate, we'd only have one cache
 * shared across all segments — defeating the purpose of segmentation.
 * The enum lets each segment instantiate its own correctly-typed cache.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * CAPACITY DISTRIBUTION
 * ─────────────────────────────────────────────────────────────────────────────
 * Total capacity is split evenly: segmentCapacity = ceil(totalCapacity / N)
 * Effective max = segmentCapacity * N (slightly > requested due to rounding).
 *
 * @param <K> Key type. Must implement equals() and hashCode() correctly.
 * @param <V> Value type.
 */
public class SegmentedCache<K, V> implements Cache<K, V> {

    // -------------------------------------------------------------------------
    // Policy enum — tells each segment which cache type to instantiate
    // -------------------------------------------------------------------------

    /**
     * Eviction policy to use for each segment's internal cache.
     *
     * Each segment creates its own instance of the chosen policy.
     * With 16 segments and LFU policy: 16 independent LFUCache instances,
     * each covering 1/16 of the key space.
     */
    public enum PolicyType {
        LRU,
        LFU,
        ARC
    }

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /**
     * Default number of segments.
     * 16 matches ConcurrentHashMap's classic default.
     * Must be a power of 2 for the bitwise mask optimisation.
     */
    public static final int DEFAULT_NUM_SEGMENTS = 16;

    // -------------------------------------------------------------------------
    // Inner class: Segment
    // -------------------------------------------------------------------------

    /**
     * One independently-locked partition of the cache.
     * Contains its own eviction-policy cache and its own ReadWriteLock.
     */
    private static class Segment<K, V> {

        /** The underlying cache for keys that hash to this segment. */
        final Cache<K, V> cache;

        /**
         * This segment's exclusive lock.
         * Unfair mode (false) for maximum throughput — starvation at the
         * segment level is unlikely because segments receive 1/N of total traffic.
         */
//        final ReadWriteLock lock = new ReentrantReadWriteLock(false);
        final ReentrantLock lock = new ReentrantLock();
        /**
         * Creates a segment with the given capacity and policy.
         *
         * @param capacity Per-segment capacity (totalCapacity / numSegments).
         * @param policy   Which eviction algorithm to use for this segment.
         */
        Segment(int capacity, PolicyType policy) {
            switch (policy) {
                case LFU:
                    this.cache = new LFUCache<>(capacity);
                    break;
                case ARC:
                    this.cache = new ARCCache<>(capacity);
                    break;
                case LRU:
                default:
                    this.cache = new LRUCache<>(capacity);
                    break;
            }
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
     * Mask for fast segment selection: index = hash & mask.
     * Equivalent to hash % numSegments but avoids division.
     * Only works because numSegments is always a power of 2.
     */
    private final int mask;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Creates a SegmentedCache with default 16 segments and LRU policy.
     * Use this when you want thread safety with default settings.
     *
     * @param totalCapacity Maximum total entries across all segments.
     */
    public SegmentedCache(int totalCapacity) {
        this(totalCapacity, DEFAULT_NUM_SEGMENTS, PolicyType.LRU);
    }

    /**
     * Creates a SegmentedCache with custom segment count and LRU policy.
     * This is the constructor CacheServer originally called — preserved for
     * backward compatibility. Now internally uses LRU (was always LRU anyway).
     *
     * @param totalCapacity Maximum total entries.
     * @param numSegments   Number of partitions. Must be a positive power of 2.
     */
    public SegmentedCache(int totalCapacity, int numSegments) {
        this(totalCapacity, numSegments, PolicyType.LRU);
    }

    /**
     * Creates a SegmentedCache with custom segment count AND eviction policy.
     *
     * This is the constructor CacheServer should use to honour the configured
     * eviction policy (LRU / LFU / ARC). Each segment gets its own instance
     * of the chosen policy type with per-segment capacity.
     *
     * Example:
     *   // 16-segment LFU cache with total capacity 1000
     *   // Each segment: LFUCache with capacity 63 (ceil(1000/16))
     *   new SegmentedCache<>(1000, 16, PolicyType.LFU)
     *
     * @param totalCapacity Maximum total entries. Must be > 0.
     * @param numSegments   Segment count. Must be a positive power of 2.
     * @param policy        Eviction policy for each segment's internal cache.
     */
    @SuppressWarnings("unchecked")
    public SegmentedCache(int totalCapacity, int numSegments, PolicyType policy) {
        if (totalCapacity <= 0) {
            throw new IllegalArgumentException(
                    "totalCapacity must be > 0, got: " + totalCapacity);
        }
        if (numSegments <= 0 || (numSegments & (numSegments - 1)) != 0) {
            throw new IllegalArgumentException(
                    "numSegments must be a positive power of 2, got: " + numSegments);
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy cannot be null");
        }

        this.numSegments = numSegments;
        this.mask        = numSegments - 1;

        // Per-segment capacity: ceiling division so total >= requested.
        int segmentCapacity = (int) (totalCapacity + numSegments - 1) / numSegments;
        segmentCapacity     = Math.max(1, segmentCapacity);

        this.segments = new Segment[numSegments];
        for (int i = 0; i < numSegments; i++) {
            segments[i] = new Segment<>(segmentCapacity, policy);
        }
    }

    // -------------------------------------------------------------------------
    // Cache<K,V> interface
    // -------------------------------------------------------------------------

    /**
     * Returns the value for the given key, or null if not present.
     * Only acquires the read lock for the segment that owns this key.
     * All other segments are unaffected.
     *
     * @param key The key to look up.
     * @return The value, or null if absent.
     */
    @Override
    public V get(K key) {
        Segment<K, V> segment = segmentFor(key);
        segment.lock.lock();
        try {
            return segment.cache.get(key);
        } finally {
            segment.lock.unlock();
        }
    }

    /**
     * Stores a key-value pair in the appropriate segment.
     * Only the segment responsible for this key is write-locked.
     *
     * @param key   The key. Cannot be null.
     * @param value The value. Cannot be null.
     */
    @Override
    public void put(K key, V value) {
        Segment<K, V> segment = segmentFor(key);
        segment.lock.lock();
        try {
            segment.cache.put(key, value);
        } finally {
            segment.lock.unlock();
        }
    }

    /**
     * Removes the key from its segment. Safe no-op if key doesn't exist.
     *
     * @param key The key to remove.
     */
    @Override
    public void evict(K key) {
        Segment<K, V> segment = segmentFor(key);
        segment.lock.lock();
        try {
            segment.cache.evict(key);
        } finally {
            segment.lock.unlock();
        }
    }

    /**
     * Returns total entries across ALL segments.
     *
     * Cost: O(N) — acquires read lock on every segment and sums sizes.
     * This is the main cost of segmented locking vs a global AtomicInteger
     * counter. Acceptable because size() is called infrequently (mostly STATS).
     *
     * @return Total entries across all segments.
     */
    @Override
    public int size() {
        int total = 0;
        for (Segment<K, V> segment : segments) {
            segment.lock.lock();
            try {
                total += segment.cache.size();
            } finally {
                segment.lock.unlock();
            }
        }
        return total;
    }

    @Override
    public void clear() {
        for (Segment<K,V> segment : segments) {
            segment.lock.lock();
            try {
                segment.cache.clear();
            } finally {
                segment.lock.unlock();
            }
        }
    }

    /**
     * Returns aggregated stats across all segments.
     *
     * Sums hits, misses, and evictions from every segment.
     * Not perfectly consistent (a write between two segment reads is invisible)
     * but accurate enough for monitoring and the STATS command.
     *
     * @return Aggregated CacheStats snapshot.
     */
    @Override
    public CacheStats getstats() {
        long totalHits      = 0;
        long totalMisses    = 0;
        long totalEvictions = 0;

        for (Segment<K, V> segment : segments) {
            segment.lock.lock();
            try {
                CacheStats s    = segment.cache.getstats();
                totalHits      += s.hits();
                totalMisses    += s.misses();
                totalEvictions += s.evictions();
            } finally {
                segment.lock.unlock();
            }
        }

        double hitRate = (totalHits + totalMisses) == 0
                ? 0.0
                : (double) totalHits / (totalHits + totalMisses);

        return new CacheStats(totalHits, totalMisses, totalEvictions, hitRate);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Determines which segment owns the given key.
     *
     * Steps:
     *   1. Get key.hashCode() — may be poorly distributed (e.g., all multiples of 16).
     *   2. Apply Wang/Jenkins secondary hash to spread bits across all positions.
     *      Same technique as Java 7 ConcurrentHashMap — prevents hot segments
     *      when the application's hashCode() is a bad distribution function.
     *   3. AND with mask to get segment index (fast power-of-2 modulo).
     *
     * @param key The key to route.
     * @return The segment responsible for this key.
     */
    private Segment<K, V> segmentFor(K key) {
        int h = key.hashCode();

        // Secondary hash (Wang/Jenkins) — improves distribution across segments
        h ^= (h >>> 20) ^ (h >>> 12);
        h ^= (h >>> 7)  ^ (h >>> 4);

        // Bitwise AND — equivalent to Math.abs(h) % numSegments but branch-free
        int index = h & mask;

        // Guard against Integer.MIN_VALUE edge case where mask gives negative index
        return segments[index < 0 ? index + numSegments : index];
    }

    // -------------------------------------------------------------------------
    // Diagnostic methods
    // -------------------------------------------------------------------------

    /**
     * Returns the number of segments. Useful in tests to verify configuration.
     */
    public int getNumSegments() {
        return numSegments;
    }

    /**
     * Returns the entry count for one specific segment by index.
     * Useful in tests to verify even key distribution across segments.
     *
     * @param segmentIndex 0 to numSegments-1.
     * @return Entries in that segment.
     */
    public int getSegmentSize(int segmentIndex) {
        if (segmentIndex < 0 || segmentIndex >= numSegments) {
            throw new IllegalArgumentException(
                    "Segment index out of range: " + segmentIndex +
                            " (numSegments=" + numSegments + ")"
            );
        }
        Segment<K, V> seg = segments[segmentIndex];
        seg.lock.lock();
        try {
            return seg.cache.size();
        } finally {
            seg.lock.unlock();
        }
    }

    /**
     * Returns which segment index a given key belongs to.
     * Useful in tests to verify that routing is consistent across calls.
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