package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CachePolicyType;
import com.cache.api.CacheStats;
import com.cache.policy.ARCCache;
import com.cache.policy.LFUCache;
import com.cache.policy.LRUCache;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Thread-safe cache split into independently locked segments, in the style
 * of the pre-Java 8 {@code ConcurrentHashMap}. Each key is routed to one
 * segment, so threads working on different segments never block each other.
 *
 * <p>Every segment runs its own instance of the chosen eviction policy over
 * its share of the capacity. Eviction is therefore per segment: the entry
 * evicted is the least recently (or frequently) used one in that segment,
 * not necessarily in the whole cache.
 *
 * <p>The total capacity is split exactly, so {@link #size()} never exceeds
 * it. If the capacity is smaller than the requested segment count, fewer
 * segments are used.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentedCache<K, V> implements Cache<K, V> {

    /** Segment count used when none is given. */
    public static final int DEFAULT_NUM_SEGMENTS = 16;

    private static final class Segment<K, V> {
        final ReentrantLock lock = new ReentrantLock();
        final Cache<K, V> cache;

        Segment(Cache<K, V> cache) {
            this.cache = cache;
        }
    }

    private final Segment<K, V>[] segments;
    private final int mask;

    /**
     * Creates an LRU cache with {@value #DEFAULT_NUM_SEGMENTS} segments.
     *
     * @param totalCapacity maximum number of entries across all segments
     */
    public SegmentedCache(int totalCapacity) {
        this(totalCapacity, DEFAULT_NUM_SEGMENTS, CachePolicyType.LRU);
    }

    /**
     * Creates an LRU cache.
     *
     * @param totalCapacity maximum number of entries across all segments
     * @param numSegments   number of segments, a power of two
     */
    public SegmentedCache(int totalCapacity, int numSegments) {
        this(totalCapacity, numSegments, CachePolicyType.LRU);
    }

    /**
     * Creates a cache with the given policy in every segment.
     *
     * @param totalCapacity maximum number of entries across all segments
     * @param numSegments   number of segments, a power of two
     * @param policy        eviction policy for each segment
     */
    @SuppressWarnings("unchecked")
    public SegmentedCache(int totalCapacity, int numSegments, CachePolicyType policy) {
        if (totalCapacity <= 0) {
            throw new IllegalArgumentException("totalCapacity must be > 0, got: " + totalCapacity);
        }
        if (numSegments <= 0 || Integer.bitCount(numSegments) != 1) {
            throw new IllegalArgumentException("numSegments must be a positive power of 2, got: " + numSegments);
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy cannot be null");
        }

        int count = Math.min(numSegments, Integer.highestOneBit(totalCapacity));
        this.segments = new Segment[count];
        this.mask = count - 1;

        int base = totalCapacity / count;
        int remainder = totalCapacity % count;
        for (int i = 0; i < count; i++) {
            int capacity = base + (i < remainder ? 1 : 0);
            segments[i] = new Segment<>(newPolicyCache(policy, capacity));
        }
    }

    @Override
    public V get(K key) {
        return withSegment(key, c -> c.get(key));
    }

    @Override
    public V peek(K key) {
        return withSegment(key, c -> c.peek(key));
    }

    @Override
    public void put(K key, V value) {
        withSegment(key, c -> {
            c.put(key, value);
            return null;
        });
    }

    @Override
    public void evict(K key) {
        withSegment(key, c -> {
            c.evict(key);
            return null;
        });
    }

    /**
     * Sums the segment sizes. Each segment is locked in turn, so the result is
     * not an atomic snapshot of the whole cache.
     */
    @Override
    public int size() {
        int[] total = {0};
        forEachSegment(c -> total[0] += c.size());
        return total[0];
    }

    @Override
    public CacheStats getStats() {
        long[] totals = new long[3];
        forEachSegment(c -> {
            CacheStats s = c.getStats();
            totals[0] += s.hits();
            totals[1] += s.misses();
            totals[2] += s.evictions();
        });
        return CacheStats.of(totals[0], totals[1], totals[2]);
    }

    @Override
    public void clear() {
        forEachSegment(Cache::clear);
    }

    /**
     * Returns the number of segments actually in use.
     *
     * @return the segment count
     */
    public int getNumSegments() {
        return segments.length;
    }

    /**
     * Returns how many entries one segment holds.
     *
     * @param segmentIndex index between 0 and {@code getNumSegments() - 1}
     * @return the segment's entry count
     */
    public int getSegmentSize(int segmentIndex) {
        if (segmentIndex < 0 || segmentIndex >= segments.length) {
            throw new IllegalArgumentException(
                    "Segment index out of range: " + segmentIndex + " (numSegments=" + segments.length + ")");
        }
        Segment<K, V> segment = segments[segmentIndex];
        segment.lock.lock();
        try {
            return segment.cache.size();
        } finally {
            segment.lock.unlock();
        }
    }

    /**
     * Returns the index of the segment that owns {@code key}.
     *
     * @param key the key
     * @return the segment index
     */
    public int getSegmentIndexFor(K key) {
        int h = key.hashCode();
        // Spread the bits so keys whose hash codes differ only in the high bits
        // do not all land in the same segment (same mix as Java 7's CHM).
        h ^= (h >>> 20) ^ (h >>> 12);
        h ^= (h >>> 7) ^ (h >>> 4);
        return h & mask;
    }

    private <R> R withSegment(K key, Function<Cache<K, V>, R> action) {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }
        Segment<K, V> segment = segments[getSegmentIndexFor(key)];
        segment.lock.lock();
        try {
            return action.apply(segment.cache);
        } finally {
            segment.lock.unlock();
        }
    }

    private void forEachSegment(Consumer<Cache<K, V>> action) {
        for (Segment<K, V> segment : segments) {
            segment.lock.lock();
            try {
                action.accept(segment.cache);
            } finally {
                segment.lock.unlock();
            }
        }
    }

    private static <K, V> Cache<K, V> newPolicyCache(CachePolicyType policy, int capacity) {
        return switch (policy) {
            case LRU -> new LRUCache<>(capacity);
            case LFU -> new LFUCache<>(capacity);
            case ARC -> new ARCCache<>(capacity);
        };
    }
}
