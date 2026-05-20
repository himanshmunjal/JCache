package com.cache.concurrent;

import com.cache.api.Cache;
import com.cache.api.CacheStats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LockFreeCache — a high-throughput cache implementation that eliminates
 * locks on the read path entirely using Java's atomic primitives and
 * ConcurrentHashMap.
 *
 * -------------------------------------------------------------------------
 * WHY LOCK-FREE?
 * -------------------------------------------------------------------------
 * CoarseGrainedCache  → one lock for the entire structure.
 *                        Every thread blocks every other thread.
 *                        Simple but terrible under contention.
 *
 * SegmentedCache      → one lock per segment (16 by default).
 *                        Threads only block threads in the same segment.
 *                        Good throughput, still has lock overhead.
 *
 * LockFreeCache       → no locks on get() at all.
 *                        ConcurrentHashMap handles concurrent reads natively.
 *                        AtomicLong / AtomicInteger for all counters.
 *                        Only eviction (structural change) needs coordination.
 *
 * In real systems, reads vastly outnumber writes (often 90:10 or 99:1).
 * Optimising the read path gives the highest overall throughput gain.
 *
 * -------------------------------------------------------------------------
 * DESIGN: LRU approximation with a ConcurrentHashMap
 * -------------------------------------------------------------------------
 * True O(1) LRU requires a doubly-linked list + HashMap. The list gives
 * ordering, but updating it on every read requires a lock (you must move
 * the accessed node to the front atomically with respect to other readers).
 *
 * This is the fundamental tension: exact LRU + lock-free reads = impossible
 * without complex non-blocking data structures (like those in Caffeine's
 * Window-TinyLFU, which uses a striped ring buffer to defer ordering updates).
 *
 * Our compromise — a practical approximation used by many real caches:
 *
 *   - ConcurrentHashMap<K, CacheEntry<K,V>> stores all data.
 *   - Each CacheEntry has an AtomicLong `lastAccessTime` updated on read.
 *   - get() is fully lock-free: just a map lookup + AtomicLong write.
 *   - put() is lock-free for the map write itself.
 *   - Eviction (when capacity is exceeded) finds the entry with the
 *     oldest `lastAccessTime` — this IS a scan, O(n), but eviction is
 *     rare relative to reads and acceptable in practice.
 *
 * This gives you:
 *   ✅ Lock-free reads (the dominant operation)
 *   ✅ Lock-free writes for non-full cache
 *   ✅ Lock-free stats (AtomicLong everywhere)
 *   ⚠️  O(n) eviction scan — acceptable because eviction is infrequent
 *
 * If you want exact O(1) LRU with lock-free reads, look at Caffeine's
 * source — it's a graduate-level data structure and overkill here.
 *
 * -------------------------------------------------------------------------
 * THREAD SAFETY GUARANTEES
 * -------------------------------------------------------------------------
 * - get()  : fully lock-free, safe for unlimited concurrent readers.
 * - put()  : ConcurrentHashMap.put() is thread-safe. Eviction uses
 *            a single AtomicReference CAS to claim the "eviction slot"
 *            so only one thread evicts at a time, others just insert.
 * - evict(): ConcurrentHashMap.remove() is thread-safe.
 * - stats  : all counters are AtomicLong — no lock needed.
 *
 * @param <K> Key type. Must correctly implement equals() and hashCode().
 * @param <V> Value type.
 */
public class LockFreeCache<K, V> implements Cache<K, V> {

    // -------------------------------------------------------------------------
    // Inner class: CacheEntry
    // -------------------------------------------------------------------------

    /**
     * Wraps a value with its access metadata.
     *
     * lastAccessTime is AtomicLong so it can be updated on every get()
     * without any lock. Multiple threads may race to update it — that's
     * fine. We don't need perfect ordering, just a recent-enough timestamp
     * to approximate LRU for eviction decisions.
     */
    private static class CacheEntry<K, V> {

        final K key;
        final V value;

        /**
         * Timestamp of the last access, in nanoseconds (System.nanoTime()).
         * We use nanoTime (not currentTimeMillis) because:
         *   - It's monotonic — never goes backward.
         *   - Higher resolution — important when many accesses happen
         *     within the same millisecond.
         *   - Not wall-clock — don't use for TTL, only for ordering.
         */
        final AtomicLong lastAccessTime;

        CacheEntry(K key, V value) {
            this.key            = key;
            this.value          = value;
            this.lastAccessTime = new AtomicLong(System.nanoTime());
        }

        /** Updates the access timestamp — called on every get(). */
        void touch() {
            lastAccessTime.set(System.nanoTime());
        }
    }

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /**
     * The backing store. ConcurrentHashMap gives us:
     *   - Thread-safe reads with no locking (uses volatile reads internally).
     *   - Striped locking on writes (16 stripes by default) — much better
     *     than a single lock.
     *   - Weakly consistent iterators — safe to iterate while other threads modify.
     */
    private final ConcurrentHashMap<K, CacheEntry<K, V>> map;

    /** Maximum number of entries before eviction fires. */
    private final int capacity;

    /**
     * Guards the eviction path. When the map exceeds capacity, the thread
     * that successfully CAS-es this from false→true performs eviction.
     * Other threads that arrive during eviction just skip — slight
     * over-capacity is acceptable vs. a lock.
     *
     * Using AtomicReference<Boolean> instead of AtomicBoolean for visibility
     * clarity — the semantics are identical.
     */
    private final AtomicReference<Boolean> evicting;

    // Stats — all AtomicLong, zero locks needed.
    private final AtomicLong hits;
    private final AtomicLong misses;
    private final AtomicLong evictions;
    private final AtomicLong totalPuts;

    /**
     * Current size tracked separately from map.size().
     * map.size() on ConcurrentHashMap is O(n) — it sums across segments.
     * AtomicInteger gives O(1) size() at the cost of slight imprecision
     * during concurrent puts (acceptable).
     */
    private final AtomicInteger currentSize;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a LockFreeCache with the given maximum capacity.
     *
     * Initial capacity of ConcurrentHashMap is set to (capacity * 4/3) + 1
     * to avoid rehashing — ConcurrentHashMap rehashes at 75% load factor,
     * so we pre-size it to hold `capacity` entries without rehashing.
     *
     * @param capacity Maximum number of entries. Must be >= 1.
     */
    public LockFreeCache(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException(
                    "Capacity must be >= 1, got: " + capacity
            );
        }
        this.capacity    = capacity;
        // Pre-size to avoid internal rehashing up to our capacity.
        int initialSize  = (int) (capacity * (4.0 / 3.0)) + 1;
        this.map         = new ConcurrentHashMap<>(initialSize);
        this.evicting    = new AtomicReference<>(Boolean.FALSE);
        this.hits        = new AtomicLong(0);
        this.misses      = new AtomicLong(0);
        this.evictions   = new AtomicLong(0);
        this.totalPuts   = new AtomicLong(0);
        this.currentSize = new AtomicInteger(0);
    }

    // -------------------------------------------------------------------------
    // Cache<K,V> interface implementation
    // -------------------------------------------------------------------------

    /**
     * Retrieves the value for the given key.
     *
     * FULLY LOCK-FREE: this method acquires no locks.
     *   1. ConcurrentHashMap.get() uses volatile reads internally — no lock.
     *   2. entry.touch() does an AtomicLong.set() — no lock.
     *   3. hits/misses increment uses AtomicLong.incrementAndGet() — no lock.
     *
     * This is the payoff for the complexity in put() and eviction.
     * Under read-heavy workloads (90%+ reads), this method dominates
     * execution time, and having it lock-free gives massive throughput gains.
     *
     * @param key The key to look up.
     * @return The value, or null if not present.
     */
    @Override
    public V get(K key) {
        CacheEntry<K, V> entry = map.get(key);

        if (entry == null) {
            // Cache miss — key not present.
            misses.incrementAndGet();
            return null;
        }

        // Cache hit — update access time for LRU approximation.
        // This is a single AtomicLong.set() — no lock, no CAS loop.
        entry.touch();
        hits.incrementAndGet();
        return entry.value;
    }

    /**
     * Inserts or updates a key-value pair.
     *
     * Steps:
     *   1. Check if key already exists (update path — no eviction needed).
     *   2. If new key: check capacity. If over, trigger eviction.
     *   3. Insert into ConcurrentHashMap.
     *
     * The eviction path uses a CAS on `evicting` so only one thread
     * evicts at a time. Other threads proceed past the capacity check —
     * the map may momentarily hold capacity+1 entries, which is fine.
     *
     * @param key   Key to insert. Cannot be null.
     * @param value Value to store. Cannot be null.
     */
    @Override
    public void put(K key, V value) {
        if (key == null)   throw new IllegalArgumentException("Key cannot be null");
        if (value == null) throw new IllegalArgumentException("Value cannot be null");

        totalPuts.incrementAndGet();

        // Check if this is an update (key already present).
        // computeIfPresent is atomic at the map level — no lock from our side.
        boolean isUpdate = map.containsKey(key);

        if (!isUpdate && currentSize.get() >= capacity) {
            // We're at capacity and this is a new key — evict first.
            // CAS: only one thread performs eviction at a time.
            if (evicting.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
                try {
                    evictLeastRecentlyUsed();
                } finally {
                    // Always release the eviction lock, even on exception.
                    evicting.set(Boolean.FALSE);
                }
            }
            // If another thread is already evicting (CAS failed), we proceed
            // and insert anyway. The map will temporarily hold one extra entry.
            // This is the "optimistic" part of lock-free design.
        }

        // Insert or update. ConcurrentHashMap.put() is internally thread-safe.
        CacheEntry<K, V> newEntry  = new CacheEntry<>(key, value);
        CacheEntry<K, V> oldEntry  = map.put(key, newEntry);

        // Adjust size counter only for genuine inserts (not updates).
        if (oldEntry == null) {
            currentSize.incrementAndGet();
        }
    }

    /**
     * Removes a key from the cache.
     *
     * ConcurrentHashMap.remove() is thread-safe — no additional locking needed.
     *
     * @param key The key to remove.
     */
    @Override
    public void evict(K key) {
        CacheEntry<K, V> removed = map.remove(key);
        if (removed != null) {
            currentSize.decrementAndGet();
            evictions.incrementAndGet();
        }
    }

    /**
     * Returns the current number of entries in the cache.
     *
     * Uses AtomicInteger.get() — O(1) and lock-free.
     * Note: may be momentarily imprecise during concurrent puts
     * (off by at most the number of concurrent inserting threads).
     *
     * @return Current entry count.
     */
    @Override
    public int size() {
        return currentSize.get();
    }

    /**
     * Returns a snapshot of cache statistics.
     * All fields are read from AtomicLong — no lock needed.
     *
     * @return Immutable CacheStats snapshot.
     */
    @Override
    public CacheStats getstats() {
        long h = hits.get();
        long m = misses.get();
        long e = evictions.get();
        long total = h + m;
        double hitRate = total == 0 ? 0.0 : (double) h / total;
        return new CacheStats(h, m, e, hitRate);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Evicts the entry with the oldest lastAccessTime (LRU approximation).
     *
     * This is O(n) — we scan all entries to find the oldest.
     * This is the deliberate trade-off: reads are O(1) lock-free,
     * eviction is O(n) but rare.
     *
     * In Caffeine (the production-grade Java cache), this is replaced
     * with a per-thread ring buffer that accumulates access events and
     * drains them in batch — but that's 2000+ lines of code.
     *
     * Called only when: map.size() >= capacity AND we won the eviction CAS.
     */
    private void evictLeastRecentlyUsed() {
        CacheEntry<K, V> lruEntry    = null;
        long             oldestTime  = Long.MAX_VALUE;

        // Weakly consistent iteration — safe under concurrent modification.
        // We may miss entries added after iteration started — fine, we just
        // need to evict SOME old entry, not necessarily the oldest globally.
        for (CacheEntry<K, V> entry : map.values()) {
            long accessTime = entry.lastAccessTime.get();
            if (accessTime < oldestTime) {
                oldestTime = accessTime;
                lruEntry   = entry;
            }
        }

        if (lruEntry != null) {
            // Remove using the key-value overload to avoid removing a
            // concurrently updated entry. If the entry was updated between
            // our scan and this remove, map.remove(key, value) will fail
            // (returns false) and we skip — acceptable, the entry is fresh.
            if (map.remove(lruEntry.key, lruEntry)) {
                currentSize.decrementAndGet();
                evictions.incrementAndGet();
            }
        }
    }
}