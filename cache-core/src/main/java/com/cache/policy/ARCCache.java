package com.cache.policy;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.api.EvictionPolicy;
import com.cache.core.DoublyLinkedList;
import com.cache.core.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ARC (Adaptive Replacement Cache)
 *
 * ARC improves upon LRU by balancing:
 *  1. Recency  -> recently accessed items
 *  2. Frequency -> repeatedly accessed items
 *
 * Unlike plain LRU, ARC adapts automatically to workload patterns.
 *
 * ARC maintains four lists:
 *
 * T1 -> Recently used entries (seen once)
 * T2 -> Frequently used entries (seen multiple times)
 *
 * B1 -> Ghost entries evicted from T1
 * B2 -> Ghost entries evicted from T2
 *
 * Ghost lists store ONLY KEYS, not values.
 * They help ARC learn whether workload prefers:
 *  - recency
 *  - frequency
 *
 * Layout:
 *
 * Real cache:
 *   T1 + T2
 *
 * Ghost history:
 *   B1 + B2
 *
 * Adaptive parameter:
 *   p -> target size for T1
 *
 * If workload repeatedly hits B1:
 *   increase p -> favor recency
 *
 * If workload repeatedly hits B2:
 *   decrease p -> favor frequency
 *
 * NOTE:
 * This is a simplified educational ARC implementation.
 * Production ARC implementations are significantly more complex.
 */
public class ARCCache<K, V> implements Cache<K, V>, EvictionPolicy<K> {

    private final int capacity;

    /**
     * Main key -> node lookup table.
     *
     * Contains ONLY real cache entries.
     * Does NOT include ghost entries.
     */
    private final Map<K, Node<K, V>> cache;

    /**
     * Ghost caches.
     *
     * These store keys only.
     *
     * B1 remembers keys evicted from T1.
     * B2 remembers keys evicted from T2.
     */
    private final Map<K, Boolean> b1Ghost;
    private final Map<K, Boolean> b2Ghost;

    /**
     * T1:
     * Recent entries accessed only once.
     */
    private final DoublyLinkedList<K, V> t1;

    /**
     * T2:
     * Frequently used entries.
     */
    private final DoublyLinkedList<K, V> t2;

    /**
     * Adaptive target size for T1.
     *
     * Range:
     * 0 <= p <= capacity
     */
    private int p = 0;

    // Stats
    private final AtomicLong hits      = new AtomicLong(0);
    private final AtomicLong misses    = new AtomicLong(0);
    private final AtomicLong evictions = new AtomicLong(0);

    public ARCCache(int capacity) {

        if (capacity <= 0) {
            throw new IllegalArgumentException(
                    "Capacity must be > 0, got: " + capacity
            );
        }

        this.capacity = capacity;

        this.cache = new HashMap<>(capacity);

        this.b1Ghost = new HashMap<>();
        this.b2Ghost = new HashMap<>();

        this.t1 = new DoublyLinkedList<>();
        this.t2 = new DoublyLinkedList<>();
    }

    // GET

    /**
     * ARC access rules:
     *
     * 1. If entry exists in T1:
     *      move to T2 (becomes frequent)
     *
     * 2. If entry exists in T2:
     *      move to front of T2
     *
     * 3. If entry absent:
     *      cache miss
     */
    @Override
    public V get(K key) {

        Node<K, V> node = cache.get(key);

        if (node == null) {
            misses.incrementAndGet();
            return null;
        }

        if (node.isExpired()) {
            evict(key);
            misses.incrementAndGet();
            return null;
        }

        hits.incrementAndGet();

        onAccess(key);

        return node.value;
    }

    // PUT

    @Override
    public void put(K key, V value) {
        put(key, value, -1);
    }

    /**
     * ARC insertion logic:
     *
     * CASE 1:
     *   Existing key -> update + access
     *
     * CASE 2:
     *   Key exists in B1 ghost
     *   -> increase p
     *   -> replace()
     *   -> move into T2
     *
     * CASE 3:
     *   Key exists in B2 ghost
     *   -> decrease p
     *   -> replace()
     *   -> move into T2
     *
     * CASE 4:
     *   Completely new key
     *   -> insert into T1
     */
    public void put(K key, V value, long ttlMillis) {

        Node<K, V> existing = cache.get(key);

        // Existing real cache entry
        if (existing != null) {

            existing.value = value;

            existing.expiryTime = (ttlMillis == -1)
                    ? -1
                    : System.currentTimeMillis() + ttlMillis;

            onAccess(key);

            return;
        }

        // Ghost hit in B1
        if (b1Ghost.containsKey(key)) {

            // Increase recency preference
            p = Math.min(capacity, p + 1);

            replace(key);

            b1Ghost.remove(key);

            Node<K, V> node = createNode(key, value, ttlMillis);

            cache.put(key, node);

            // Directly promote into T2
            t2.addToFront(node);

            return;
        }

        // Ghost hit in B2
        if (b2Ghost.containsKey(key)) {

            // Increase frequency preference
            p = Math.max(0, p - 1);

            replace(key);

            b2Ghost.remove(key);

            Node<K, V> node = createNode(key, value, ttlMillis);

            cache.put(key, node);

            t2.addToFront(node);

            return;
        }

        // New entry

        if (size() >= capacity) {
            replace(key);
        }

        Node<K, V> node = createNode(key, value, ttlMillis);

        cache.put(key, node);

        // New entries always start in T1
        t1.addToFront(node);
    }

    // ACCESS HANDLING

    /**
     * ARC promotion rules:
     *
     * T1 hit:
     *   move to T2
     *
     * T2 hit:
     *   move to front of T2
     */
    @Override
    public void onAccess(K key) {

        Node<K, V> node = cache.get(key);

        if (node == null) {
            return;
        }

        // Try removing from T1
        try {

            t1.remove(node);

            // Promote into T2
            t2.addToFront(node);

            return;

        } catch (Exception ignored) {
        }

        // Otherwise already in T2
        t2.moveToFront(node);
    }

    @Override
    public void onInsert(K key) {
        // No-op
    }

    // REPLACEMENT LOGIC

    /**
     * ARC replacement policy.
     *
     * Decides whether eviction should come from:
     *  - T1 (recent)
     *  - T2 (frequent)
     *
     * based on adaptive parameter p.
     */
    private void replace(K incomingKey) {

        // Prefer evicting from T1
        if (!t1.isEmpty() && (t1.size() > p)) {

            Node<K, V> victim = t1.removeLast();

            if (victim != null) {

                cache.remove(victim.key);

                // Add key into ghost history
                b1Ghost.put(victim.key, true);

                evictions.incrementAndGet();
            }

            return;
        }

        // Otherwise evict from T2
        Node<K, V> victim = t2.removeLast();

        if (victim != null) {

            cache.remove(victim.key);

            // Add key into ghost history
            b2Ghost.put(victim.key, true);

            evictions.incrementAndGet();
        }
    }

    // EXPLICIT EVICTION

    @Override
    public void evict(K key) {

        Node<K, V> node = cache.remove(key);

        if (node == null) {
            return;
        }

        try {
            t1.remove(node);
        } catch (Exception ignored) {

            try {
                t2.remove(node);
            } catch (Exception ignoredAgain) {
            }
        }

        evictions.incrementAndGet();
    }

    /**
     * Evicts one entry using ARC replacement logic.
     */
    @Override
    public K evict() {

        Node<K, V> victim;

        if (!t1.isEmpty() && t1.size() > p) {
            victim = t1.peekLast();
        } else {
            victim = t2.peekLast();
        }

        if (victim == null) {
            return null;
        }

        K key = victim.key;

        evict(key);

        return key;
    }

    // HELPERS

    private Node<K, V> createNode(K key, V value, long ttlMillis) {

        return (ttlMillis == -1)
                ? new Node<>(key, value)
                : new Node<>(key, value, ttlMillis);
    }

    // CACHE INFO

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public CacheStats getstats() {

        long h = hits.get();
        long m = misses.get();

        long total = h + m;

        double hitRate =
                (total == 0)
                        ? 0.0
                        : (double) h / total;

        return new CacheStats(
                h,
                m,
                evictions.get(),
                hitRate
        );
    }

    // DEBUG

    @Override
    public String toString() {

        return "ARCCache(" +
                "capacity=" + capacity +
                ", size=" + size() +
                ", p=" + p +
                ", T1=" + t1 +
                ", T2=" + t2 +
                ", B1=" + b1Ghost.keySet() +
                ", B2=" + b2Ghost.keySet() +
                ")";
    }
}