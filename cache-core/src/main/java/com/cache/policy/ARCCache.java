package com.cache.policy;

import com.cache.api.Cache;
import com.cache.api.CacheStats;
import com.cache.api.EvictionPolicy;
import com.cache.core.DoublyLinkedList;
import com.cache.core.Node;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;

import static com.cache.policy.LRUCache.requireKey;
import static com.cache.policy.LRUCache.requireValue;

/**
 * Adaptive Replacement Cache, as described by Megiddo and Modha in
 * "ARC: A Self-Tuning, Low Overhead Replacement Cache" (FAST '03).
 *
 * <p>Resident entries live in two LRU lists: {@code T1} for keys seen once
 * recently and {@code T2} for keys seen at least twice. Two ghost lists,
 * {@code B1} and {@code B2}, remember the keys (not values) recently evicted
 * from each. A put for a key found in a ghost list is evidence that the
 * corresponding resident list was too small, so the target size {@code p} of
 * {@code T1} is shifted towards it. Ghost lists are bounded so that
 * {@code |T1| + |B1| <= c} and {@code |T1| + |T2| + |B1| + |B2| <= 2c}.
 *
 * <p>The paper treats every request as "fetch on miss". Here a {@code get}
 * miss has no value to insert, so ghost hits are only acted on by
 * {@link #put}.
 *
 * <p>Not thread-safe.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ARCCache<K, V> implements Cache<K, V>, EvictionPolicy<K> {

    // Node.frequency doubles as a marker for which resident list a node is on.
    private static final int IN_T1 = 1;
    private static final int IN_T2 = 2;

    private final int capacity;
    private final Map<K, Node<K, V>> resident;
    private final DoublyLinkedList<K, V> t1 = new DoublyLinkedList<>();
    private final DoublyLinkedList<K, V> t2 = new DoublyLinkedList<>();
    // Insertion order is LRU order: the first element is the oldest ghost.
    private final LinkedHashSet<K> b1 = new LinkedHashSet<>();
    private final LinkedHashSet<K> b2 = new LinkedHashSet<>();

    private int p;

    private long hits;
    private long misses;
    private long evictions;

    /**
     * Creates an empty cache.
     *
     * @param capacity maximum number of resident entries, must be positive
     */
    public ARCCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be > 0, got: " + capacity);
        }
        this.capacity = capacity;
        this.resident = new HashMap<>(capacity);
    }

    @Override
    public V get(K key) {
        requireKey(key);
        Node<K, V> node = resident.get(key);
        if (node == null) {
            misses++;
            return null;
        }
        if (node.isExpired()) {
            evict(key);
            misses++;
            return null;
        }
        hits++;
        promote(node);
        return node.value;
    }

    @Override
    public void put(K key, V value) {
        put(key, value, Node.NO_EXPIRY);
    }

    /**
     * Stores a value that expires after {@code ttlMillis}.
     *
     * @param key       the key
     * @param value     the value
     * @param ttlMillis time to live in milliseconds, or {@link Node#NO_EXPIRY}
     */
    public void put(K key, V value, long ttlMillis) {
        requireKey(key);
        requireValue(value);

        Node<K, V> existing = resident.get(key);
        if (existing != null) {
            existing.value = value;
            existing.expireAfter(ttlMillis);
            promote(existing);
            return;
        }

        Node<K, V> node = new Node<>(key, value);
        node.expireAfter(ttlMillis);

        if (b1.contains(key)) {
            p = Math.min(capacity, p + Math.max(1, b2.size() / b1.size()));
            b1.remove(key);
            makeRoom(false);
            link(node, IN_T2);
        } else if (b2.contains(key)) {
            p = Math.max(0, p - Math.max(1, b1.size() / b2.size()));
            b2.remove(key);
            makeRoom(true);
            link(node, IN_T2);
        } else {
            trimGhostsForNewKey();
            link(node, IN_T1);
        }
    }

    @Override
    public void evict(K key) {
        Node<K, V> node = resident.remove(key);
        if (node != null) {
            listOf(node).remove(node);
        }
    }

    @Override
    public V peek(K key) {
        Node<K, V> node = resident.get(key);
        return node == null || node.isExpired() ? null : node.value;
    }

    @Override
    public int size() {
        return resident.size();
    }

    @Override
    public CacheStats getStats() {
        return CacheStats.of(hits, misses, evictions);
    }

    @Override
    public void clear() {
        resident.clear();
        t1.clear();
        t2.clear();
        b1.clear();
        b2.clear();
        p = 0;
    }

    @Override
    public void onAccess(K key) {
        Node<K, V> node = resident.get(key);
        if (node != null) {
            promote(node);
        }
    }

    @Override
    public void onInsert(K key) {
        // put() decides between T1 and T2 itself.
    }

    @Override
    public K evict() {
        return resident.isEmpty() ? null : replace(false);
    }

    /**
     * Returns the current target size of {@code T1}. Exposed for tests and
     * diagnostics.
     *
     * @return the adaptation parameter {@code p}
     */
    public int targetRecencySize() {
        return p;
    }

    /** Number of keys remembered in the two ghost lists. */
    int ghostCount() {
        return b1.size() + b2.size();
    }

    @Override
    public String toString() {
        return "ARCCache(capacity=" + capacity + ", size=" + size() + ", p=" + p
                + ", T1=" + t1 + ", T2=" + t2 + ", B1=" + b1 + ", B2=" + b2 + ")";
    }

    /** Case I of the paper: a hit moves the entry to the MRU end of T2. */
    private void promote(Node<K, V> node) {
        if (node.frequency == IN_T1) {
            t1.remove(node);
            node.frequency = IN_T2;
            t2.addToFront(node);
        } else {
            t2.moveToFront(node);
        }
    }

    /**
     * Case IV of the paper: before admitting a key that is in no list, keep
     * the ghost lists within their bounds and free a slot if the cache is full.
     */
    private void trimGhostsForNewKey() {
        int l1 = t1.size() + b1.size();
        int total = l1 + t2.size() + b2.size();
        if (l1 >= capacity) {
            if (t1.size() < capacity) {
                removeOldest(b1);
                makeRoom(false);
            } else {
                // T1 alone fills the cache; drop its LRU entry without remembering it.
                Node<K, V> victim = t1.removeLast();
                resident.remove(victim.key);
                evictions++;
            }
        } else if (total >= capacity) {
            if (total >= 2 * capacity) {
                removeOldest(b2);
            }
            makeRoom(false);
        }
    }

    private void makeRoom(boolean hitInB2) {
        if (resident.size() >= capacity) {
            replace(hitInB2);
        }
    }

    /**
     * The paper's REPLACE subroutine: evicts the LRU entry of T1 or T2,
     * depending on how T1 compares with its target size, and remembers the key
     * in the matching ghost list.
     */
    private K replace(boolean hitInB2) {
        Node<K, V> victim;
        boolean fromT1 = t2.isEmpty() || t1.size() > p || (hitInB2 && t1.size() == p);
        if (!t1.isEmpty() && fromT1) {
            victim = t1.removeLast();
            b1.add(victim.key);
        } else {
            victim = t2.removeLast();
            b2.add(victim.key);
        }
        resident.remove(victim.key);
        evictions++;
        return victim.key;
    }

    private void link(Node<K, V> node, int list) {
        node.frequency = list;
        listOf(node).addToFront(node);
        resident.put(node.key, node);
    }

    private DoublyLinkedList<K, V> listOf(Node<K, V> node) {
        return node.frequency == IN_T1 ? t1 : t2;
    }

    private static <K> void removeOldest(LinkedHashSet<K> ghosts) {
        Iterator<K> it = ghosts.iterator();
        if (it.hasNext()) {
            it.next();
            it.remove();
        }
    }
}
