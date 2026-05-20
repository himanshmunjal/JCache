package com.cache.core;

/**
 * Intrusive doubly linked list used internally by cache eviction policies.
 *
 * Design decisions:
 *  - Uses sentinel head and tail nodes so that every real node always has
 *    non-null prev and next. This eliminates all null checks in add/remove.
 *  - Package-private — this is an internal data structure, not part of the
 *    public API. Cache implementations in the policy package use it directly.
 *  - Not thread-safe on its own. The cache layer above is responsible for
 *    synchronization. Keeping locking out of this class lets us swap locking
 *    strategies (coarse, segmented, lock-free) without touching the list.
 *
 * Layout (most-recent → least-recent):
 *   head <-> [newest node] <-> ... <-> [oldest node] <-> tail
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class DoublyLinkedList<K, V> {

    // Sentinel nodes — never hold real data, never removed
    private final Node<K, V> head;
    private final Node<K, V> tail;

    private int size;

    public DoublyLinkedList() {
        head = new Node<>(null, null);
        tail = new Node<>(null, null);
        head.next = tail;
        tail.prev = head;
        size = 0;
    }

    // ── Core operations ───────────────────────────────────────────────────────

    /**
     * Inserts node immediately after head (most-recently-used position).
     * O(1).
     */
    public void addToFront(Node<K, V> node) {
        node.prev      = head;
        node.next      = head.next;
        head.next.prev = node;
        head.next      = node;
        size++;
    }

    /**
     * Unlinks node from wherever it currently sits in the list.
     * Caller is responsible for ensuring the node is actually in this list.
     * O(1) — no traversal needed because nodes carry their own prev/next.
     */
    public void remove(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        // Null out pointers to avoid subtle bugs if the node is reused
        node.prev = null;
        node.next = null;
        size--;
    }

    /**
     * Removes and returns the node just before the tail (least-recently-used).
     * Returns null if the list is empty.
     * O(1).
     */
    public Node<K, V> removeLast() {
        if (isEmpty()) return null;
        Node<K, V> last = tail.prev;
        remove(last);
        return last;
    }

    /**
     * Returns the node just before the tail without removing it.
     * Returns null if the list is empty.
     */
    public Node<K, V> peekLast() {
        if (isEmpty()) return null;
        return tail.prev;
    }

    /**
     * Returns the node just after the head (most-recently-used) without removing it.
     * Returns null if the list is empty.
     */
    public Node<K, V> peekFirst() {
        if (isEmpty()) return null;
        return head.next;
    }

    // ── Convenience ───────────────────────────────────────────────────────────

    /**
     * Moves an already-linked node to the front in O(1).
     * Used by LRU on every cache hit.
     */
    public void moveToFront(Node<K, V> node) {
        remove(node);
        addToFront(node);
    }

    public Node<K,V> removeFirst(){
        if(isEmpty())return null;
        Node<K,V> first = head.next;
        remove(first);
        return first;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DLL[");
        Node<K, V> curr = head.next;
        while (curr != tail) {
            sb.append(curr.key);
            if (curr.next != tail) sb.append(" <-> ");
            curr = curr.next;
        }
        sb.append("]");
        return sb.toString();
    }
}