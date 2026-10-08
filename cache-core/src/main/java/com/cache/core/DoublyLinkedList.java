package com.cache.core;

/**
 * Intrusive doubly linked list with sentinel head and tail nodes, so that
 * insertions and removals never need null checks. The front of the list is
 * the most recently used end.
 *
 * <p>Not thread-safe. Callers are expected to guard it with whatever lock
 * protects the surrounding cache.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class DoublyLinkedList<K, V> {

    private final Node<K, V> head = new Node<>(null, null);
    private final Node<K, V> tail = new Node<>(null, null);
    private int size;

    /** Creates an empty list. */
    public DoublyLinkedList() {
        head.next = tail;
        tail.prev = head;
    }

    /**
     * Inserts {@code node} at the front. The node must not currently be linked.
     *
     * @param node the node to insert
     */
    public void addToFront(Node<K, V> node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
        size++;
    }

    /**
     * Unlinks {@code node}. The caller must make sure the node belongs to this
     * list; removing a node that is already unlinked is a no-op.
     *
     * @param node the node to remove
     */
    public void remove(Node<K, V> node) {
        if (node == null || (node.prev == null && node.next == null)) {
            return;
        }
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
        size--;
    }

    /**
     * Moves an already linked node to the front.
     *
     * @param node the node to move
     */
    public void moveToFront(Node<K, V> node) {
        remove(node);
        addToFront(node);
    }

    /**
     * Removes and returns the last (least recently used) node.
     *
     * @return the removed node, or {@code null} if the list is empty
     */
    public Node<K, V> removeLast() {
        if (isEmpty()) {
            return null;
        }
        Node<K, V> last = tail.prev;
        remove(last);
        return last;
    }

    /**
     * Removes and returns the first (most recently used) node.
     *
     * @return the removed node, or {@code null} if the list is empty
     */
    public Node<K, V> removeFirst() {
        if (isEmpty()) {
            return null;
        }
        Node<K, V> first = head.next;
        remove(first);
        return first;
    }

    /**
     * Returns the last node without removing it.
     *
     * @return the last node, or {@code null} if the list is empty
     */
    public Node<K, V> peekLast() {
        return isEmpty() ? null : tail.prev;
    }

    /**
     * Returns the first node without removing it.
     *
     * @return the first node, or {@code null} if the list is empty
     */
    public Node<K, V> peekFirst() {
        return isEmpty() ? null : head.next;
    }

    /** Drops every node. Nodes that were linked are left with stale pointers. */
    public void clear() {
        head.next = tail;
        tail.prev = head;
        size = 0;
    }

    /**
     * Returns whether the list has no nodes.
     *
     * @return {@code true} if empty
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the number of linked nodes.
     *
     * @return the node count
     */
    public int size() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (Node<K, V> n = head.next; n != tail; n = n.next) {
            sb.append(n.key);
            if (n.next != tail) {
                sb.append(" <-> ");
            }
        }
        return sb.append(']').toString();
    }
}
