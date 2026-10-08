package com.cache.api;

/** The eviction policies available out of the box. */
public enum CachePolicyType {
    /** Least recently used. */
    LRU,
    /** Least frequently used, with periodic frequency decay. */
    LFU,
    /** Adaptive Replacement Cache (Megiddo and Modha, 2003). */
    ARC
}
