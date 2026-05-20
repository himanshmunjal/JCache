package com.cache.api;

public interface Cache <K,V>{
    V get(K key);
    void put(K key, V value);
    void evict(K key);
    int size();
    CacheStats getstats();
}