package com.cache.api;

public interface EvictionPolicy <K>{
    void onAccess(K key);
    void onInsert(K key);
    K evict();
}
