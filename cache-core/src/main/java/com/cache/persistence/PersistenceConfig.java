package com.cache.persistence;

public record PersistenceConfig(
        boolean enabled,
        String  snapshotBasePath,
        long    snapshotIntervalMs,
        int     fsyncEveryNWrites
) {
    public static PersistenceConfig disabled() {
        return new PersistenceConfig(false, "./jcache", 300_000, 100);
    }
    public static PersistenceConfig enabled(String basePath) {
        return new PersistenceConfig(true, basePath, 300_000L, 100);
    }
}