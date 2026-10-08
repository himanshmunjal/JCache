package com.example.jcache;

import com.cache.client.CacheClient;
import com.cache.client.ConnectionPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Cache-aside reads from many threads through a {@link ConnectionPool}: look
 * in the cache, and on a miss load from the source of truth and store the
 * result with a TTL.
 *
 * <pre>mvn compile exec:java -Dexec.mainClass=com.example.jcache.CacheAside</pre>
 */
public class CacheAside {

    private final ConnectionPool pool;
    private final Function<String, String> loader;
    private final long ttlSeconds;

    /**
     * @param pool       connections to the server
     * @param loader     loads a value on a miss; may return {@code null}
     * @param ttlSeconds TTL for loaded values
     */
    public CacheAside(ConnectionPool pool, Function<String, String> loader, long ttlSeconds) {
        this.pool = pool;
        this.loader = loader;
        this.ttlSeconds = ttlSeconds;
    }

    /** @return the cached or freshly loaded value, or {@code null} if the loader has none */
    public String get(String key) throws IOException, TimeoutException, InterruptedException {
        CacheClient conn = pool.acquire();
        try {
            String value = conn.get(key);
            if (value == null) {
                value = loader.apply(key);
                if (value != null) {
                    conn.put(key, value, ttlSeconds);
                }
            }
            return value;
        } finally {
            pool.release(conn);
        }
    }

    /** Drops a key after the source of truth changed. */
    public void invalidate(String key) throws IOException, TimeoutException, InterruptedException {
        CacheClient conn = pool.acquire();
        try {
            conn.delete(key);
        } finally {
            pool.release(conn);
        }
    }

    public static void main(String[] args) throws Exception {
        JCacheSettings settings = JCacheSettings.fromEnv();
        Map<String, String> database = Map.of(
                "product:1", "Keyboard",
                "product:2", "Mouse",
                "product:3", "Monitor");
        AtomicInteger loads = new AtomicInteger();
        Function<String, String> slowLoad = key -> {
            loads.incrementAndGet();
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return database.get(key);
        };

        try (ConnectionPool pool = new ConnectionPool(settings.host(), settings.port(), 8)) {
            CacheAside products = new CacheAside(pool, slowLoad, 300);
            for (String key : database.keySet()) {
                products.invalidate(key);
            }

            ExecutorService workers = Executors.newFixedThreadPool(8);
            int requests = 1_000;
            for (int i = 0; i < requests; i++) {
                String key = "product:" + (i % 3 + 1);
                workers.submit(() -> products.get(key));
            }
            workers.shutdown();
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Requests did not finish in 30 seconds");
            }

            System.out.println(requests + " reads, " + loads.get() + " loads from the database");
            System.out.println("product:2 -> " + products.get("product:2"));
        }
    }
}
