package com.example.jcache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

/**
 * Uses JCache from Jedis, a Redis client, over RESP on the same port as the
 * text protocol. Replies follow Redis: {@code null} for a missing key,
 * {@code -2} from {@code TTL}, counts from {@code DEL} and {@code EXISTS}.
 *
 * <pre>mvn compile exec:java -Dexec.mainClass=com.example.jcache.RedisClientExample</pre>
 */
public final class RedisClientExample {

    private RedisClientExample() {
    }

    public static void main(String[] args) {
        JCacheSettings settings = JCacheSettings.fromEnv();

        try (Jedis jedis = new Jedis(settings.host(), settings.port())) {
            System.out.println("PING                -> " + jedis.ping());

            jedis.set("user:1", "Alice Smith", SetParams.setParams().ex(3600));
            System.out.println("GET user:1          -> " + jedis.get("user:1"));

            jedis.setex("otp:1", 60, "493021");
            System.out.println("TTL otp:1           -> " + jedis.ttl("otp:1"));

            // RESP values are binary-safe, so line breaks are allowed here but not in the text protocol.
            jedis.set("note:1", "line one\nline two");
            System.out.println("GET note:1          -> " + jedis.get("note:1").replace("\n", "\\n"));

            System.out.println("EXISTS user:1 user:2 -> " + jedis.exists("user:1", "user:2"));
            System.out.println("DBSIZE              -> " + jedis.dbSize());
            System.out.println("DEL 4 keys          -> " + jedis.del("user:1", "otp:1", "note:1", "user:2"));
            System.out.println("GET user:1          -> " + jedis.get("user:1"));
            System.out.println("TTL user:1          -> " + jedis.ttl("user:1"));

            jedis.info("stats").lines()
                    .filter(line -> line.startsWith("hitRate:") || line.startsWith("rateLimited:"))
                    .forEach(line -> System.out.println("INFO " + line));
        }
    }
}
