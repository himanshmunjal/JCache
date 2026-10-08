package com.example.jcache;

import com.cache.client.CacheClient;

import java.io.IOException;

/**
 * Connects to one server and runs every basic command once.
 *
 * <pre>mvn compile exec:java</pre>
 */
public final class QuickStart {

    private QuickStart() {
    }

    public static void main(String[] args) throws IOException {
        JCacheSettings settings = JCacheSettings.fromEnv();

        try (CacheClient client = new CacheClient(settings.host(), settings.port())) {
            if (!client.ping()) {
                throw new IllegalStateException("No PONG from " + settings.host() + ":" + settings.port());
            }
            System.out.println("Connected to " + settings.host() + ":" + settings.port());

            client.put("user:1", "Alice Smith");
            System.out.println("GET user:1        -> " + client.get("user:1"));

            client.put("otp:1", "493021", 60);
            System.out.println("TTL otp:1         -> " + client.ttl("otp:1") + "s");

            client.persist("otp:1");
            System.out.println("TTL after PERSIST -> " + client.ttl("otp:1"));

            client.delete("user:1");
            client.delete("otp:1");
            System.out.println("GET after DELETE  -> " + client.get("user:1"));

            System.out.println("STATS             -> " + client.stats());
        }
    }
}
