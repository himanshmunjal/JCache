package com.example.jcache;

import com.cache.client.CacheClient;
import com.cache.client.CacheClient.CacheClientException;

/**
 * Sends commands faster than the server's per-connection rate limit allows,
 * first without retries to show the rejections, then through
 * {@link RateLimitRetry}. Start the server with {@code JCACHE_RATE_LIMIT} set;
 * see the README.
 *
 * <pre>mvn compile exec:java -Dexec.mainClass=com.example.jcache.RateLimitDemo</pre>
 */
public final class RateLimitDemo {

    private static final int COMMANDS = 50;

    private RateLimitDemo() {
    }

    public static void main(String[] args) throws Exception {
        JCacheSettings settings = JCacheSettings.fromEnv();

        try (CacheClient client = new CacheClient(settings.host(), settings.port())) {
            int accepted = 0;
            int rejected = 0;
            for (int i = 0; i < COMMANDS; i++) {
                try {
                    client.put("rl:demo:" + i, "value " + i);
                    accepted++;
                } catch (CacheClientException e) {
                    if (!RateLimitRetry.isRateLimited(e)) {
                        throw e;
                    }
                    rejected++;
                }
            }
            System.out.println(COMMANDS + " PUTs without retry -> " + accepted + " accepted, " + rejected + " rejected");
            if (rejected == 0) {
                System.out.println("Nothing was rejected: this server has no rate limit (JCACHE_RATE_LIMIT=0)");
            }
            System.out.println("PING is not limited -> " + client.ping());

            RateLimitRetry retry = new RateLimitRetry(8, 25);
            long start = System.nanoTime();
            for (int i = 0; i < COMMANDS; i++) {
                String key = "rl:demo:" + i;
                retry.call(() -> {
                    client.put(key, "value");
                    return null;
                });
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            System.out.println(COMMANDS + " PUTs with retry    -> all accepted in " + elapsedMs + " ms, "
                    + retry.retries() + " retries");

            for (int i = 0; i < COMMANDS; i++) {
                String key = "rl:demo:" + i;
                retry.call(() -> {
                    client.delete(key);
                    return null;
                });
            }
            String rateLimited = retry.call(() -> client.stats().get("rateLimited"));
            System.out.println("STATS rateLimited  -> " + rateLimited + " (server total)");
        }
    }
}
