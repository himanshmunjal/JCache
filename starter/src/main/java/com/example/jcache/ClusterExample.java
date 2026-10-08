package com.example.jcache;

import com.cache.client.ClusterCacheClient;

/**
 * Shards keys over several servers with {@link ClusterCacheClient}. Start the
 * three-node cluster from the JCache repository first, or set
 * {@code JCACHE_NODES}.
 *
 * <pre>mvn compile exec:java -Dexec.mainClass=com.example.jcache.ClusterExample</pre>
 */
public final class ClusterExample {

    private ClusterExample() {
    }

    public static void main(String[] args) throws Exception {
        JCacheSettings settings = JCacheSettings.fromEnv();

        ClusterCacheClient.Builder builder = ClusterCacheClient.builder().withHealthCheck(5_000);
        for (int i = 0; i < settings.nodes().size(); i++) {
            JCacheSettings.Node node = settings.nodes().get(i);
            builder.addServer("node-" + (i + 1), node.host(), node.port());
        }

        try (ClusterCacheClient cluster = builder.build()) {
            for (int i = 1; i <= 6; i++) {
                String key = "user:" + i;
                cluster.put(key, "User " + i);
                System.out.println(key + " -> " + cluster.getRoutingTarget(key).getId());
            }
            System.out.println("user:3 = " + cluster.get("user:3"));
            System.out.println("ping   = " + cluster.ping());
        }
    }
}
