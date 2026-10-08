package com.example.jcache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Where to find the JCache server, read from environment variables so the
 * examples match however the container was started.
 *
 * <ul>
 *   <li>{@code JCACHE_HOST}: server host, default {@code localhost}</li>
 *   <li>{@code JCACHE_PORT}: server port, default {@code 6379}</li>
 *   <li>{@code JCACHE_NODES}: comma-separated {@code host:port} list for the
 *       cluster example, default the three nodes of the compose file</li>
 * </ul>
 *
 * @param host  server host
 * @param port  server port
 * @param nodes cluster nodes as {@code host:port}
 */
public record JCacheSettings(String host, int port, List<Node> nodes) {

    /** One cluster node. */
    public record Node(String host, int port) {
        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    static final String DEFAULT_NODES = "localhost:6379,localhost:6380,localhost:6381";

    /** @return settings from the process environment */
    public static JCacheSettings fromEnv() {
        return from(System.getenv());
    }

    /**
     * Reads settings from a map of variables.
     *
     * @param env variable name to value
     * @return the settings
     * @throws IllegalArgumentException if a port is not a number in [1, 65535]
     */
    public static JCacheSettings from(Map<String, String> env) {
        String host = value(env, "JCACHE_HOST", "localhost");
        int port = parsePort(value(env, "JCACHE_PORT", "6379"));
        List<Node> nodes = new ArrayList<>();
        for (String entry : value(env, "JCACHE_NODES", DEFAULT_NODES).split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colon = trimmed.lastIndexOf(':');
            if (colon <= 0) {
                throw new IllegalArgumentException("Expected host:port in JCACHE_NODES, got: " + trimmed);
            }
            nodes.add(new Node(trimmed.substring(0, colon), parsePort(trimmed.substring(colon + 1))));
        }
        return new JCacheSettings(host, port, List.copyOf(nodes));
    }

    private static String value(Map<String, String> env, String name, String fallback) {
        String v = env.get(name);
        return v == null || v.isBlank() ? fallback : v.trim();
    }

    private static int parsePort(String text) {
        int port;
        try {
            port = Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Port is not a number: " + text);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535, got: " + port);
        }
        return port;
    }
}
