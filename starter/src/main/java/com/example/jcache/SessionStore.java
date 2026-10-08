package com.example.jcache;

import com.cache.client.CacheClient;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Login sessions that expire after a period of inactivity. The server drops
 * expired keys itself, so there is no cleanup job.
 *
 * <pre>mvn compile exec:java -Dexec.mainClass=com.example.jcache.SessionStore</pre>
 */
public class SessionStore {

    private static final String PREFIX = "session:";

    private final CacheClient client;
    private final long idleSeconds;

    /**
     * @param client      connection to use
     * @param idleSeconds how long a session lives without being used
     */
    public SessionStore(CacheClient client, long idleSeconds) {
        this.client = client;
        this.idleSeconds = idleSeconds;
    }

    /** @return a new session id for the user */
    public String create(String userId) throws IOException {
        String sessionId = UUID.randomUUID().toString();
        client.put(PREFIX + sessionId, userId, idleSeconds);
        return sessionId;
    }

    /** Looks up a session and, if it is still alive, extends it. */
    public Optional<String> touch(String sessionId) throws IOException {
        String userId = client.get(PREFIX + sessionId);
        if (userId != null) {
            client.expire(PREFIX + sessionId, idleSeconds);
        }
        return Optional.ofNullable(userId);
    }

    /** Ends a session. */
    public void logout(String sessionId) throws IOException {
        client.delete(PREFIX + sessionId);
    }

    public static void main(String[] args) throws IOException {
        JCacheSettings settings = JCacheSettings.fromEnv();
        try (CacheClient client = new CacheClient(settings.host(), settings.port())) {
            SessionStore sessions = new SessionStore(client, 1800);

            String id = sessions.create("user-42");
            System.out.println("Created session " + id);
            System.out.println("touch  -> " + sessions.touch(id).orElse("expired"));
            System.out.println("ttl    -> " + client.ttl(PREFIX + id) + "s");

            sessions.logout(id);
            System.out.println("after logout -> " + sessions.touch(id).orElse("expired"));
        }
    }
}
