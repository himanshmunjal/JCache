package com.example.jcache;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JCacheSettingsTest {

    @Test
    void defaultsMatchTheDockerImage() {
        JCacheSettings s = JCacheSettings.from(Map.of());
        assertEquals("localhost", s.host());
        assertEquals(6379, s.port());
        assertEquals(3, s.nodes().size());
        assertEquals(new JCacheSettings.Node("localhost", 6381), s.nodes().get(2));
    }

    @Test
    void readsHostAndPort() {
        JCacheSettings s = JCacheSettings.from(Map.of("JCACHE_HOST", "cache.internal", "JCACHE_PORT", " 7000 "));
        assertEquals("cache.internal", s.host());
        assertEquals(7000, s.port());
    }

    @Test
    void blankValuesFallBackToDefaults() {
        JCacheSettings s = JCacheSettings.from(Map.of("JCACHE_HOST", " ", "JCACHE_PORT", ""));
        assertEquals("localhost", s.host());
        assertEquals(6379, s.port());
    }

    @Test
    void parsesNodeList() {
        JCacheSettings s = JCacheSettings.from(Map.of("JCACHE_NODES", "a:1, b:2,,"));
        assertEquals(List.of(new JCacheSettings.Node("a", 1), new JCacheSettings.Node("b", 2)), s.nodes());
    }

    @Test
    void rejectsBadPorts() {
        assertThrows(IllegalArgumentException.class, () -> JCacheSettings.from(Map.of("JCACHE_PORT", "abc")));
        assertThrows(IllegalArgumentException.class, () -> JCacheSettings.from(Map.of("JCACHE_PORT", "0")));
        assertThrows(IllegalArgumentException.class, () -> JCacheSettings.from(Map.of("JCACHE_NODES", "a:70000")));
        assertThrows(IllegalArgumentException.class, () -> JCacheSettings.from(Map.of("JCACHE_NODES", "nohost")));
    }
}
