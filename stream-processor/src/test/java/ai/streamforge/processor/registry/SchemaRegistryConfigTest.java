package ai.streamforge.processor.registry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaRegistryConfigTest {

    @Test
    void defaults_are_sensible() {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder().build();
        assertEquals("http://localhost:8081", cfg.getUrl());
        assertEquals(SchemaRegistryConfig.Backend.CONFLUENT, cfg.getBackend());
        assertEquals(512, cfg.getCacheSize());
        assertEquals(10_000, cfg.getTimeoutMs());
        assertFalse(cfg.hasAuth());
    }

    @Test
    void confluent_backend_has_empty_api_prefix() {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
                .backend(SchemaRegistryConfig.Backend.CONFLUENT)
                .build();
        assertEquals("", cfg.getApiPrefix());
    }

    @Test
    void apicurio_backend_has_compat_prefix() {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
                .backend(SchemaRegistryConfig.Backend.APICURIO)
                .build();
        assertEquals("/apis/ccompat/v6", cfg.getApiPrefix());
    }

    @Test
    void builder_sets_all_fields() {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
                .url("http://registry.prod:8081")
                .backend(SchemaRegistryConfig.Backend.APICURIO)
                .username("alice")
                .password("secret")
                .cacheSize(256)
                .timeoutMs(5_000)
                .build();

        assertEquals("http://registry.prod:8081", cfg.getUrl());
        assertEquals(SchemaRegistryConfig.Backend.APICURIO, cfg.getBackend());
        assertEquals("alice", cfg.getUsername());
        assertEquals("secret", cfg.getPassword());
        assertEquals(256, cfg.getCacheSize());
        assertEquals(5_000, cfg.getTimeoutMs());
        assertTrue(cfg.hasAuth());
    }

    @Test
    void trailing_slash_stripped_from_url() {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
                .url("http://host:8081/")
                .build();
        assertFalse(cfg.getUrl().endsWith("/"));
    }

    @Test
    void hasAuth_false_when_credentials_blank() {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
                .username("").password("").build();
        assertFalse(cfg.hasAuth());
    }

    @Test
    void toString_contains_url_and_backend() {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
                .url("http://sr:8081").build();
        String s = cfg.toString();
        assertTrue(s.contains("sr:8081"));
        assertTrue(s.contains("CONFLUENT") || s.contains("confluent"));
    }

    @Test
    void fromEnv_uses_defaults_when_vars_absent() {
        // Env vars are not set in the test environment — check defaults apply
        // (without patching env; real SCHEMA_REGISTRY_URL likely absent in CI)
        SchemaRegistryConfig cfg = SchemaRegistryConfig.fromEnv();
        assertNotNull(cfg.getUrl());
        assertNotNull(cfg.getBackend());
        assertTrue(cfg.getCacheSize() > 0);
    }
}
