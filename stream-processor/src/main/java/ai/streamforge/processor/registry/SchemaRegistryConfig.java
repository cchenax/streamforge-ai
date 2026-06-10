package ai.streamforge.processor.registry;

import java.io.Serializable;

/**
 * Connection parameters for a Confluent-compatible Schema Registry.
 *
 * <p>Supports both <b>Confluent Schema Registry</b> (self-hosted or Confluent Cloud)
 * and <b>Apicurio Registry v2</b> in Confluent-compat mode
 * ({@code /apis/ccompat/v6} path prefix).
 *
 * <p>Configure via builder or environment variables:
 * <pre>
 *   SCHEMA_REGISTRY_URL      http://localhost:8081
 *   SCHEMA_REGISTRY_BACKEND  confluent | apicurio        (default: confluent)
 *   SCHEMA_REGISTRY_USERNAME HTTP Basic Auth username     (optional)
 *   SCHEMA_REGISTRY_PASSWORD HTTP Basic Auth password     (optional)
 *   SCHEMA_REGISTRY_API_KEY  Confluent Cloud API key      (optional)
 *   SCHEMA_REGISTRY_SECRET   Confluent Cloud API secret   (optional)
 *   SCHEMA_REGISTRY_CACHE_SIZE   int                      (default: 512)
 *   SCHEMA_REGISTRY_TIMEOUT_MS   int                      (default: 10000)
 * </pre>
 *
 * <p>Usage:
 * <pre>
 *   SchemaRegistryConfig cfg = SchemaRegistryConfig.fromEnv();
 *   // or
 *   SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
 *       .url("http://registry:8081")
 *       .username("user").password("pass")
 *       .build();
 * </pre>
 */
public final class SchemaRegistryConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Registry backend type. */
    public enum Backend {
        /** Confluent Schema Registry — REST API at the root URL. */
        CONFLUENT,
        /** Apicurio Registry v2 — Confluent compat API at {@code /apis/ccompat/v6}. */
        APICURIO
    }

    private final String url;
    private final Backend backend;
    private final String username;
    private final String password;
    private final int cacheSize;
    private final int timeoutMs;

    private SchemaRegistryConfig(Builder b) {
        this.url       = b.url;
        this.backend   = b.backend;
        this.username  = b.username;
        this.password  = b.password;
        this.cacheSize = b.cacheSize;
        this.timeoutMs = b.timeoutMs;
    }

    // ── Accessors ────────────────────────────────────────────────────────────

    public String getUrl()       { return url; }
    public Backend getBackend()  { return backend; }
    public String getUsername()  { return username; }
    public String getPassword()  { return password; }
    public int getCacheSize()    { return cacheSize; }
    public int getTimeoutMs()    { return timeoutMs; }

    /**
     * URL prefix appended before each API path.
     * Confluent SR → {@code ""}; Apicurio compat mode → {@code "/apis/ccompat/v6"}.
     */
    public String getApiPrefix() {
        return backend == Backend.APICURIO ? "/apis/ccompat/v6" : "";
    }

    /** Return {@code true} if Basic Auth credentials are configured. */
    public boolean hasAuth() {
        return username != null && !username.isEmpty()
            && password != null && !password.isEmpty();
    }

    // ── Factory methods ──────────────────────────────────────────────────────

    /** Build from environment variables (see class javadoc). */
    public static SchemaRegistryConfig fromEnv() {
        String backendStr = env("SCHEMA_REGISTRY_BACKEND", "confluent").toLowerCase();
        Backend backend = backendStr.contains("apicurio") ? Backend.APICURIO : Backend.CONFLUENT;

        // Confluent Cloud uses API key/secret; plain SR uses username/password
        String user = env("SCHEMA_REGISTRY_API_KEY",
                          env("SCHEMA_REGISTRY_USERNAME", null));
        String pass = env("SCHEMA_REGISTRY_SECRET",
                          env("SCHEMA_REGISTRY_PASSWORD", null));

        return builder()
                .url(env("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
                .backend(backend)
                .username(user)
                .password(pass)
                .cacheSize(Integer.parseInt(env("SCHEMA_REGISTRY_CACHE_SIZE", "512")))
                .timeoutMs(Integer.parseInt(env("SCHEMA_REGISTRY_TIMEOUT_MS", "10000")))
                .build();
    }

    public static Builder builder() { return new Builder(); }

    // ── Builder ──────────────────────────────────────────────────────────────

    public static final class Builder {
        private String  url       = "http://localhost:8081";
        private Backend backend   = Backend.CONFLUENT;
        private String  username  = null;
        private String  password  = null;
        private int     cacheSize = 512;
        private int     timeoutMs = 10_000;

        public Builder url(String url)              { this.url = url.stripTrailing().replaceAll("/$",""); return this; }
        public Builder backend(Backend b)           { this.backend = b;    return this; }
        public Builder username(String u)           { this.username = u;   return this; }
        public Builder password(String p)           { this.password = p;   return this; }
        public Builder cacheSize(int n)             { this.cacheSize = n;  return this; }
        public Builder timeoutMs(int ms)            { this.timeoutMs = ms; return this; }

        public SchemaRegistryConfig build() { return new SchemaRegistryConfig(this); }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    @Override
    public String toString() {
        return "SchemaRegistryConfig{url='" + url + "', backend=" + backend
                + ", auth=" + hasAuth() + ", cacheSize=" + cacheSize + "}";
    }
}
