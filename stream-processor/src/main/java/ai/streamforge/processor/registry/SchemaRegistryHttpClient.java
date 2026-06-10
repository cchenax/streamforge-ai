package ai.streamforge.processor.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/**
 * Lightweight HTTP client for the Confluent Schema Registry REST API.
 *
 * <p>Supports both <b>Confluent Schema Registry</b> and <b>Apicurio v2</b> in
 * Confluent-compat mode — the {@link SchemaRegistryConfig#getApiPrefix()} value
 * is automatically prepended to every path.
 *
 * <p>Uses the Java 11 {@code java.net.http.HttpClient} — no extra dependencies.
 * Avro schemas are returned as parsed {@link Schema} objects and cached via a
 * caller-supplied {@link SchemaCache}.
 *
 * <h3>Retry policy</h3>
 * 5xx responses are retried up to {@value #MAX_RETRIES} times with a fixed
 * 250 ms delay.  4xx errors are raised immediately as {@link SchemaRegistryException}.
 *
 * <p>This class is {@code Serializable} so Flink can ship it to task managers.
 * The underlying {@code HttpClient} is {@code transient} and re-created lazily on
 * each task manager after deserialization.
 */
public final class SchemaRegistryHttpClient implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryHttpClient.class);

    private static final int    MAX_RETRIES  = 3;
    private static final long   RETRY_DELAY_MS = 250L;
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private final SchemaRegistryConfig config;
    private final String baseUrl;           // config.url + config.apiPrefix

    private transient HttpClient  httpClient;
    private transient ObjectMapper objectMapper;

    // ── Exceptions ───────────────────────────────────────────────────────────

    /** Wraps non-2xx responses from the schema registry. */
    public static class SchemaRegistryException extends IOException {
        private final int statusCode;
        public SchemaRegistryException(String message, int statusCode) {
            super(message);
            this.statusCode = statusCode;
        }
        public int getStatusCode() { return statusCode; }
    }

    /** Thrown when a subject or schema ID is not found (HTTP 404). */
    public static class SchemaNotFoundException extends SchemaRegistryException {
        public SchemaNotFoundException(String message) { super(message, 404); }
    }

    // ── Construction ─────────────────────────────────────────────────────────

    public SchemaRegistryHttpClient(SchemaRegistryConfig config) {
        this.config  = config;
        this.baseUrl = config.getUrl() + config.getApiPrefix();
    }

    private HttpClient httpClient() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(config.getTimeoutMs()))
                    .build();
        }
        return httpClient;
    }

    private ObjectMapper objectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /**
     * Fetch an Avro {@link Schema} by its globally-unique Confluent schema ID.
     *
     * @param schemaId  The integer schema ID embedded in the Confluent wire format.
     * @return          Parsed {@link Schema}.
     * @throws IOException on HTTP or parse errors.
     */
    public Schema getSchemaById(int schemaId) throws IOException {
        String body = get("/schemas/ids/" + schemaId);
        JsonNode root = objectMapper().readTree(body);
        String schemaStr = root.path("schema").asText();
        return new Schema.Parser().parse(schemaStr);
    }

    /**
     * Fetch the latest registered schema for a subject.
     *
     * @param subject  Subject name, e.g. {@code "user-events-value"}.
     * @return         Parsed {@link Schema}.
     * @throws IOException on HTTP or parse errors.
     */
    public Schema getLatestSchema(String subject) throws IOException {
        String body = get("/subjects/" + subject + "/versions/latest");
        JsonNode root = objectMapper().readTree(body);
        String schemaStr = root.path("schema").asText();
        return new Schema.Parser().parse(schemaStr);
    }

    /**
     * Register a new schema version for a subject.
     *
     * <p>Idempotent — returns the existing ID if the schema is already registered.
     *
     * @param subject   Subject name.
     * @param avroSchema Avro {@link Schema} to register.
     * @return           Globally-unique schema ID.
     * @throws IOException on HTTP or parse errors.
     */
    public int registerSchema(String subject, Schema avroSchema) throws IOException {
        ObjectNode payload = objectMapper().createObjectNode();
        payload.put("schema", avroSchema.toString());
        payload.put("schemaType", "AVRO");
        String body = post("/subjects/" + subject + "/versions", payload.toString());
        JsonNode root = objectMapper().readTree(body);
        return root.path("id").asInt();
    }

    /**
     * Test whether {@code avroSchema} is compatible with the latest version of
     * {@code subject} according to the registered compatibility level.
     *
     * @param subject    Subject name.
     * @param avroSchema Schema to test.
     * @return           {@code true} if compatible.
     * @throws IOException on HTTP errors.
     */
    public boolean checkCompatibility(String subject, Schema avroSchema) throws IOException {
        ObjectNode payload = objectMapper().createObjectNode();
        payload.put("schema", avroSchema.toString());
        payload.put("schemaType", "AVRO");
        try {
            String body = post(
                "/compatibility/subjects/" + subject + "/versions/latest",
                payload.toString()
            );
            JsonNode root = objectMapper().readTree(body);
            return root.path("is_compatible").asBoolean(false);
        } catch (SchemaNotFoundException e) {
            // No versions yet → trivially compatible
            return true;
        }
    }

    // ── HTTP helpers ─────────────────────────────────────────────────────────

    private String get(String path) throws IOException {
        return request("GET", path, null);
    }

    private String post(String path, String jsonBody) throws IOException {
        return request("POST", path, jsonBody);
    }

    private String request(String method, String path, String body) throws IOException {
        URI uri = URI.create(baseUrl + path);
        IOException lastException = null;

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            HttpRequest.Builder req = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofMillis(config.getTimeoutMs()))
                    .header("Content-Type", CONTENT_TYPE)
                    .header("Accept", CONTENT_TYPE);

            if (config.hasAuth()) {
                String creds = config.getUsername() + ":" + config.getPassword();
                req.header("Authorization", "Basic " +
                        Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8)));
            }

            if ("POST".equals(method) && body != null) {
                req.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
            } else {
                req.GET();
            }

            try {
                HttpResponse<String> resp = httpClient().send(
                        req.build(), HttpResponse.BodyHandlers.ofString());
                int status = resp.statusCode();

                if (status >= 200 && status < 300) {
                    return resp.body();
                }
                if (status == 404) {
                    throw new SchemaNotFoundException("Not found: " + path);
                }
                if (status >= 500) {
                    lastException = new SchemaRegistryException(
                            "HTTP " + status + " from " + path + ": " + resp.body(), status);
                    LOG.warn("[SchemaRegistry] 5xx on attempt {}/{}: {}", attempt + 1, MAX_RETRIES, lastException.getMessage());
                    Thread.sleep(RETRY_DELAY_MS);
                    continue;
                }
                // 4xx (non-404) — fail immediately
                throw new SchemaRegistryException(
                        "HTTP " + status + " from " + path + ": " + resp.body(), status);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while calling schema registry", e);
            } catch (SchemaRegistryException e) {
                throw e;
            } catch (Exception e) {
                lastException = new IOException("Request to " + uri + " failed: " + e.getMessage(), e);
                LOG.warn("[SchemaRegistry] Error on attempt {}/{}: {}", attempt + 1, MAX_RETRIES, e.getMessage());
                try { Thread.sleep(RETRY_DELAY_MS); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", ie);
                }
            }
        }
        throw (lastException != null ? lastException :
                new IOException("Max retries exceeded for " + path));
    }

    @Override
    public String toString() {
        return "SchemaRegistryHttpClient{baseUrl='" + baseUrl + "'}";
    }
}
