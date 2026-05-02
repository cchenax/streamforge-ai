package ai.streamforge.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Debezium CDC envelope for a MySQL row change event.
 *
 * Relevant Debezium ops: c=create, u=update, d=delete, r=snapshot-read.
 * Only the {@code after} field is populated for inserts; both {@code before}
 * and {@code after} are present for updates.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcEvent {

    /** Debezium operation type: c, u, d, r. */
    public String op;

    /** Debezium processing timestamp (milliseconds since epoch). */
    @JsonProperty("ts_ms")
    public long tsMs;

    /** Row state after the change — null for deletes. */
    public UserEventRow after;

    /**
     * Schema version detected by {@link ai.streamforge.processor.deserialization.SchemaEvolutionHandler}.
     * Transient — not serialized to Kafka or persisted in Flink state.
     */
    public transient SchemaVersion schemaVersion = SchemaVersion.UNKNOWN;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UserEventRow {

        @JsonProperty("user_id")
        public String userId;

        @JsonProperty("event_type")
        public String eventType;

        /** Application-level event timestamp (milliseconds since epoch). */
        @JsonProperty("created_at")
        public Long createdAt;

        /** V2+: browser/app session identifier (nullable). */
        @JsonProperty("session_id")
        public String sessionId;

        /** V2+: client IP address (nullable). */
        @JsonProperty("ip_address")
        public String ipAddress;
    }
}
