package ai.streamforge.processor.model;

/**
 * Tracks the schema version of an incoming {@link CdcEvent}, detected from
 * which fields are present in the Debezium {@code after} payload.
 */
public enum SchemaVersion {
    /** Original schema: user_id, event_type, created_at. */
    V1,
    /** Extended schema: adds session_id and ip_address (both nullable). */
    V2,
    /** Fields don't match any known version; treated as V1 with nulls for missing fields. */
    UNKNOWN
}
