package ai.streamforge.processor.deserialization;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.SchemaVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Stateless utility that normalizes a raw Debezium JSON payload into a
 * {@link CdcEvent}, handling field aliases introduced by schema renames and
 * detecting which schema version the event conforms to.
 *
 * <h3>Field aliases</h3>
 * <table border="1">
 *   <tr><th>Canonical field</th><th>Accepted aliases</th></tr>
 *   <tr><td>user_id</td><td>uid</td></tr>
 *   <tr><td>event_type</td><td>type</td></tr>
 *   <tr><td>created_at</td><td>ts</td></tr>
 * </table>
 *
 * <h3>Schema versions</h3>
 * <ul>
 *   <li>{@link SchemaVersion#V1} — original columns: user_id, event_type, created_at</li>
 *   <li>{@link SchemaVersion#V2} — adds session_id and ip_address (both nullable)</li>
 *   <li>{@link SchemaVersion#UNKNOWN} — after payload absent or key fields missing</li>
 * </ul>
 */
public final class SchemaEvolutionHandler {

    private SchemaEvolutionHandler() {}

    /**
     * Parses {@code bytes} into a {@link CdcEvent} with schema normalization applied.
     *
     * @throws IOException if the bytes are not valid JSON
     */
    public static CdcEvent handle(byte[] bytes, ObjectMapper mapper) throws IOException {
        JsonNode root = mapper.readTree(bytes);

        CdcEvent event = new CdcEvent();
        event.op   = textOrNull(root, "op");
        event.tsMs = root.path("ts_ms").asLong(0L);

        JsonNode afterNode = root.path("after");
        if (!afterNode.isMissingNode() && !afterNode.isNull()) {
            event.after         = normalizeRow(afterNode);
            event.schemaVersion = detectVersion(afterNode);
        } else {
            event.schemaVersion = SchemaVersion.UNKNOWN;
        }

        return event;
    }

    // ── Version detection ────────────────────────────────────────────────────

    static SchemaVersion detectVersion(JsonNode afterNode) {
        if (afterNode.has("session_id") || afterNode.has("ip_address")) {
            return SchemaVersion.V2;
        }
        if (afterNode.has("user_id") || afterNode.has("uid")) {
            return SchemaVersion.V1;
        }
        return SchemaVersion.UNKNOWN;
    }

    // ── Field normalization ──────────────────────────────────────────────────

    static CdcEvent.UserEventRow normalizeRow(JsonNode afterNode) {
        CdcEvent.UserEventRow row = new CdcEvent.UserEventRow();
        // user_id — also accepts legacy alias "uid" (column renamed in some deployments)
        row.userId    = coalesceText(afterNode, "user_id", "uid");
        // event_type — also accepts abbreviated alias "type"
        row.eventType = coalesceText(afterNode, "event_type", "type");
        // created_at — also accepts shortened alias "ts"
        row.createdAt = coalesceTimestamp(afterNode, "created_at", "ts");
        // V2 fields (nullable; null when absent in V1 payloads)
        row.sessionId = textOrNull(afterNode, "session_id");
        row.ipAddress = textOrNull(afterNode, "ip_address");
        return row;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** Returns the text value of the first non-null key found among {@code keys}. */
    static String coalesceText(JsonNode node, String... keys) {
        for (String key : keys) {
            String val = textOrNull(node, key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }

    /** Returns the long value of the first non-null key found among {@code keys}. */
    static Long coalesceTimestamp(JsonNode node, String... keys) {
        for (String key : keys) {
            JsonNode child = node.get(key);
            if (child != null && !child.isNull()) {
                return child.asLong();
            }
        }
        return null;
    }

    static String textOrNull(JsonNode node, String field) {
        JsonNode child = node.get(field);
        return (child != null && !child.isNull()) ? child.asText() : null;
    }
}
