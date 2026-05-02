package ai.streamforge.processor;

import ai.streamforge.processor.deserialization.SchemaEvolutionHandler;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.SchemaVersion;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaEvolutionHandlerTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
    }

    // ── Schema version detection ─────────────────────────────────────────────

    @Test
    void detectsV1SchemaFromUserIdField() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"user_id":"u1","event_type":"click","created_at":1700000000000}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals(SchemaVersion.V1, event.schemaVersion);
    }

    @Test
    void detectsV2SchemaWhenSessionIdPresent() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"user_id":"u1","event_type":"click","created_at":1700000000000,
                           "session_id":"sess-abc","ip_address":"192.168.1.1"}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals(SchemaVersion.V2, event.schemaVersion);
        assertEquals("sess-abc", event.after.sessionId);
        assertEquals("192.168.1.1", event.after.ipAddress);
    }

    @Test
    void detectsUnknownSchemaWhenAfterIsNull() throws Exception {
        String json = """
                {"op":"d","ts_ms":1700000000000,"after":null}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals(SchemaVersion.UNKNOWN, event.schemaVersion);
        assertNull(event.after);
    }

    // ── Field alias normalization ────────────────────────────────────────────

    @Test
    void normalizesUidAliasToUserId() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"uid":"user-renamed","event_type":"view","created_at":1700000000000}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals("user-renamed", event.after.userId);
    }

    @Test
    void normalizesTypeAliasToEventType() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"user_id":"u1","type":"purchase","created_at":1700000000000}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals("purchase", event.after.eventType);
    }

    @Test
    void normalizesTsAliasToCreatedAt() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"user_id":"u1","event_type":"click","ts":1699999999000}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals(1699999999000L, event.after.createdAt);
    }

    @Test
    void canonicalFieldTakesPriorityOverAlias() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"user_id":"canonical","uid":"alias","event_type":"click","created_at":1700000000000}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals("canonical", event.after.userId);
    }

    // ── Missing / nullable fields ────────────────────────────────────────────

    @Test
    void toleratesMissingOptionalFields() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"user_id":"u1"}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals("u1", event.after.userId);
        assertNull(event.after.eventType);
        assertNull(event.after.createdAt);
        assertNull(event.after.sessionId);
        assertNull(event.after.ipAddress);
    }

    @Test
    void toleratesNullFieldValues() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "after":{"user_id":"u1","event_type":null,"created_at":null}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals("u1", event.after.userId);
        assertNull(event.after.eventType);
        assertNull(event.after.createdAt);
    }

    // ── Envelope fields ──────────────────────────────────────────────────────

    @Test
    void parsesOpAndTsMs() throws Exception {
        String json = """
                {"op":"u","ts_ms":1700001234567,
                 "after":{"user_id":"u2","event_type":"update","created_at":1700001234000}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals("u", event.op);
        assertEquals(1700001234567L, event.tsMs);
    }

    @Test
    void ignoresUnknownEnvelopeFields() throws Exception {
        String json = """
                {"op":"c","ts_ms":1700000000000,
                 "source":{"connector":"mysql","table":"user_events"},
                 "transaction":null,
                 "after":{"user_id":"u1","event_type":"click","created_at":1700000000000}}
                """;
        CdcEvent event = SchemaEvolutionHandler.handle(json.getBytes(), mapper);
        assertEquals("c", event.op);
        assertEquals("u1", event.after.userId);
    }
}
