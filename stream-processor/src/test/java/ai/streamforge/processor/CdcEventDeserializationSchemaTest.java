package ai.streamforge.processor;

import ai.streamforge.processor.deserialization.CdcEventDeserializationSchema;
import ai.streamforge.processor.model.CdcEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CdcEventDeserializationSchemaTest {

    private CdcEventDeserializationSchema schema;

    @BeforeEach
    void setUp() throws Exception {
        schema = new CdcEventDeserializationSchema();
        schema.open(null);
    }

    @Test
    void deserializesInsertEvent() throws Exception {
        String json = """
                {
                  "op": "c",
                  "ts_ms": 1700000000000,
                  "after": {
                    "user_id": "user-42",
                    "event_type": "click",
                    "created_at": 1700000000000
                  }
                }
                """;

        CdcEvent event = schema.deserialize(json.getBytes());

        assertEquals("c", event.op);
        assertEquals(1700000000000L, event.tsMs);
        assertNotNull(event.after);
        assertEquals("user-42", event.after.userId);
        assertEquals("click", event.after.eventType);
    }

    @Test
    void ignoresUnknownDebeziumEnvelopeFields() throws Exception {
        String json = """
                {
                  "op": "u",
                  "ts_ms": 1700000001000,
                  "before": {"user_id": "user-42", "event_type": "view"},
                  "after":  {"user_id": "user-42", "event_type": "click"},
                  "source": {"connector": "mysql", "table": "user_events"},
                  "transaction": null
                }
                """;

        CdcEvent event = schema.deserialize(json.getBytes());

        assertEquals("u", event.op);
        assertNotNull(event.after);
        assertEquals("user-42", event.after.userId);
        assertEquals("click", event.after.eventType);
    }

    @Test
    void handlesDeleteEventWithNullAfter() throws Exception {
        String json = """
                {
                  "op": "d",
                  "ts_ms": 1700000002000,
                  "after": null,
                  "before": {"user_id": "user-99", "event_type": "logout"}
                }
                """;

        CdcEvent event = schema.deserialize(json.getBytes());

        assertEquals("d", event.op);
        assertNull(event.after);
    }

    @Test
    void returnsNullForNullInput() throws Exception {
        assertNull(schema.deserialize(null));
    }

    @Test
    void isEndOfStreamAlwaysReturnsFalse() {
        assertFalse(schema.isEndOfStream(new CdcEvent()));
    }
}
