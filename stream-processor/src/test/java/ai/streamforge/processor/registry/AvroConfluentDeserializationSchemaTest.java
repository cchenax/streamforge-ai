package ai.streamforge.processor.registry;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.SchemaVersion;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AvroConfluentDeserializationSchema}.
 *
 * A stub {@link SchemaRegistryHttpClient} is injected via package-private
 * reflection to avoid any real HTTP calls.
 */
class AvroConfluentDeserializationSchemaTest {

    private static final int  TEST_SCHEMA_ID = 42;
    private static final byte MAGIC          = 0x00;

    /** Minimal CdcEvent / UserEventRow Avro schema (V2). */
    private static final String V2_SCHEMA_JSON =
        "{\"namespace\":\"ai.streamforge.processor\"," +
        "\"type\":\"record\"," +
        "\"name\":\"CdcEvent\"," +
        "\"fields\":[" +
          "{\"name\":\"op\",\"type\":\"string\"}," +
          "{\"name\":\"ts_ms\",\"type\":\"long\"}," +
          "{\"name\":\"after\",\"type\":[\"null\",{" +
            "\"type\":\"record\",\"name\":\"UserEventRow\"," +
            "\"fields\":[" +
              "{\"name\":\"user_id\",\"type\":\"string\"}," +
              "{\"name\":\"event_type\",\"type\":\"string\"}," +
              "{\"name\":\"created_at\",\"type\":[\"null\",\"long\"],\"default\":null}," +
              "{\"name\":\"session_id\",\"type\":[\"null\",\"string\"],\"default\":null}," +
              "{\"name\":\"ip_address\",\"type\":[\"null\",\"string\"],\"default\":null}" +
            "]}],\"default\":null}" +
        "]}";

    private Schema avroSchema;

    @BeforeEach
    void setUp() {
        avroSchema = new Schema.Parser().parse(V2_SCHEMA_JSON);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** Encode a GenericRecord as Confluent-framed Avro bytes. */
    private byte[] encodeConfluent(GenericRecord record, int schemaId) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(MAGIC);
        buf.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(buf, null);
        new GenericDatumWriter<GenericRecord>(avroSchema).write(record, enc);
        enc.flush();
        return buf.toByteArray();
    }

    /** Build a AvroConfluentDeserializationSchema with a stub cache pre-loaded with our test schema. */
    private AvroConfluentDeserializationSchema buildSchema(boolean fallbackToJson) throws Exception {
        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder()
                .url("http://stub:8081").build();
        AvroConfluentDeserializationSchema ds =
                new AvroConfluentDeserializationSchema(cfg, null, fallbackToJson);

        // Use open() to initialise transient fields, then inject pre-loaded cache
        ds.open(null);

        // Inject schema directly into the cache to avoid HTTP
        injectSchemaIntoCache(ds, TEST_SCHEMA_ID, avroSchema);
        return ds;
    }

    /** Reflectively inject a schema into the deserialization schema's cache. */
    private void injectSchemaIntoCache(
            AvroConfluentDeserializationSchema ds, int id, Schema schema) throws Exception {
        var cacheField = AvroConfluentDeserializationSchema.class
                .getDeclaredField("schemaCache");
        cacheField.setAccessible(true);
        SchemaCache cache = (SchemaCache) cacheField.get(ds);
        cache.put(id, schema);
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    @Test
    void null_message_returns_null() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);
        assertNull(ds.deserialize(null));
    }

    @Test
    void empty_message_returns_null() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);
        assertNull(ds.deserialize(new byte[0]));
    }

    @Test
    void too_short_message_returns_null() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);
        // 4 bytes with magic byte but no full header
        assertNull(ds.deserialize(new byte[]{MAGIC, 0, 0, 0}));
    }

    @Test
    void decodes_avro_insert_event() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);

        // Build a V2 insert event
        Schema rowSchema = avroSchema.getField("after").schema().getTypes().get(1);
        GenericRecord row = new GenericData.Record(rowSchema);
        row.put("user_id",    "user-99");
        row.put("event_type", "click");
        row.put("created_at", 1_700_000_000_000L);
        row.put("session_id", "sess-abc");
        row.put("ip_address", null);

        GenericRecord root = new GenericData.Record(avroSchema);
        root.put("op",    "c");
        root.put("ts_ms", 1_700_000_001_000L);
        root.put("after", row);

        byte[] encoded = encodeConfluent(root, TEST_SCHEMA_ID);
        CdcEvent event = ds.deserialize(encoded);

        assertNotNull(event);
        assertEquals("c",            event.op);
        assertEquals(1_700_000_001_000L, event.tsMs);
        assertNotNull(event.after);
        assertEquals("user-99",  event.after.userId);
        assertEquals("click",    event.after.eventType);
        assertEquals(1_700_000_000_000L, (long) event.after.createdAt);
        assertEquals("sess-abc", event.after.sessionId);
        assertNull(event.after.ipAddress);
        assertEquals(SchemaVersion.V2, event.schemaVersion);
    }

    @Test
    void decodes_delete_event_with_null_after() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);

        GenericRecord root = new GenericData.Record(avroSchema);
        root.put("op",    "d");
        root.put("ts_ms", 999L);
        root.put("after", null);

        byte[] encoded = encodeConfluent(root, TEST_SCHEMA_ID);
        CdcEvent event = ds.deserialize(encoded);

        assertNotNull(event);
        assertEquals("d", event.op);
        assertNull(event.after);
        assertEquals(SchemaVersion.UNKNOWN, event.schemaVersion);
    }

    @Test
    void infers_v1_schema_version() throws Exception {
        // Use a V1 schema (no session_id / ip_address)
        String v1Json =
            "{\"namespace\":\"ai.streamforge.processor\"," +
            "\"type\":\"record\",\"name\":\"CdcEvent\"," +
            "\"fields\":[" +
              "{\"name\":\"op\",\"type\":\"string\"}," +
              "{\"name\":\"ts_ms\",\"type\":\"long\"}," +
              "{\"name\":\"after\",\"type\":[\"null\",{" +
                "\"type\":\"record\",\"name\":\"UserEventRow\"," +
                "\"fields\":[" +
                  "{\"name\":\"user_id\",\"type\":\"string\"}," +
                  "{\"name\":\"event_type\",\"type\":\"string\"}" +
                "]}],\"default\":null}" +
            "]}";
        Schema v1Schema = new Schema.Parser().parse(v1Json);

        SchemaRegistryConfig cfg = SchemaRegistryConfig.builder().url("http://stub:8081").build();
        AvroConfluentDeserializationSchema ds = new AvroConfluentDeserializationSchema(cfg, null, false);
        ds.open(null);
        injectSchemaIntoCache(ds, 1, v1Schema);

        Schema rowSchema = v1Schema.getField("after").schema().getTypes().get(1);
        GenericRecord row = new GenericData.Record(rowSchema);
        row.put("user_id", "u1");
        row.put("event_type", "view");

        GenericRecord root = new GenericData.Record(v1Schema);
        root.put("op", "c");
        root.put("ts_ms", 0L);
        root.put("after", row);

        byte[] encoded;
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            buf.write(MAGIC);
            buf.write(ByteBuffer.allocate(4).putInt(1).array());
            BinaryEncoder enc = EncoderFactory.get().binaryEncoder(buf, null);
            new GenericDatumWriter<GenericRecord>(v1Schema).write(root, enc);
            enc.flush();
            encoded = buf.toByteArray();
        }

        CdcEvent event = ds.deserialize(encoded);
        assertEquals(SchemaVersion.V1, event.schemaVersion);
    }

    @Test
    void non_confluent_byte_falls_back_to_json_when_enabled() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(true);

        // Build a plain JSON CDC event
        ObjectMapper om = new ObjectMapper();
        byte[] json = om.writeValueAsBytes(om.createObjectNode()
                .put("op", "c")
                .put("ts_ms", 12345L)
                .putNull("after"));

        // First byte is '{' (0x7B), not 0x00
        CdcEvent event = ds.deserialize(json);
        assertNotNull(event);
        assertEquals("c", event.op);
    }

    @Test
    void non_confluent_byte_returns_null_when_fallback_disabled() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);
        byte[] nonAvro = new byte[]{0x7B, 0x22, 0x6F, 0x70, 0x22, 0x3A, 0x22, 0x63, 0x22, 0x7D};
        // Non-Avro data with fallback disabled → null (logged as warning)
        assertNull(ds.deserialize(nonAvro));
    }

    @Test
    void is_end_of_stream_always_false() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);
        assertFalse(ds.isEndOfStream(new CdcEvent()));
    }

    @Test
    void produced_type_is_cdc_event() throws Exception {
        AvroConfluentDeserializationSchema ds = buildSchema(false);
        assertEquals(CdcEvent.class, ds.getProducedType().getTypeClass());
    }
}
