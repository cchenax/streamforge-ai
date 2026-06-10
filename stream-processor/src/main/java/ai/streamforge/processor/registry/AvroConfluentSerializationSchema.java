package ai.streamforge.processor.registry;

import ai.streamforge.processor.model.UserEventCount;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Flink {@link SerializationSchema} that encodes {@link UserEventCount} records
 * as <b>Confluent-framed Avro</b> bytes for publishing to Kafka.
 *
 * <h3>Wire format (Confluent Schema Registry framing)</h3>
 * <pre>
 *   Byte 0       : magic byte = 0x00
 *   Bytes 1–4    : schema ID (big-endian uint32)
 *   Bytes 5+     : Avro binary-encoded payload
 * </pre>
 *
 * <p>The schema is registered in the registry on the first {@link #serialize} call
 * and the resulting ID is cached for the lifetime of the serializer.  Subsequent
 * calls skip the registration round-trip.
 *
 * <h3>Schema</h3>
 * The Avro schema used matches {@code schema-registry/schemas/user_event_count.avsc}:
 * <ul>
 *   <li>{@code user_id}        – string</li>
 *   <li>{@code event_count}    – long</li>
 *   <li>{@code window_start_ms} – long</li>
 *   <li>{@code window_end_ms}  – long</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>
 *   SchemaRegistryConfig config = SchemaRegistryConfig.fromEnv();
 *   SerializationSchema&lt;UserEventCount&gt; schema =
 *       new AvroConfluentSerializationSchema(config, "user-event-counts-value");
 * </pre>
 *
 * <p>Drop-in replacement for
 * {@link ai.streamforge.processor.serialization.UserEventCountSerializationSchema}.
 */
public class AvroConfluentSerializationSchema implements SerializationSchema<UserEventCount> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AvroConfluentSerializationSchema.class);

    private static final byte   MAGIC_BYTE  = 0x00;
    private static final int    HEADER_SIZE = 5;   // 1 magic + 4 schema-id

    /** Avro schema for {@link UserEventCount} (mirrors user_event_count.avsc). */
    private static final String SCHEMA_JSON =
        "{\"namespace\":\"ai.streamforge.processor\"," +
        "\"type\":\"record\"," +
        "\"name\":\"UserEventCount\"," +
        "\"doc\":\"Aggregated insert-event count per user per tumbling window.\"," +
        "\"fields\":[" +
        "{\"name\":\"user_id\",\"type\":\"string\"}," +
        "{\"name\":\"event_count\",\"type\":\"long\"}," +
        "{\"name\":\"window_start_ms\",\"type\":\"long\"}," +
        "{\"name\":\"window_end_ms\",\"type\":\"long\"}" +
        "]}";

    private final SchemaRegistryConfig registryConfig;
    private final String subject;

    private transient SchemaRegistryHttpClient httpClient;
    private transient Schema   avroSchema;
    private transient int      schemaId = -1;    // -1 = not yet registered

    // ── Constructor ──────────────────────────────────────────────────────────

    /**
     * @param registryConfig  Registry connection settings.
     * @param subject         Kafka subject name under which the schema is registered,
     *                        e.g. {@code "user-event-counts-value"}.
     */
    public AvroConfluentSerializationSchema(SchemaRegistryConfig registryConfig, String subject) {
        this.registryConfig = registryConfig;
        this.subject        = subject;
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        httpClient = new SchemaRegistryHttpClient(registryConfig);
        avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
        LOG.info("[AvroConfluentSerializationSchema] opened — subject={}", subject);
    }

    // ── SerializationSchema ──────────────────────────────────────────────────

    @Override
    public byte[] serialize(UserEventCount record) {
        try {
            ensureSchemaRegistered();
            return encodeWithHeader(record);
        } catch (Exception e) {
            LOG.error("[AvroConfluentSerializationSchema] Serialization failed: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize UserEventCount", e);
        }
    }

    // ── Schema registration ──────────────────────────────────────────────────

    private void ensureSchemaRegistered() throws IOException {
        if (schemaId >= 0) return;
        schemaId = httpClient.registerSchema(subject, avroSchema);
        LOG.info("[AvroConfluentSerializationSchema] Registered schema under {}: id={}", subject, schemaId);
    }

    // ── Encoding ─────────────────────────────────────────────────────────────

    private byte[] encodeWithHeader(UserEventCount evt) throws IOException {
        // Build Avro GenericRecord
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("user_id",         evt.userId);
        avroRecord.put("event_count",     evt.count);
        avroRecord.put("window_start_ms", evt.windowStartMs);
        avroRecord.put("window_end_ms",   evt.windowEndMs);

        // Encode to Avro binary
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64);
        // Reserve 5 bytes for the header
        buf.write(new byte[HEADER_SIZE]);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(buf, null);
        new GenericDatumWriter<GenericRecord>(avroSchema).write(avroRecord, encoder);
        encoder.flush();

        // Write Confluent header into the reserved bytes
        byte[] result = buf.toByteArray();
        result[0] = MAGIC_BYTE;
        ByteBuffer.wrap(result, 1, 4).putInt(schemaId);
        return result;
    }

    // ── Accessors ────────────────────────────────────────────────────────────

    /** Return the registered schema ID (available after first {@link #serialize} call). */
    public int getSchemaId() { return schemaId; }

    /** Return the Avro schema JSON string used by this serializer. */
    public static String getSchemaJson() { return SCHEMA_JSON; }
}
