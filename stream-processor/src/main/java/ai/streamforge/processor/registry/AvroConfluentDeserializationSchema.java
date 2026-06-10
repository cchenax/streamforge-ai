package ai.streamforge.processor.registry;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.SchemaVersion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Flink {@link DeserializationSchema} that decodes <b>Confluent-framed Avro</b>
 * bytes into {@link CdcEvent} POJOs.
 *
 * <h3>Wire format (Confluent Schema Registry framing)</h3>
 * <pre>
 *   Byte 0       : magic byte = 0x00
 *   Bytes 1–4    : schema ID (big-endian uint32)
 *   Bytes 5+     : Avro binary-encoded payload
 * </pre>
 *
 * <h3>Schema resolution</h3>
 * <ol>
 *   <li>The schema ID is extracted from bytes 1–4.</li>
 *   <li>The writer schema is fetched from the registry (cached after first use).</li>
 *   <li>An optional <em>reader schema</em> can be supplied for schema evolution /
 *       field projection; when absent the writer schema is used directly.</li>
 *   <li>The decoded {@link GenericRecord} is mapped to a {@link CdcEvent} POJO.</li>
 * </ol>
 *
 * <h3>Fallback behaviour</h3>
 * If the first byte is <em>not</em> the Confluent magic byte, the deserializer
 * falls back to plain JSON parsing via
 * {@link ai.streamforge.processor.deserialization.SchemaEvolutionHandler} so that
 * existing JSON-encoded CDC events keep flowing during a migration window.
 * Set {@code fallbackToJson=false} to disable this and fail-fast instead.
 *
 * <h3>Usage</h3>
 * <pre>
 *   SchemaRegistryConfig config = SchemaRegistryConfig.fromEnv();
 *   DeserializationSchema&lt;CdcEvent&gt; schema =
 *       new AvroConfluentDeserializationSchema(config);
 *
 *   // optional: pin a reader schema for projection
 *   DeserializationSchema&lt;CdcEvent&gt; projected =
 *       new AvroConfluentDeserializationSchema(config, readerSchemaJson, true);
 * </pre>
 *
 * <p>Drop-in replacement for
 * {@link ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema}.
 */
public class AvroConfluentDeserializationSchema implements DeserializationSchema<CdcEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AvroConfluentDeserializationSchema.class);

    /** Confluent wire-format magic byte. */
    private static final byte MAGIC_BYTE = 0x00;
    /** Size of the wire-format header: 1 (magic) + 4 (schema ID). */
    private static final int HEADER_SIZE = 5;

    private final SchemaRegistryConfig registryConfig;
    private final String readerSchemaJson;   // null → use writer schema
    private final boolean fallbackToJson;

    private transient SchemaRegistryHttpClient httpClient;
    private transient SchemaCache              schemaCache;
    private transient Schema                   readerSchema;
    private transient com.fasterxml.jackson.databind.ObjectMapper jsonMapper;

    // ── Constructors ─────────────────────────────────────────────────────────

    /**
     * Create a deserializer that uses the writer schema for decoding and falls back
     * to JSON when the magic byte is absent.
     *
     * @param registryConfig  Registry connection settings.
     */
    public AvroConfluentDeserializationSchema(SchemaRegistryConfig registryConfig) {
        this(registryConfig, null, true);
    }

    /**
     * Full constructor.
     *
     * @param registryConfig   Registry connection settings.
     * @param readerSchemaJson Optional reader schema JSON (for schema evolution).
     *                         Pass {@code null} to use the writer schema as-is.
     * @param fallbackToJson   If {@code true}, parse as JSON when magic byte is absent.
     */
    public AvroConfluentDeserializationSchema(
            SchemaRegistryConfig registryConfig,
            String readerSchemaJson,
            boolean fallbackToJson) {
        this.registryConfig  = registryConfig;
        this.readerSchemaJson = readerSchemaJson;
        this.fallbackToJson  = fallbackToJson;
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void open(InitializationContext context) {
        httpClient  = new SchemaRegistryHttpClient(registryConfig);
        schemaCache = new SchemaCache(registryConfig.getCacheSize());
        jsonMapper  = new com.fasterxml.jackson.databind.ObjectMapper();
        if (readerSchemaJson != null && !readerSchemaJson.isBlank()) {
            readerSchema = new Schema.Parser().parse(readerSchemaJson);
        }
        LOG.info("[AvroConfluentDeserializationSchema] opened — registry={}, fallbackJson={}",
                registryConfig.getUrl(), fallbackToJson);
    }

    // ── DeserializationSchema ────────────────────────────────────────────────

    @Override
    public CdcEvent deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }

        // ── Confluent wire-format path ────────────────────────────────────
        if (message[0] == MAGIC_BYTE) {
            if (message.length < HEADER_SIZE) {
                LOG.warn("[AvroConfluentDeserializationSchema] Message too short ({} bytes); skipping",
                        message.length);
                return null;
            }
            int schemaId = ByteBuffer.wrap(message, 1, 4).getInt();
            Schema writerSchema = getOrFetchSchema(schemaId);

            try {
                GenericRecord record = decodeAvro(message, writerSchema);
                return genericRecordToCdcEvent(record);
            } catch (Exception e) {
                LOG.warn("[AvroConfluentDeserializationSchema] Avro decode failed for schemaId={}: {}",
                        schemaId, e.getMessage());
                return null;
            }
        }

        // ── JSON fallback path ────────────────────────────────────────────
        if (fallbackToJson) {
            try {
                return ai.streamforge.processor.deserialization.SchemaEvolutionHandler
                        .handle(message, jsonMapper);
            } catch (Exception e) {
                LOG.warn("[AvroConfluentDeserializationSchema] JSON fallback failed: {}", e.getMessage());
                return null;
            }
        }

        LOG.warn("[AvroConfluentDeserializationSchema] Not Confluent-framed and fallback disabled; skipping message");
        return null;
    }

    @Override
    public boolean isEndOfStream(CdcEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CdcEvent> getProducedType() {
        return TypeInformation.of(CdcEvent.class);
    }

    // ── Schema resolution ────────────────────────────────────────────────────

    private Schema getOrFetchSchema(int schemaId) throws IOException {
        Schema cached = schemaCache.get(schemaId);
        if (cached != null) {
            return cached;
        }
        Schema schema = httpClient.getSchemaById(schemaId);
        schemaCache.put(schemaId, schema);
        return schema;
    }

    // ── Avro decoding ────────────────────────────────────────────────────────

    private GenericRecord decodeAvro(byte[] message, Schema writerSchema) throws IOException {
        GenericDatumReader<GenericRecord> reader = (readerSchema != null)
                ? new GenericDatumReader<>(writerSchema, readerSchema)
                : new GenericDatumReader<>(writerSchema);

        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(message, HEADER_SIZE, message.length - HEADER_SIZE, null);
        return reader.read(null, decoder);
    }

    // ── GenericRecord → CdcEvent mapping ────────────────────────────────────

    /**
     * Map an Avro {@link GenericRecord} to a {@link CdcEvent} POJO.
     *
     * <p>Field names match the Avro schemas in {@code schema-registry/schemas/}.
     * Unknown fields are silently ignored (forward-compatible).
     */
    private static CdcEvent genericRecordToCdcEvent(GenericRecord record) {
        CdcEvent event = new CdcEvent();
        event.op   = toStr(record.get("op"));
        event.tsMs = toLong(record.get("ts_ms"), 0L);

        Object afterObj = record.get("after");
        if (afterObj instanceof GenericRecord) {
            GenericRecord after = (GenericRecord) afterObj;
            CdcEvent.UserEventRow row = new CdcEvent.UserEventRow();
            row.userId    = toStr(after.get("user_id"));
            row.eventType = toStr(after.get("event_type"));
            row.createdAt = toLongOrNull(after.get("created_at"));
            row.sessionId = toStr(after.get("session_id"));
            row.ipAddress = toStr(after.get("ip_address"));
            row.metadata  = toStr(after.get("metadata"));
            event.after   = row;

            // Infer schema version from which fields are populated
            event.schemaVersion = inferVersion(row);
        } else {
            event.schemaVersion = SchemaVersion.UNKNOWN;
        }
        return event;
    }

    private static SchemaVersion inferVersion(CdcEvent.UserEventRow row) {
        if (row.metadata != null) return SchemaVersion.V3;
        if (row.sessionId != null || row.ipAddress != null) return SchemaVersion.V2;
        if (row.userId != null || row.eventType != null)    return SchemaVersion.V1;
        return SchemaVersion.UNKNOWN;
    }

    // ── Type helpers ─────────────────────────────────────────────────────────

    private static String toStr(Object val) {
        return val == null ? null : val.toString();
    }

    private static long toLong(Object val, long defaultVal) {
        if (val instanceof Number) return ((Number) val).longValue();
        return defaultVal;
    }

    private static Long toLongOrNull(Object val) {
        if (val instanceof Number) return ((Number) val).longValue();
        return null;
    }
}
