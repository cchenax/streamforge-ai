package ai.streamforge.processor.deserialization;

import ai.streamforge.processor.model.CdcEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drop-in replacement for {@link CdcEventDeserializationSchema} that adds schema
 * evolution support via {@link SchemaEvolutionHandler}:
 *
 * <ul>
 *   <li>Detects schema version (V1 / V2 / UNKNOWN) and stores it on the event.</li>
 *   <li>Normalizes renamed fields (uid → user_id, type → event_type, ts → created_at).</li>
 *   <li>Extracts V2 columns (session_id, ip_address) when present.</li>
 *   <li>Logs and skips unparseable records instead of crashing the job; invalid
 *       inserts are further routed to the dead-letter side output by
 *       {@link SchemaEvolutionFilter}.</li>
 * </ul>
 */
public class SchemaAwareCdcDeserializationSchema implements DeserializationSchema<CdcEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaAwareCdcDeserializationSchema.class);

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public CdcEvent deserialize(byte[] message) {
        if (message == null) {
            return null;
        }
        try {
            return SchemaEvolutionHandler.handle(message, objectMapper);
        } catch (Exception e) {
            LOG.warn("Skipping unparseable CDC record ({}): {}", e.getClass().getSimpleName(), e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(CdcEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CdcEvent> getProducedType() {
        return TypeInformation.of(CdcEvent.class);
    }
}
