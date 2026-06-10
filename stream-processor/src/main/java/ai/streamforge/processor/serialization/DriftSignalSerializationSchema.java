package ai.streamforge.processor.serialization;

import ai.streamforge.processor.drift.DriftSignal;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Serializes {@link DriftSignal} to JSON bytes for the Kafka drift-signals sink. */
public class DriftSignalSerializationSchema implements SerializationSchema<DriftSignal> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(DriftSignal element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize DriftSignal: " + element, e);
        }
    }
}
