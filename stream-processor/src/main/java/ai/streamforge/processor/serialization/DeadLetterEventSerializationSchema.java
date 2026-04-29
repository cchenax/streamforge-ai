package ai.streamforge.processor.serialization;

import ai.streamforge.processor.model.DeadLetterEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Serializes {@link DeadLetterEvent} to JSON bytes for the dead-letter Kafka topic. */
public class DeadLetterEventSerializationSchema implements SerializationSchema<DeadLetterEvent> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(DeadLetterEvent element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize DeadLetterEvent: " + element, e);
        }
    }
}
