package ai.streamforge.processor.serialization;

import ai.streamforge.processor.model.InferencePrediction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Serializes {@link InferencePrediction} to JSON bytes for the Kafka sink. */
public class InferencePredictionSerializationSchema implements SerializationSchema<InferencePrediction> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(InferencePrediction element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize InferencePrediction: " + element, e);
        }
    }
}
