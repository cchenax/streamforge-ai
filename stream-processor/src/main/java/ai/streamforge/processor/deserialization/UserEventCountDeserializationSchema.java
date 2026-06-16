package ai.streamforge.processor.deserialization;

import ai.streamforge.processor.model.UserEventCount;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/** Deserializes JSON bytes from the {@code user.event.counts} topic into {@link UserEventCount}. */
public class UserEventCountDeserializationSchema implements DeserializationSchema<UserEventCount> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public UserEventCount deserialize(byte[] message) {
        try {
            return objectMapper.readValue(message, UserEventCount.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize UserEventCount", e);
        }
    }

    @Override
    public boolean isEndOfStream(UserEventCount nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserEventCount> getProducedType() {
        return TypeInformation.of(UserEventCount.class);
    }
}
