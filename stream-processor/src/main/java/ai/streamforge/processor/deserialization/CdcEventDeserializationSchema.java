package ai.streamforge.processor.deserialization;

import ai.streamforge.processor.model.CdcEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Deserializes Kafka value bytes into {@link CdcEvent} using Jackson.
 *
 * The ObjectMapper is created lazily on {@link #open} to avoid serialization
 * issues when Flink ships this schema to task managers.
 */
public class CdcEventDeserializationSchema implements DeserializationSchema<CdcEvent> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public CdcEvent deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return objectMapper.readValue(message, CdcEvent.class);
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
