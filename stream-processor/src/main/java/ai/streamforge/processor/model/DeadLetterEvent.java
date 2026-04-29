package ai.streamforge.processor.model;

/** A CDC record that could not be processed; routed to the dead-letter Kafka topic. */
public class DeadLetterEvent {

    public String rawPayload;
    public String errorMessage;
    public long failedAtMs;

    public DeadLetterEvent() {}

    public DeadLetterEvent(String rawPayload, String errorMessage) {
        this.rawPayload = rawPayload;
        this.errorMessage = errorMessage;
        this.failedAtMs = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "DeadLetterEvent{error='" + errorMessage + "', failedAtMs=" + failedAtMs + "}";
    }
}
