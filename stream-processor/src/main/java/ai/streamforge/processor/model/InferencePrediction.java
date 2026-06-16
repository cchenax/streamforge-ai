package ai.streamforge.processor.model;

/** Model server prediction attached to one {@link UserEventCount} window. */
public class InferencePrediction {

    public String userId;
    public long   windowStartMs;
    public long   windowEndMs;
    public long   eventCount;

    /** Predicted activity label: "low" | "medium" | "high" */
    public String label;

    /** Model confidence in [0, 1]. */
    public double confidence;

    /** Wall-clock latency of the model-server round-trip, in milliseconds. */
    public long inferenceLatencyMs;

    /** Epoch-ms when this prediction was produced. */
    public long predictedAtMs;

    public InferencePrediction() {}

    public InferencePrediction(
            String userId,
            long windowStartMs,
            long windowEndMs,
            long eventCount,
            String label,
            double confidence,
            long inferenceLatencyMs) {
        this.userId             = userId;
        this.windowStartMs      = windowStartMs;
        this.windowEndMs        = windowEndMs;
        this.eventCount         = eventCount;
        this.label              = label;
        this.confidence         = confidence;
        this.inferenceLatencyMs = inferenceLatencyMs;
        this.predictedAtMs      = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "InferencePrediction{userId='" + userId + "', label='" + label
                + "', confidence=" + String.format("%.3f", confidence)
                + ", latencyMs=" + inferenceLatencyMs + "}";
    }
}
