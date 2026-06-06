package ai.streamforge.processor.drift;

import java.util.Map;

/**
 * Per-window snapshot of the user-event-count distribution.
 * Emitted by {@link DriftSignalFunction} and written to a dedicated Kafka topic
 * for downstream drift analysis.
 */
public class DriftSignal {

    /** Window start epoch-ms (matches {@code UserEventCount.windowStartMs}). */
    public long windowStartMs;

    /** Window end epoch-ms. */
    public long windowEndMs;

    /** Number of distinct users seen in this window. */
    public int uniqueUsers;

    /** Sum of all event counts across users in this window. */
    public long totalEvents;

    /**
     * Histogram of event counts: bucket label → user count in that bucket.
     * Buckets: "1", "2-5", "6-10", "11-20", "21-50", "51-100", "100+".
     */
    public Map<String, Long> countHistogram;

    /** Required by Flink's POJO serializer. */
    public DriftSignal() {}

    public DriftSignal(long windowStartMs, long windowEndMs, int uniqueUsers,
                       long totalEvents, Map<String, Long> countHistogram) {
        this.windowStartMs  = windowStartMs;
        this.windowEndMs    = windowEndMs;
        this.uniqueUsers    = uniqueUsers;
        this.totalEvents    = totalEvents;
        this.countHistogram = countHistogram;
    }

    @Override
    public String toString() {
        return "DriftSignal{window=[" + windowStartMs + "," + windowEndMs
                + "), uniqueUsers=" + uniqueUsers + ", totalEvents=" + totalEvents
                + ", histogram=" + countHistogram + "}";
    }
}
