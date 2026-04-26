package ai.streamforge.processor.model;

/** Aggregated event count for one user within a tumbling time window. */
public class UserEventCount {

    public String userId;
    public long count;
    public long windowStartMs;
    public long windowEndMs;

    /** Required by Flink's POJO serializer. */
    public UserEventCount() {}

    public UserEventCount(String userId, long count, long windowStartMs, long windowEndMs) {
        this.userId = userId;
        this.count = count;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
    }

    @Override
    public String toString() {
        return "UserEventCount{userId='" + userId + "', count=" + count
                + ", windowStartMs=" + windowStartMs + ", windowEndMs=" + windowEndMs + "}";
    }
}
