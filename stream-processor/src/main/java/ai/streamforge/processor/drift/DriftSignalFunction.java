package ai.streamforge.processor.drift;

import ai.streamforge.processor.model.UserEventCount;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Accumulates per-window {@link UserEventCount} records (keyed by windowStartMs)
 * and emits a {@link DriftSignal} when the window is complete.
 *
 * <p>A processing-time timer fires at {@code windowEndMs + allowedLatenessMs} to flush
 * the accumulated distribution snapshot for that window.
 *
 * <p>Wire-up in the main job:
 * <pre>{@code
 *   counts
 *     .keyBy(e -> e.windowStartMs)
 *     .process(new DriftSignalFunction(allowedLatenessMs))
 *     .name("Drift Signal Aggregator")
 *     .sinkTo(driftSink);
 * }</pre>
 */
public class DriftSignalFunction
        extends KeyedProcessFunction<Long, UserEventCount, DriftSignal> {

    private static final long serialVersionUID = 1L;

    /** Extra ms to wait after windowEnd before flushing, to absorb stragglers. */
    private final long allowedLatenessMs;

    /**
     * State: userId → event count for the current window key (windowStartMs).
     * The key is the windowStartMs itself (from KeyedProcessFunction).
     */
    private transient MapState<String, Long> userCounts;

    public DriftSignalFunction(long allowedLatenessMs) {
        this.allowedLatenessMs = allowedLatenessMs;
    }

    @Override
    public void open(Configuration parameters) {
        userCounts = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("drift-user-counts", String.class, Long.class));
    }

    @Override
    public void processElement(UserEventCount value, Context ctx, Collector<DriftSignal> out)
            throws Exception {
        userCounts.put(value.userId, value.count);

        // Register flush timer once (idempotent — Flink deduplicates same timestamp timers per key).
        long flushAt = value.windowEndMs + allowedLatenessMs;
        ctx.timerService().registerProcessingTimeTimer(flushAt);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DriftSignal> out)
            throws Exception {
        long windowStartMs = ctx.getCurrentKey();

        int  uniqueUsers = 0;
        long totalEvents = 0;
        Map<String, Long> histogram = initHistogram();

        for (Map.Entry<String, Long> entry : userCounts.entries()) {
            uniqueUsers++;
            long count = entry.getValue();
            totalEvents += count;
            histogram.merge(bucket(count), 1L, Long::sum);
        }

        // windowEndMs = windowStartMs + (timer - allowedLatenessMs - windowStartMs)
        // Reconstruct from timer: flushAt = windowEndMs + allowedLatenessMs
        long windowEndMs = timestamp - allowedLatenessMs;

        out.collect(new DriftSignal(windowStartMs, windowEndMs, uniqueUsers, totalEvents, histogram));
        userCounts.clear();
    }

    // ── Histogram helpers ────────────────────────────────────────────────────

    private static Map<String, Long> initHistogram() {
        Map<String, Long> h = new HashMap<>();
        for (String b : BUCKETS) h.put(b, 0L);
        return h;
    }

    private static final String[] BUCKETS = {"1", "2-5", "6-10", "11-20", "21-50", "51-100", "100+"};

    static String bucket(long count) {
        if (count == 1)          return "1";
        if (count <= 5)          return "2-5";
        if (count <= 10)         return "6-10";
        if (count <= 20)         return "11-20";
        if (count <= 50)         return "21-50";
        if (count <= 100)        return "51-100";
        return "100+";
    }
}
