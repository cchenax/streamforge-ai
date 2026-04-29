package ai.streamforge.processor.deserialization;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Routes {@link CdcEvent} records between the main stream and the dead-letter
 * side output.
 *
 * <p>A record is sent to the dead-letter queue when it is an insert ({@code op=c})
 * or snapshot read ({@code op=r}) whose {@code after.userId} could not be resolved
 * after schema normalization.  This covers the case where the column was renamed to
 * an unrecognised alias or removed entirely.  All other events (updates, deletes,
 * and valid inserts) pass through to the main output unchanged.
 *
 * <p>Wire in the job:
 * <pre>{@code
 * SingleOutputStreamOperator<CdcEvent> filtered =
 *         rawEvents.process(new SchemaEvolutionFilter()).name("Schema Evolution Filter");
 * DataStream<DeadLetterEvent> dlq = filtered.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);
 * }</pre>
 */
public class SchemaEvolutionFilter extends ProcessFunction<CdcEvent, CdcEvent> {

    public static final OutputTag<DeadLetterEvent> DLQ_TAG =
            new OutputTag<DeadLetterEvent>("dead-letter") {};

    @Override
    public void processElement(
            CdcEvent event,
            Context ctx,
            Collector<CdcEvent> out) {

        if (isInsertOrRead(event) && lacksUserId(event)) {
            ctx.output(DLQ_TAG, new DeadLetterEvent(
                    event.toString(),
                    "user_id unresolvable after schema normalization (version=" + event.schemaVersion + ")"));
            return;
        }
        out.collect(event);
    }

    private static boolean isInsertOrRead(CdcEvent event) {
        return "c".equals(event.op) || "r".equals(event.op);
    }

    private static boolean lacksUserId(CdcEvent event) {
        return event.after == null || event.after.userId == null;
    }
}
