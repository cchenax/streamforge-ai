package ai.streamforge.processor;

import ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema;
import ai.streamforge.processor.deserialization.SchemaEvolutionFilter;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.model.UserEventCount;
import ai.streamforge.processor.serialization.DeadLetterEventSerializationSchema;
import ai.streamforge.processor.serialization.UserEventCountSerializationSchema;
import ai.streamforge.processor.sink.IcebergSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink CDC aggregation job.
 *
 * Reads Debezium MySQL CDC events from Kafka, counts insert events per user
 * in tumbling event-time windows, and writes {@link UserEventCount} records
 * to an output Kafka topic.
 *
 * <p>Configuration via environment variables:
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}  — default {@code localhost:9092}</li>
 *   <li>{@code KAFKA_SOURCE_TOPIC}       — default {@code cdc.streamforge.user_events}</li>
 *   <li>{@code KAFKA_SINK_TOPIC}         — default {@code user.event.counts}</li>
 *   <li>{@code KAFKA_DLQ_TOPIC}          — default {@code cdc.dead.letter}; set empty to disable</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}     — default {@code flink-cdc-user-event-count}</li>
 *   <li>{@code WINDOW_SIZE_SECONDS}      — default {@code 60}</li>
 *   <li>{@code OUT_OF_ORDERNESS_SECONDS} — default {@code 5}</li>
 * </ul>
 *
 * <p>Optional Apache Iceberg sink (set {@code ICEBERG_ENABLED=true} to activate):
 * <ul>
 *   <li>{@code ICEBERG_ENABLED}       — default {@code false}</li>
 *   <li>{@code ICEBERG_CATALOG_TYPE}  — {@code hadoop} (default), {@code hive}, or {@code rest}</li>
 *   <li>{@code ICEBERG_WAREHOUSE}     — default {@code file:///tmp/iceberg-warehouse}</li>
 *   <li>{@code ICEBERG_DATABASE}      — default {@code streamforge}</li>
 *   <li>{@code ICEBERG_TABLE}         — default {@code user_event_counts}</li>
 *   <li>{@code ICEBERG_S3_ENDPOINT}   — S3/MinIO endpoint, e.g. {@code http://minio:9000}</li>
 *   <li>{@code ICEBERG_S3_ACCESS_KEY} — S3/MinIO access key</li>
 *   <li>{@code ICEBERG_S3_SECRET_KEY} — S3/MinIO secret key</li>
 * </ul>
 */
public class CdcUserEventCountJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcUserEventCountJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrapServers      = env("KAFKA_BOOTSTRAP_SERVERS",  "localhost:9092");
        String sourceTopic           = env("KAFKA_SOURCE_TOPIC",       "cdc.streamforge.user_events");
        String sinkTopic             = env("KAFKA_SINK_TOPIC",         "user.event.counts");
        String dlqTopic              = env("KAFKA_DLQ_TOPIC",          "cdc.dead.letter");
        String consumerGroup         = env("KAFKA_CONSUMER_GROUP",     "flink-cdc-user-event-count");
        long   windowSizeSeconds     = Long.parseLong(env("WINDOW_SIZE_SECONDS",      "60"));
        long   outOfOrdernessSeconds = Long.parseLong(env("OUT_OF_ORDERNESS_SECONDS", "5"));

        LOG.info("Starting CdcUserEventCountJob: source={}, sink={}, dlq={}, window={}s",
                sourceTopic, sinkTopic, dlqTopic.isBlank() ? "disabled" : dlqTopic, windowSizeSeconds);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        KafkaSource<CdcEvent> source = KafkaSource.<CdcEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SchemaAwareCdcDeserializationSchema())
                .build();

        WatermarkStrategy<CdcEvent> watermarkStrategy = WatermarkStrategy
                .<CdcEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessSeconds))
                .withTimestampAssigner((event, recordTimestamp) -> event.tsMs)
                .withIdleness(Duration.ofMinutes(1));

        // ── Schema evolution filter + dead-letter routing ────────────────────
        SingleOutputStreamOperator<CdcEvent> filteredEvents = env
                .fromSource(source, watermarkStrategy, "Kafka CDC Source")
                .process(new SchemaEvolutionFilter())
                .name("Schema Evolution Filter");

        DataStream<DeadLetterEvent> deadLetters =
                filteredEvents.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);

        if (!dlqTopic.isBlank()) {
            KafkaSink<DeadLetterEvent> dlqSink = KafkaSink.<DeadLetterEvent>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.<DeadLetterEvent>builder()
                                    .setTopic(dlqTopic)
                                    .setValueSerializationSchema(new DeadLetterEventSerializationSchema())
                                    .build()
                    ).build();
            deadLetters.sinkTo(dlqSink).name("Kafka DLQ: " + dlqTopic);
        } else {
            deadLetters.print().name("DLQ Log");
        }

        // ── Main aggregation pipeline ────────────────────────────────────────
        DataStream<UserEventCount> counts = filteredEvents
                .filter(e -> "c".equals(e.op) && e.after != null && e.after.userId != null)
                .name("Filter: inserts only")
                .keyBy(e -> e.after.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .aggregate(new EventCountAggregator(), new WindowMetadataFunction())
                .name("Aggregate: count events per user per window");

        KafkaSink<UserEventCount> sink = KafkaSink.<UserEventCount>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserEventCount>builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new UserEventCountSerializationSchema())
                                .build()
                )
                .build();

        counts.sinkTo(sink).name("Kafka Sink: " + sinkTopic);

        // ── Optional Iceberg sink ────────────────────────────────────────────
        if (Boolean.parseBoolean(env("ICEBERG_ENABLED", "false"))) {
            String catalogType  = env("ICEBERG_CATALOG_TYPE",  "hadoop");
            String warehouse    = env("ICEBERG_WAREHOUSE",     "file:///tmp/iceberg-warehouse");
            String database     = env("ICEBERG_DATABASE",      "streamforge");
            String icebergTable = env("ICEBERG_TABLE",         "user_event_counts");
            String s3Endpoint   = env("ICEBERG_S3_ENDPOINT",   "");
            String s3AccessKey  = env("ICEBERG_S3_ACCESS_KEY", "");
            String s3SecretKey  = env("ICEBERG_S3_SECRET_KEY", "");

            LOG.info("Iceberg sink enabled: catalog={}, warehouse={}, table={}.{}",
                    catalogType, warehouse, database, icebergTable);
            IcebergSinkFactory.attach(counts, catalogType, warehouse, database, icebergTable,
                    s3Endpoint, s3AccessKey, s3SecretKey);
        }

        env.execute("CdcUserEventCountJob");
    }

    // ── Aggregation functions ────────────────────────────────────────────────

    /** Incrementally accumulates a running event count. */
    static class EventCountAggregator implements AggregateFunction<CdcEvent, Long, Long> {
        @Override public Long createAccumulator()           { return 0L; }
        @Override public Long add(CdcEvent e, Long acc)     { return acc + 1; }
        @Override public Long getResult(Long acc)           { return acc; }
        @Override public Long merge(Long a, Long b)         { return a + b; }
    }

    /** Attaches window boundaries and the keyed userId to the final count. */
    static class WindowMetadataFunction
            extends ProcessWindowFunction<Long, UserEventCount, String, TimeWindow> {
        @Override
        public void process(
                String userId,
                Context ctx,
                Iterable<Long> counts,
                Collector<UserEventCount> out) {
            out.collect(new UserEventCount(
                    userId,
                    counts.iterator().next(),
                    ctx.window().getStart(),
                    ctx.window().getEnd()));
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
