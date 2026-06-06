package ai.streamforge.processor;

import ai.streamforge.processor.CdcAggregationFunctions.EventCountAggregator;
import ai.streamforge.processor.CdcAggregationFunctions.WindowMetadataFunction;
import ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema;
import ai.streamforge.processor.deserialization.SchemaEvolutionFilter;
import ai.streamforge.processor.drift.DriftSignal;
import ai.streamforge.processor.drift.DriftSignalFunction;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.model.DriftSignal;
import ai.streamforge.processor.model.QuarantineEvent;
import ai.streamforge.processor.model.UserEventCount;
import ai.streamforge.processor.serialization.DeadLetterEventSerializationSchema;
import ai.streamforge.processor.serialization.DriftSignalSerializationSchema;
import ai.streamforge.processor.serialization.UserEventCountSerializationSchema;
import ai.streamforge.processor.sink.IcebergSinkFactory;
import ai.streamforge.processor.validation.CdcEventValidator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Flink CDC aggregation job — primary entry point.
 *
 * <p>Reads Debezium MySQL CDC events from Kafka, counts insert events per user
 * in tumbling event-time windows, and writes {@link UserEventCount} to two sinks:
 * <ol>
 *   <li>Kafka ({@code user.event.counts}) — for real-time consumers.</li>
 *   <li>Iceberg on MinIO — the primary analytics store, always enabled.</li>
 * </ol>
 *
 * <h2>Pipeline stages</h2>
 * <ol>
 *   <li><b>Schema evolution filter</b> — routes unresolvable records to the DLQ topic.</li>
 *   <li><b>Inline validator</b> — checks schema, nulls, and value ranges; quarantines
 *       bad rows to the quarantine topic.</li>
 *   <li><b>Drift monitor</b> — side pipeline keyed by constant "global"; detects
 *       row-count deltas and per-user distribution skew; emits signals to the drift topic.</li>
 *   <li><b>Main aggregation</b> — counts inserts per user per tumbling window;
 *       writes to the sink topic and optionally Iceberg.</li>
 * </ol>
 *
 * <h2>Configuration (environment variables)</h2>
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}  — default {@code localhost:9092}</li>
 *   <li>{@code KAFKA_SOURCE_TOPIC}       — default {@code cdc.streamforge.user_events}</li>
 *   <li>{@code KAFKA_SINK_TOPIC}         — default {@code user.event.counts}</li>
 *   <li>{@code KAFKA_DLQ_TOPIC}          — default {@code cdc.dead.letter}; blank = disabled</li>
 *   <li>{@code KAFKA_QUARANTINE_TOPIC}   — default {@code cdc.quarantine}</li>
 *   <li>{@code KAFKA_DRIFT_TOPIC}        — default {@code cdc.drift.signals}</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}     — default {@code flink-cdc-user-event-count}</li>
 *   <li>{@code KAFKA_SINK_COMPRESSION}   — {@code none} (default), {@code snappy}, {@code lz4}, {@code zstd}</li>
 *   <li>{@code KAFKA_SINK_BATCH_SIZE_BYTES} — default {@code 16384}</li>
 *   <li>{@code KAFKA_SINK_LINGER_MS}     — default {@code 5}</li>
 *   <li>{@code KAFKA_SINK_ACKS}          — default {@code all}</li>
 * </ul>
 *
 * <h3>Window</h3>
 * <ul>
 *   <li>{@code WINDOW_SIZE_SECONDS}      — default {@code 60}</li>
 *   <li>{@code OUT_OF_ORDERNESS_SECONDS} — default {@code 5}</li>
 *   <li>{@code VALIDATION_MAX_FUTURE_SKEW_MS} — default {@code 3600000} (1 h)</li>
 *   <li>{@code VALIDATION_USER_ID_MAX_LEN}    — default {@code 128}</li>
 *   <li>{@code DRIFT_COUNT_DELTA_THRESHOLD}   — default {@code 0.5}</li>
 *   <li>{@code DRIFT_SKEW_THRESHOLD}          — default {@code 0.3}</li>
 *   <li>{@code DRIFT_MIN_WINDOW_COUNT}        — default {@code 10}</li>
 * </ul>
 *
 * <p>Flink parallelism and checkpointing knobs:
 * <ul>
 *   <li>{@code FLINK_PARALLELISM}              — operator parallelism, default {@code -1} (use cluster default)</li>
 *   <li>{@code CHECKPOINT_INTERVAL_MS}         — checkpoint interval in ms, default {@code 30000}</li>
 *   <li>{@code CHECKPOINT_TIMEOUT_MS}          — per-checkpoint timeout in ms, default {@code 60000}</li>
 *   <li>{@code CHECKPOINT_MIN_PAUSE_MS}        — min pause between checkpoints in ms, default {@code 0}</li>
 *   <li>{@code CHECKPOINT_MAX_CONCURRENT}      — max concurrent checkpoints, default {@code 1}</li>
 *   <li>{@code CHECKPOINT_MODE}                — {@code exactly_once} (default) or {@code at_least_once}</li>
 *   <li>{@code CHECKPOINT_UNALIGNED}           — enable unaligned checkpoints, default {@code false}</li>
 *   <li>{@code RESTART_ATTEMPTS}               — fixed-delay restart attempts, default {@code 3}</li>
 *   <li>{@code RESTART_DELAY_MS}               — fixed-delay restart interval in ms, default {@code 10000}</li>
 * </ul>
 *
 * <p>Kafka producer tuning knobs (applied to the main sink and DLQ sink):
 * <ul>
 *   <li>{@code KAFKA_SINK_COMPRESSION}         — {@code none} (default), {@code snappy}, {@code lz4}, {@code zstd}, {@code gzip}</li>
 *   <li>{@code KAFKA_SINK_BATCH_SIZE_BYTES}    — producer batch.size in bytes, default {@code 16384} (16 KB)</li>
 *   <li>{@code KAFKA_SINK_LINGER_MS}           — producer linger.ms, default {@code 5}</li>
 *   <li>{@code KAFKA_SINK_BUFFER_MEMORY_BYTES} — producer buffer.memory in bytes, default {@code 33554432} (32 MB)</li>
 *   <li>{@code KAFKA_SINK_ACKS}                — producer acks, default {@code all}</li>
 * </ul>
 *
 * <p>Optional Apache Iceberg sink (set {@code ICEBERG_ENABLED=true} to activate):
 * <ul>
 *   <li>{@code ICEBERG_ENABLED}       — default {@code true}; set {@code false} only for local dev</li>
 *   <li>{@code ICEBERG_CATALOG_TYPE}  — {@code hadoop} (default), {@code hive}, or {@code rest}</li>
 *   <li>{@code ICEBERG_WAREHOUSE}     — default {@code s3a://streamforge/warehouse}</li>
 *   <li>{@code ICEBERG_DATABASE}      — default {@code streamforge}</li>
 *   <li>{@code ICEBERG_TABLE}         — default {@code user_event_counts}</li>
 *   <li>{@code ICEBERG_S3_ENDPOINT}   — MinIO endpoint, e.g. {@code http://minio:9000}</li>
 *   <li>{@code ICEBERG_S3_ACCESS_KEY} — S3/MinIO access key</li>
 *   <li>{@code ICEBERG_S3_SECRET_KEY} — S3/MinIO secret key</li>
 *   <li>{@code ICEBERG_REST_URI}      — REST catalog base URL, e.g. {@code http://iceberg-rest:8181};
 *       required when {@code ICEBERG_CATALOG_TYPE=rest}</li>
 * </ul>
 */
public class CdcUserEventCountJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcUserEventCountJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrapServers      = env("KAFKA_BOOTSTRAP_SERVERS",    "localhost:9092");
        String sourceTopic           = env("KAFKA_SOURCE_TOPIC",         "cdc.streamforge.user_events");
        String sinkTopic             = env("KAFKA_SINK_TOPIC",           "user.event.counts");
        String dlqTopic              = env("KAFKA_DLQ_TOPIC",            "cdc.dead.letter");
        String driftTopic            = env("KAFKA_DRIFT_SIGNALS_TOPIC",  "feature.drift.signals");
        String consumerGroup         = env("KAFKA_CONSUMER_GROUP",       "flink-cdc-user-event-count");
        long   windowSizeSeconds     = Long.parseLong(env("WINDOW_SIZE_SECONDS",      "60"));
        long   outOfOrdernessSeconds = Long.parseLong(env("OUT_OF_ORDERNESS_SECONDS", "5"));

        LOG.info("Starting CdcUserEventCountJob: source={}, sink={}, dlq={}, drift={}, window={}s",
                sourceTopic, sinkTopic, dlqTopic.isBlank() ? "disabled" : dlqTopic,
                driftTopic.isBlank() ? "disabled" : driftTopic, windowSizeSeconds);

        // ── Iceberg (first-class sink, enabled by default) ───────────────────
        boolean icebergEnabled = Boolean.parseBoolean(env("ICEBERG_ENABLED", "true"));
        String  catalogType    = env("ICEBERG_CATALOG_TYPE",  "hadoop");
        String  warehouse      = env("ICEBERG_WAREHOUSE",     "s3a://streamforge/warehouse");
        String  database       = env("ICEBERG_DATABASE",      "streamforge");
        String  icebergTable   = env("ICEBERG_TABLE",         "user_event_counts");
        String  s3Endpoint     = env("ICEBERG_S3_ENDPOINT",   "");
        String  s3AccessKey    = env("ICEBERG_S3_ACCESS_KEY", "");
        String  s3SecretKey    = env("ICEBERG_S3_SECRET_KEY", "");

        LOG.info("Starting CdcUserEventCountJob: source={}, sink={}, dlq={}, window={}s " +
                 "parallelism={}, ckpt={}ms mode={} iceberg={}",
                sourceTopic, sinkTopic, dlqTopic.isBlank() ? "disabled" : dlqTopic,
                windowSizeSeconds,
                parallelism < 0 ? "cluster-default" : parallelism,
                ckptIntervalMs, ckptModeStr,
                icebergEnabled ? warehouse + "/" + database + "." + icebergTable : "disabled");

        // ── Flink environment ────────────────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }

        // ── Checkpointing ────────────────────────────────────────────────────
        CheckpointingMode ckptMode = "at_least_once".equalsIgnoreCase(checkpointModeStr)
                ? CheckpointingMode.AT_LEAST_ONCE
                : CheckpointingMode.EXACTLY_ONCE;

        env.enableCheckpointing(checkpointIntervalMs, ckptMode);

        CheckpointConfig ckptCfg = env.getCheckpointConfig();
        ckptCfg.setCheckpointTimeout(checkpointTimeoutMs);
        ckptCfg.setMinPauseBetweenCheckpoints(checkpointMinPauseMs);
        ckptCfg.setMaxConcurrentCheckpoints(maxConcurrentCkpts);
        ckptCfg.enableUnalignedCheckpoints(unalignedCheckpoints);
        ckptCfg.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(restartAttempts, restartDelayMs));

        // ── Source ───────────────────────────────────────────────────────────
        KafkaSource<CdcEvent> source = KafkaSource.<CdcEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SchemaAwareCdcDeserializationSchema())
                .build();

        WatermarkStrategy<CdcEvent> watermarks = WatermarkStrategy
                .<CdcEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrderSecs))
                .withTimestampAssigner((event, ts) -> event.tsMs)
                .withIdleness(Duration.ofMinutes(1));

        // ── Stage 1: Schema evolution filter → DLQ ───────────────────────────
        SingleOutputStreamOperator<CdcEvent> schemaFiltered = env
                .fromSource(source, watermarks, "Kafka CDC Source")
                .process(new SchemaEvolutionFilter())
                .name("Schema Evolution Filter");

        DataStream<DeadLetterEvent> deadLetters =
                schemaFiltered.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);

        Properties kafkaSinkProps = buildKafkaSinkProps();

        if (!dlqTopic.isBlank()) {
            deadLetters.sinkTo(kafkaSink(bootstrapServers, dlqTopic,
                    new DeadLetterEventSerializationSchema()))
                    .name("Kafka DLQ: " + dlqTopic);
        } else {
            deadLetters.print().name("DLQ Log");
        }

        // ── Stage 2: Inline validator → quarantine ───────────────────────────
        SingleOutputStreamOperator<CdcEvent> validatedEvents = schemaFiltered
                .process(new CdcEventValidator(maxFutureSkewMs, userIdMaxLen))
                .name("Inline Validator");

        DataStream<QuarantineEvent> quarantined =
                validatedEvents.getSideOutput(CdcEventValidator.QUARANTINE_TAG);

        quarantined.sinkTo(kafkaSink(bootstrapServers, quarantineTopic,
                new QuarantineEventSerializationSchema()))
                .name("Kafka Quarantine: " + quarantineTopic);

        // ── Stage 3: Drift monitor (side pipeline) ───────────────────────────
        // Key by constant so all events within a window reach a single task,
        // enabling cross-user skew detection without a shuffle on the hot path.
        DataStream<DriftSignal> driftSignals = validatedEvents
                .filter(e -> "c".equals(e.op) || "r".equals(e.op))
                .name("Drift filter: inserts + reads")
                .keyBy(e -> "global")
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .process(new DriftMonitorFunction(deltaThreshold, skewThreshold, minWindowCount))
                .name("Drift Monitor");

        driftSignals.sinkTo(kafkaSink(bootstrapServers, driftTopic,
                new DriftSignalSerializationSchema()))
                .name("Kafka Drift: " + driftTopic);

        // ── Stage 4: Main aggregation pipeline ───────────────────────────────
        DataStream<UserEventCount> counts = validatedEvents
                .filter(e -> "c".equals(e.op) && e.after != null && e.after.userId != null)
                .name("Filter: inserts only")
                .keyBy(e -> e.after.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .aggregate(new EventCountAggregator(), new WindowMetadataFunction())
                .name("Aggregate: count events per user per window");

        counts.sinkTo(kafkaSink(bootstrapServers, sinkTopic,
                new UserEventCountSerializationSchema()))
                .name("Kafka Sink: " + sinkTopic);

        // ── Drift signal aggregation ─────────────────────────────────────────
        // Re-key by windowStartMs so all users from the same window are co-located,
        // then flush a per-window distribution snapshot after the window closes.
        if (!driftTopic.isBlank()) {
            long allowedLatenessMs = outOfOrdernessSeconds * 1_000 * 2; // 2× watermark lag
            DataStream<DriftSignal> driftSignals = counts
                    .keyBy(e -> e.windowStartMs)
                    .process(new DriftSignalFunction(allowedLatenessMs))
                    .name("Drift Signal Aggregator");

            KafkaSink<DriftSignal> driftSink = KafkaSink.<DriftSignal>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.<DriftSignal>builder()
                                    .setTopic(driftTopic)
                                    .setValueSerializationSchema(new DriftSignalSerializationSchema())
                                    .build()
                    ).build();
            driftSignals.sinkTo(driftSink).name("Kafka Drift Sink: " + driftTopic);
        }

        // ── Optional Iceberg sink ────────────────────────────────────────────
        if (Boolean.parseBoolean(env("ICEBERG_ENABLED", "false"))) {
            String catalogType  = env("ICEBERG_CATALOG_TYPE",  "hadoop");
            String warehouse    = env("ICEBERG_WAREHOUSE",     "file:///tmp/iceberg-warehouse");
            String database     = env("ICEBERG_DATABASE",      "streamforge");
            String icebergTable = env("ICEBERG_TABLE",         "user_event_counts");
            String s3Endpoint   = env("ICEBERG_S3_ENDPOINT",   "");
            String s3AccessKey  = env("ICEBERG_S3_ACCESS_KEY", "");
            String s3SecretKey  = env("ICEBERG_S3_SECRET_KEY", "");
            String restUri      = env("ICEBERG_REST_URI",      "");

            LOG.info("Iceberg sink enabled: catalog={}, warehouse={}, table={}.{}",
                    catalogType, warehouse, database, icebergTable);
            IcebergSinkFactory.attach(counts, catalogType, warehouse, database, icebergTable,
                    s3Endpoint, s3AccessKey, s3SecretKey, restUri);
        }

        env.execute("CdcUserEventCountJob");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static <T> KafkaSink<T> kafkaSink(
            String bootstrapServers,
            String topic,
            org.apache.flink.api.common.serialization.SerializationSchema<T> schema) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<T>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(schema)
                                .build())
                .build();
    }

    static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    /**
     * Builds Kafka producer properties from environment variables.
     * These apply to both the main sink and the DLQ sink.
     */
    private static Properties buildKafkaSinkProps() {
        Properties props = new Properties();

        // Batching: larger batch + linger improves throughput at the cost of latency
        props.setProperty("batch.size",    env("KAFKA_SINK_BATCH_SIZE_BYTES",    "16384"));
        props.setProperty("linger.ms",     env("KAFKA_SINK_LINGER_MS",           "5"));
        props.setProperty("buffer.memory", env("KAFKA_SINK_BUFFER_MEMORY_BYTES", "33554432"));

        // Compression: reduces network/disk I/O; snappy/lz4 best for throughput, zstd for ratio
        String compression = env("KAFKA_SINK_COMPRESSION", "none");
        if (!"none".equalsIgnoreCase(compression)) {
            props.setProperty("compression.type", compression);
        }

        // Durability: "all" waits for all in-sync replicas; "1" or "0" for lower latency
        props.setProperty("acks", env("KAFKA_SINK_ACKS", "all"));

        return props;
    }
}
