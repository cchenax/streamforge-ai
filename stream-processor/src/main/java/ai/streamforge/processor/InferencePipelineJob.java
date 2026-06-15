package ai.streamforge.processor;

import ai.streamforge.processor.deserialization.UserEventCountDeserializationSchema;
import ai.streamforge.processor.inference.ModelServerAsyncFunction;
import ai.streamforge.processor.model.InferencePrediction;
import ai.streamforge.processor.model.UserEventCount;
import ai.streamforge.processor.serialization.InferencePredictionSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Real-time inference pipeline job.
 *
 * <p>Reads {@link UserEventCount} records produced by {@link CdcUserEventCountJob}
 * from a Kafka topic, calls a model server HTTP endpoint for each record via
 * Flink's async I/O API, and writes the resulting {@link InferencePrediction}
 * records back to Kafka.
 *
 * <pre>
 *   Kafka (user.event.counts)
 *        │
 *        ▼
 *   [Async HTTP] ──► model-server /predict
 *        │
 *        ▼
 *   Kafka (user.event.predictions)
 * </pre>
 *
 * <p>Configuration via environment variables:
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}  — default {@code localhost:9092}</li>
 *   <li>{@code KAFKA_SOURCE_TOPIC}       — default {@code user.event.counts}</li>
 *   <li>{@code KAFKA_SINK_TOPIC}         — default {@code user.event.predictions}</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}     — default {@code flink-inference-pipeline}</li>
 *   <li>{@code MODEL_SERVER_URL}         — default {@code http://localhost:8080/predict}</li>
 *   <li>{@code MODEL_SERVER_TIMEOUT_MS}  — per-request timeout, default {@code 2000}</li>
 *   <li>{@code ASYNC_MAX_CONCURRENT}     — max in-flight async requests, default {@code 100}</li>
 *   <li>{@code ASYNC_TIMEOUT_MS}         — Flink async-I/O timeout, default {@code 5000}</li>
 * </ul>
 */
public class InferencePipelineJob {

    private static final Logger LOG = LoggerFactory.getLogger(InferencePipelineJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrapServers  = env("KAFKA_BOOTSTRAP_SERVERS",  "localhost:9092");
        String sourceTopic       = env("KAFKA_SOURCE_TOPIC",       "user.event.counts");
        String sinkTopic         = env("KAFKA_SINK_TOPIC",         "user.event.predictions");
        String consumerGroup     = env("KAFKA_CONSUMER_GROUP",     "flink-inference-pipeline");
        String modelServerUrl    = env("MODEL_SERVER_URL",         "http://localhost:8080/predict");
        long   requestTimeoutMs  = Long.parseLong(env("MODEL_SERVER_TIMEOUT_MS", "2000"));
        int    maxConcurrent     = Integer.parseInt(env("ASYNC_MAX_CONCURRENT",  "100"));
        long   asyncTimeoutMs    = Long.parseLong(env("ASYNC_TIMEOUT_MS",        "5000"));

        LOG.info("Starting InferencePipelineJob: source={}, sink={}, modelServer={}",
                sourceTopic, sinkTopic, modelServerUrl);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        // ── Source: UserEventCount from Kafka ────────────────────────────────
        KafkaSource<UserEventCount> source = KafkaSource.<UserEventCount>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UserEventCountDeserializationSchema())
                .build();

        DataStream<UserEventCount> counts = env
                .fromSource(source,
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "Kafka: " + sourceTopic);

        // ── Async inference: call model server without blocking Flink threads ─
        DataStream<InferencePrediction> predictions = AsyncDataStream.unorderedWait(
                counts,
                new ModelServerAsyncFunction(modelServerUrl, requestTimeoutMs),
                asyncTimeoutMs,
                TimeUnit.MILLISECONDS,
                maxConcurrent)
                .name("Async Model Server: " + modelServerUrl);

        // ── Sink: InferencePrediction → Kafka ────────────────────────────────
        KafkaSink<InferencePrediction> sink = KafkaSink.<InferencePrediction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<InferencePrediction>builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new InferencePredictionSerializationSchema())
                                .build())
                .build();

        predictions.sinkTo(sink).name("Kafka Sink: " + sinkTopic);

        env.execute("InferencePipelineJob");
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
