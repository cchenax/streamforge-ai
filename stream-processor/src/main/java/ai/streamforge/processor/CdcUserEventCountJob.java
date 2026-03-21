package ai.streamforge.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Properties;

public class CdcUserEventCountJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap.servers", "localhost:9092");
        String inputTopic = params.get("input.topic", "streamforge.streamforge.customers");
        String outputTopic = params.get("output.topic", "streamforge.features.user_event_counts");
        String groupId = params.get("group.id", "streamforge-flink-user-count");
        long windowSeconds = params.getLong("window.seconds", 60L);
        String startupMode = params.get("startup.mode", "earliest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(startupMode.equalsIgnoreCase("latest")
                        ? OffsetsInitializer.latest()
                        : OffsetsInitializer.earliest())
                .build();

        DataStream<String> rawEvents = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-cdc-source"
        );

        DataStream<CdcUserEvent> parsedEvents = rawEvents
                .process(new ParseDebeziumEvent())
                .name("parse-debezium-cdc-events");

        DataStream<String> featureOutput = parsedEvents
                .keyBy(CdcUserEvent::userId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
                .process(new UserCountWindowAggregator())
                .name("count-events-per-user-window");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(buildAcksProducerProperties())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        featureOutput.sinkTo(sink).name("kafka-feature-sink");
        featureOutput.print("processed-feature");

        env.execute("StreamForge CDC User Event Count Job");
    }

    private static Properties buildAcksProducerProperties() {
        Properties props = new Properties();
        props.setProperty("acks", "all");
        return props;
    }

    private record CdcUserEvent(String userId, String op, long sourceTsMs) {
    }

    private static class ParseDebeziumEvent extends ProcessFunction<String, CdcUserEvent> {
        @Override
        public void processElement(String value, Context ctx, Collector<CdcUserEvent> out) throws Exception {
            JsonNode root = MAPPER.readTree(value);
            JsonNode payload = root.path("payload");
            if (payload.isMissingNode() || payload.isNull()) {
                return;
            }

            String op = payload.path("op").asText("");
            if (op.isEmpty() || "d".equals(op)) {
                // For this feature job we count create/update/read events only.
                return;
            }

            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull()) {
                return;
            }

            // Try common user identifier fields first, then fallback to id.
            String userId = firstNonEmpty(
                    textOrEmpty(after, "user_id"),
                    textOrEmpty(after, "uid"),
                    textOrEmpty(after, "id")
            );
            if (userId.isEmpty()) {
                return;
            }

            long tsMs = payload.path("ts_ms").asLong(0L);
            out.collect(new CdcUserEvent(userId, op, tsMs));
        }

        private String textOrEmpty(JsonNode node, String fieldName) {
            JsonNode field = node.path(fieldName);
            return field.isMissingNode() || field.isNull() ? "" : field.asText("");
        }

        private String firstNonEmpty(String... values) {
            for (String v : values) {
                if (v != null && !v.isEmpty()) {
                    return v;
                }
            }
            return "";
        }
    }

    private static class UserCountWindowAggregator
            extends ProcessWindowFunction<CdcUserEvent, String, String, TimeWindow> {

        @Override
        public void process(
                String userId,
                Context context,
                Iterable<CdcUserEvent> events,
                Collector<String> out
        ) {
            long count = 0L;
            String lastOp = "unknown";
            long lastSourceTsMs = 0L;

            for (CdcUserEvent event : events) {
                count++;
                lastOp = event.op();
                lastSourceTsMs = Math.max(lastSourceTsMs, event.sourceTsMs());
            }

            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();

            String output = String.format(
                    "{\"feature\":\"user_event_count\",\"user_id\":\"%s\",\"window_start\":\"%s\",\"window_end\":\"%s\",\"event_count\":%d,\"last_op\":\"%s\",\"last_source_ts_ms\":%d}",
                    escapeJson(userId),
                    Instant.ofEpochMilli(windowStart),
                    Instant.ofEpochMilli(windowEnd),
                    count,
                    escapeJson(lastOp),
                    lastSourceTsMs
            );
            out.collect(output);
        }

        private String escapeJson(String raw) {
            return raw.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
