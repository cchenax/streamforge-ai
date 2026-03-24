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
            if (value == null) {
                return;
            }

            String trimmed = value.trim();
            if (trimmed.isEmpty() || "null".equals(trimmed)) {
                return;
            }

            JsonNode root = MAPPER.readTree(trimmed);
            JsonNode payload = root.path("payload");
            if (payload.isMissingNode() || payload.isNull()) {
                return;
            }

            // Debezium schema evolution messages can have a different envelope shape and may not
            // include an "after" struct. For this feature job we only care about row-level changes.
            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull()) {
                return;
            }

            String op = firstNonEmpty(
                    textOrEmpty(payload, "op"),
                    textOrEmpty(root, "op")
            );

            // For this feature job we count create/update/read events only.
            if (!("c".equals(op) || "u".equals(op) || "r".equals(op))) {
                return;
            }

            String userId = extractUserId(after);
            if (userId.isEmpty()) {
                return;
            }

            // ts_ms can be relocated across envelope versions; keep parsing tolerant.
            long tsMs = parseTsMs(
                    payload.path("ts_ms"),
                    root.path("ts_ms"),
                    payload.path("source").path("ts_ms")
            );

            out.collect(new CdcUserEvent(userId, op, tsMs));
        }

        private static long parseTsMs(JsonNode... candidates) {
            for (JsonNode n : candidates) {
                if (n == null || n.isMissingNode() || n.isNull()) {
                    continue;
                }
                if (n.isNumber()) {
                    return n.asLong(0L);
                }
                if (n.isTextual()) {
                    String s = n.asText("");
                    if (s == null || s.isBlank()) {
                        continue;
                    }
                    try {
                        return Long.parseLong(s);
                    } catch (NumberFormatException ignored) {
                        // Keep trying next candidate.
                    }
                }
            }
            return 0L;
        }

        private String extractUserId(JsonNode after) {
            // Try common user identifier fields first (field additions won't break this).
            String fromDirectFields = firstNonEmpty(
                    textOrEmpty(after, "user_id"),
                    textOrEmpty(after, "uid"),
                    textOrEmpty(after, "id"),
                    textOrEmpty(after, "userId"),
                    textOrEmpty(after, "USER_ID"),
                    textOrEmpty(after, "userID")
            );
            if (!fromDirectFields.isEmpty()) {
                return fromDirectFields;
            }

            // Example: if schema evolution nests identifiers under a "user" object.
            JsonNode userNode = after.path("user");
            if (!userNode.isMissingNode() && !userNode.isNull()) {
                String fromUserNode = firstNonEmpty(
                        textOrEmpty(userNode, "user_id"),
                        textOrEmpty(userNode, "uid"),
                        textOrEmpty(userNode, "id")
                );
                if (!fromUserNode.isEmpty()) {
                    return fromUserNode;
                }
            }

            // Last-resort heuristic: pick the first *_id field at the top-level.
            // This keeps the pipeline running when upstream renamed the identifier field.
            if (after.isObject()) {
                var it = after.fieldNames();
                while (it.hasNext()) {
                    String fieldName = it.next();
                    if (fieldName == null) {
                        continue;
                    }
                    if (fieldName.endsWith("_id")) {
                        JsonNode v = after.path(fieldName);
                        if (v != null && !v.isMissingNode() && !v.isNull()) {
                            String candidate = v.asText("");
                            if (candidate != null && !candidate.isEmpty()) {
                                return candidate;
                            }
                        }
                    }
                }
            }

            return "";
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
