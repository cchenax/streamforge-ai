package ai.streamforge.processor.inference;

import ai.streamforge.processor.model.InferencePrediction;
import ai.streamforge.processor.model.UserEventCount;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Flink async function that calls a model server HTTP endpoint for each
 * {@link UserEventCount} record and emits an {@link InferencePrediction}.
 *
 * <p>Uses Java 11 {@link HttpClient} with non-blocking async dispatch so Flink
 * task threads are never blocked waiting for the remote call.
 *
 * <p>On HTTP error or timeout the record is emitted with label="error" and
 * confidence=0.0 so the pipeline never stalls.
 *
 * <p>Configuration via constructor parameters (environment-variable wiring is
 * done in {@link ai.streamforge.processor.InferencePipelineJob}):
 * <ul>
 *   <li>{@code modelServerUrl} — e.g. {@code http://model-server:8080/predict}</li>
 *   <li>{@code timeoutMs}      — per-request timeout, default 2000 ms</li>
 * </ul>
 */
public class ModelServerAsyncFunction
        extends RichAsyncFunction<UserEventCount, InferencePrediction> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ModelServerAsyncFunction.class);

    private final String modelServerUrl;
    private final long   timeoutMs;

    private transient HttpClient   httpClient;
    private transient ObjectMapper objectMapper;
    private transient Counter      requestsTotal;
    private transient Counter      errorsTotal;

    public ModelServerAsyncFunction(String modelServerUrl, long timeoutMs) {
        this.modelServerUrl = modelServerUrl;
        this.timeoutMs      = timeoutMs;
    }

    @Override
    public void open(Configuration parameters) {
        httpClient   = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
        objectMapper = new ObjectMapper();

        MetricGroup mg = getRuntimeContext().getMetricGroup().addGroup("model_server");
        requestsTotal  = mg.counter("requests_total");
        errorsTotal    = mg.counter("errors_total");
    }

    @Override
    public void asyncInvoke(UserEventCount input, ResultFuture<InferencePrediction> resultFuture) {
        long startNs = System.nanoTime();
        requestsTotal.inc();

        ObjectNode body = objectMapper.createObjectNode()
                .put("userId",         input.userId)
                .put("count",          input.count)
                .put("windowStartMs",  input.windowStartMs)
                .put("windowEndMs",    input.windowEndMs);

        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(URI.create(modelServerUrl))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)))
                    .build();
        } catch (Exception e) {
            errorsTotal.inc();
            resultFuture.complete(Collections.singleton(errorPrediction(input, 0)));
            return;
        }

        CompletableFuture<HttpResponse<String>> future =
                httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());

        future.whenComplete((response, ex) -> {
            long latencyMs = (System.nanoTime() - startNs) / 1_000_000;
            if (ex != null || response.statusCode() != 200) {
                errorsTotal.inc();
                LOG.warn("Model server error (latency={}ms, ex={})", latencyMs,
                        ex != null ? ex.getMessage() : "HTTP " + response.statusCode());
                resultFuture.complete(Collections.singleton(errorPrediction(input, latencyMs)));
                return;
            }
            try {
                JsonNode json        = objectMapper.readTree(response.body());
                String   label       = json.path("label").asText("unknown");
                double   confidence  = json.path("confidence").asDouble(0.0);
                resultFuture.complete(Collections.singleton(
                        new InferencePrediction(
                                input.userId,
                                input.windowStartMs,
                                input.windowEndMs,
                                input.count,
                                label,
                                confidence,
                                latencyMs)));
            } catch (Exception parseEx) {
                errorsTotal.inc();
                LOG.warn("Failed to parse model server response: {}", parseEx.getMessage());
                resultFuture.complete(Collections.singleton(errorPrediction(input, latencyMs)));
            }
        });
    }

    @Override
    public void timeout(UserEventCount input, ResultFuture<InferencePrediction> resultFuture) {
        errorsTotal.inc();
        LOG.warn("Model server timed out for userId={}", input.userId);
        resultFuture.complete(Collections.singleton(errorPrediction(input, timeoutMs)));
    }

    private InferencePrediction errorPrediction(UserEventCount input, long latencyMs) {
        return new InferencePrediction(
                input.userId, input.windowStartMs, input.windowEndMs,
                input.count, "error", 0.0, latencyMs);
    }
}
