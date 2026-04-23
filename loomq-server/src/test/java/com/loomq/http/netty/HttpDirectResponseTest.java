package com.loomq.http.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loomq.api.ErrorResponse;
import com.loomq.api.IntentActionResponse;
import com.loomq.common.MetricsCollector;
import com.loomq.metrics.LoomQMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HttpDirectResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("HttpErrorResponse should match Jackson JSON")
    void httpErrorResponseMatchesJackson() throws Exception {
        HttpErrorResponse response = new HttpErrorResponse(
            422,
            ErrorResponse.of("42201", "Invalid payload", Map.of("field", "intentId"))
        );

        String directJson = new String(HttpErrorResponseSerializer.toBytes(response), StandardCharsets.UTF_8);
        String jacksonJson = objectMapper.writeValueAsString(response);

        assertEquals(
            objectMapper.readValue(jacksonJson, Map.class),
            objectMapper.readValue(directJson, Map.class)
        );
    }

    @Test
    @DisplayName("/metrics response should match Jackson JSON")
    void metricsResponseMatchesJackson() throws Exception {
        LoomQMetrics metrics = LoomQMetrics.getInstance();
        MetricsCollector collector = MetricsCollector.getInstance();
        metrics.reset();
        try {
            metrics.incrementIntentsCreated();
            metrics.incrementIntentsCancelled();
            metrics.updateActiveDispatches(2);
            metrics.updateWalHealth(true);
            metrics.updateWalLastFlushTime(1713582000000L);
            metrics.updateWalPendingWrites(3);
            metrics.updateWalRingBufferSize(1024);
            collector.updatePendingIntents(7);
            collector.updateIntentStatus("SCHEDULED", 4);
            collector.updateIntentStatus("DUE", 1);

            LoomQMetrics.MetricsSnapshot snapshot = metrics.snapshot();
            String directJson = new String(MetricsResponseSerializer.toBytes(snapshot), StandardCharsets.UTF_8);
            String jacksonJson = objectMapper.writeValueAsString(snapshot);

            assertEquals(
                objectMapper.readValue(jacksonJson, Map.class),
                objectMapper.readValue(directJson, Map.class)
            );
        } finally {
            metrics.reset();
        }
    }

    @Test
    @DisplayName("IntentActionResponse should match Jackson JSON")
    void intentActionResponseMatchesJackson() throws Exception {
        IntentActionResponse response = new IntentActionResponse("intent-123", "DISPATCHING");

        String directJson = new String(IntentActionResponseSerializer.toBytes(response), StandardCharsets.UTF_8);
        String jacksonJson = objectMapper.writeValueAsString(response);

        assertEquals(
            objectMapper.readValue(jacksonJson, Map.class),
            objectMapper.readValue(directJson, Map.class)
        );
    }
}
