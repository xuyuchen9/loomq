package com.loomq.http.netty;

import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RadixRouter 单元测试
 */
class RadixRouterTest {

    @Test
    void testSimplePath() {
        RadixRouter router = new RadixRouter();

        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) -> Map.of("status", "UP"));

        RouteMatch match = router.match(HttpMethod.GET, "/health");
        assertNotNull(match, "Should match /health");
        assertTrue(match.pathParams().isEmpty());
    }

    @Test
    void testPathWithParam() {
        RadixRouter router = new RadixRouter();

        router.add(HttpMethod.GET, "/v1/intents/{intentId}",
            (method, uri, body, headers, pathParams) -> Map.of("id", pathParams.get("intentId")));

        RouteMatch match = router.match(HttpMethod.GET, "/v1/intents/abc123");
        assertNotNull(match, "Should match /v1/intents/abc123");
        assertEquals("abc123", match.pathParams().get("intentId"));
    }

    @Test
    void testPostPath() {
        RadixRouter router = new RadixRouter();

        router.add(HttpMethod.POST, "/v1/intents",
            (method, uri, body, headers, pathParams) -> Map.of("created", true));

        RouteMatch match = router.match(HttpMethod.POST, "/v1/intents");
        assertNotNull(match, "Should match POST /v1/intents");
    }

    @Test
    void testNotFound() {
        RadixRouter router = new RadixRouter();

        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) -> Map.of("status", "UP"));

        RouteMatch match = router.match(HttpMethod.GET, "/unknown");
        assertNull(match, "Should not match /unknown");
    }

    @Test
    void testMethodMismatch() {
        RadixRouter router = new RadixRouter();

        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) -> Map.of("status", "UP"));

        RouteMatch match = router.match(HttpMethod.POST, "/health");
        assertNull(match, "Should not match POST /health");
    }

    @Test
    void testMultipleRoutes() {
        RadixRouter router = new RadixRouter();

        router.add(HttpMethod.GET, "/health", (method, uri, body, headers, pathParams) -> Map.of("status", "UP"));
        router.add(HttpMethod.POST, "/v1/intents", (method, uri, body, headers, pathParams) -> Map.of("created", true));
        router.add(HttpMethod.GET, "/v1/intents/{intentId}",
            (method, uri, body, headers, pathParams) -> Map.of("id", pathParams.get("intentId")));
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/cancel",
            (method, uri, body, headers, pathParams) -> Map.of("cancelled", pathParams.get("intentId")));

        assertNotNull(router.match(HttpMethod.GET, "/health"));
        assertNotNull(router.match(HttpMethod.POST, "/v1/intents"));
        assertNotNull(router.match(HttpMethod.GET, "/v1/intents/abc"));
        assertNotNull(router.match(HttpMethod.POST, "/v1/intents/abc/cancel"));
        assertNull(router.match(HttpMethod.DELETE, "/v1/intents"));
    }
}
