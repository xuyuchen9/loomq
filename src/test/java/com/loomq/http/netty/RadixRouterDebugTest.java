package com.loomq.http.netty;

import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * 调试路由器
 */
class RadixRouterDebugTest {

    @Test
    void debugRouteRegistration() {
        RadixRouter router = new RadixRouter();

        System.out.println("=== Registering POST /v1/intents ===");
        router.add(HttpMethod.POST, "/v1/intents",
            (method, uri, body, headers, pathParams) -> Map.of("endpoint", "create"));

        System.out.println("\n=== Testing after POST registration ===");
        RouteMatch postMatch = router.match(HttpMethod.POST, "/v1/intents");
        System.out.println("POST /v1/intents match: " + (postMatch != null));

        RouteMatch getMatch1 = router.match(HttpMethod.GET, "/v1/intents/test-id");
        System.out.println("GET /v1/intents/test-id match: " + (getMatch1 != null) + " (expected: false)");

        System.out.println("\n=== Registering GET /v1/intents/{intentId} ===");
        router.add(HttpMethod.GET, "/v1/intents/{intentId}",
            (method, uri, body, headers, pathParams) -> Map.of("endpoint", "get"));

        System.out.println("\n=== Testing after GET registration ===");
        RouteMatch postMatch2 = router.match(HttpMethod.POST, "/v1/intents");
        System.out.println("POST /v1/intents match: " + (postMatch2 != null) + " (expected: true)");

        RouteMatch getMatch2 = router.match(HttpMethod.GET, "/v1/intents/test-id");
        System.out.println("GET /v1/intents/test-id match: " + (getMatch2 != null) + " (expected: true)");
        if (getMatch2 != null) {
            System.out.println("  pathParams: " + getMatch2.pathParams());
        }
    }
}
