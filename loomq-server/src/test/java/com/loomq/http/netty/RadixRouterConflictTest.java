package com.loomq.http.netty;

import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 测试路由冲突场景
 */
class RadixRouterConflictTest {

    @Test
    void testPostAndGetWithParamConflict() {
        RadixRouter router = new RadixRouter();

        // 注册与 LoomQ 相同的路由
        router.add(HttpMethod.POST, "/v1/intents",
            (method, uri, body, headers, pathParams) -> Map.of("endpoint", "create"));
        router.add(HttpMethod.GET, "/v1/intents/{intentId}",
            (method, uri, body, headers, pathParams) -> Map.of("endpoint", "get", "id", pathParams.get("intentId")));

        // 测试 POST
        RouteMatch postMatch = router.match(HttpMethod.POST, "/v1/intents");
        assertNotNull(postMatch, "POST /v1/intents should match");

        // 测试 GET with param
        RouteMatch getMatch = router.match(HttpMethod.GET, "/v1/intents/test-id-123");
        assertNotNull(getMatch, "GET /v1/intents/test-id should match");
        assertEquals("test-id-123", getMatch.pathParams().get("intentId"));
    }
}
