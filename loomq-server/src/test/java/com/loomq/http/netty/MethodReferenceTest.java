package com.loomq.http.netty;

import com.loomq.store.IntentStore;
import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 比较方法引用和 lambda
 */
class MethodReferenceTest {

    @Test
    void testWithLambda() {
        RadixRouter router = new RadixRouter();
        IntentStore store = new IntentStore();

        router.add(HttpMethod.POST, "/v1/intents",
            (method, uri, body, headers, pathParams) -> Map.of("endpoint", "create"));
        router.add(HttpMethod.GET, "/v1/intents/{intentId}",
            (method, uri, body, headers, pathParams) -> {
                String id = pathParams.get("intentId");
                return Map.of("endpoint", "get", "id", id);
            });

        assertNotNull(router.match(HttpMethod.POST, "/v1/intents"));
        assertNotNull(router.match(HttpMethod.GET, "/v1/intents/test-id"));
    }

    @Test
    void testWithMethodReference() {
        RadixRouter router = new RadixRouter();
        IntentStore store = new IntentStore();

        // 创建一个简单的处理器对象
        TestHandler handler = new TestHandler(store);

        router.add(HttpMethod.POST, "/v1/intents", handler::create);
        router.add(HttpMethod.GET, "/v1/intents/{intentId}", handler::get);

        assertNotNull(router.match(HttpMethod.POST, "/v1/intents"));
        assertNotNull(router.match(HttpMethod.GET, "/v1/intents/test-id"));
    }

    static class TestHandler {
        private final IntentStore store;

        TestHandler(IntentStore store) {
            this.store = store;
        }

        Object create(HttpMethod method, String uri, byte[] body, Map<String, String> headers,
                      Map<String, String> pathParams) {
            return Map.of("endpoint", "create");
        }

        Object get(HttpMethod method, String uri, byte[] body, Map<String, String> headers,
                   Map<String, String> pathParams) {
            String id = pathParams.get("intentId");
            return Map.of("endpoint", "get", "id", id);
        }
    }
}
