package com.loomq.http.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.loomq.config.SecurityConfig;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class NettyRequestHandlerTest {

    @Test
    void shouldForwardHttpHeadersToRouteHandler() throws Exception {
        RadixRouter router = new RadixRouter();
        router.add(HttpMethod.GET, "/headers", (method, uri, body, headers, pathParams) ->
            Map.of("expectedRevision", headers.get("X-LoomQ-Expected-Revision")));

        EmbeddedChannel channel = new EmbeddedChannel(
            new NettyRequestHandler(router, 1, 100, HttpMetrics.getInstance()));
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/headers",
                Unpooled.EMPTY_BUFFER
            );
            request.headers().set("X-LoomQ-Expected-Revision", "42");

            channel.writeInbound(request);
            FullHttpResponse response = awaitResponse(channel);

            assertEquals(HttpResponseStatus.OK, response.status());
            String body = response.content().toString(StandardCharsets.UTF_8);
            assertTrue(body.contains("\"expectedRevision\":\"42\""));
            response.release();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldRejectProtectedRoutesWhenSecurityTokenIsMissing() throws Exception {
        AtomicBoolean invoked = new AtomicBoolean(false);
        RadixRouter router = new RadixRouter();
        router.add(HttpMethod.GET, "/v1/intents/{intentId}", (method, uri, body, headers, pathParams) -> {
            invoked.set(true);
            return Map.of("ok", true);
        });

        EmbeddedChannel channel = new EmbeddedChannel(
            new NettyRequestHandler(
                router,
                1,
                100,
                HttpMetrics.getInstance(),
                new SecurityConfig(true, "X-LoomQ-Token", Set.of("secret-token"))));
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/v1/intents/abc",
                Unpooled.EMPTY_BUFFER
            );

            channel.writeInbound(request);
            FullHttpResponse response = awaitResponse(channel);

            assertEquals(HttpResponseStatus.UNAUTHORIZED, response.status());
            assertTrue(response.headers().contains("WWW-Authenticate"));
            assertFalse(invoked.get(), "unauthorized request must not reach route handler");
            response.release();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldAcceptBearerTokenForProtectedRoutes() throws Exception {
        RadixRouter router = new RadixRouter();
        router.add(HttpMethod.GET, "/metrics", (method, uri, body, headers, pathParams) ->
            Map.of("ok", true));

        EmbeddedChannel channel = new EmbeddedChannel(
            new NettyRequestHandler(
                router,
                1,
                100,
                HttpMetrics.getInstance(),
                new SecurityConfig(true, "X-LoomQ-Token", Set.of("secret-token"))));
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/metrics",
                Unpooled.EMPTY_BUFFER
            );
            request.headers().set("X-LoomQ-Token", "Bearer secret-token");

            channel.writeInbound(request);
            FullHttpResponse response = awaitResponse(channel);

            assertEquals(HttpResponseStatus.OK, response.status());
            assertTrue(response.content().toString(StandardCharsets.UTF_8).contains("\"ok\":true"));
            response.release();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldKeepHealthRoutesOpenWhenSecurityIsEnabled() throws Exception {
        RadixRouter router = new RadixRouter();
        router.add(HttpMethod.GET, "/health/ready", (method, uri, body, headers, pathParams) ->
            Map.of("ready", true));

        EmbeddedChannel channel = new EmbeddedChannel(
            new NettyRequestHandler(
                router,
                1,
                100,
                HttpMetrics.getInstance(),
                new SecurityConfig(true, "X-LoomQ-Token", Set.of("secret-token"))));
        try {
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/health/ready",
                Unpooled.EMPTY_BUFFER
            );

            channel.writeInbound(request);
            FullHttpResponse response = awaitResponse(channel);

            assertEquals(HttpResponseStatus.OK, response.status());
            assertTrue(response.content().toString(StandardCharsets.UTF_8).contains("\"ready\":true"));
            response.release();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static FullHttpResponse awaitResponse(EmbeddedChannel channel) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            channel.runPendingTasks();
            Object outbound = channel.readOutbound();
            if (outbound instanceof FullHttpResponse response) {
                return response;
            }
            Thread.sleep(10);
        }
        return fail("expected outbound HTTP response");
    }
}
