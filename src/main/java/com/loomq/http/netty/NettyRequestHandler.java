package com.loomq.http.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Netty HTTP 请求处理器
 *
 * 核心设计：
 * 1. I/O 线程仅处理编解码和请求体提取
 * 2. 请求体立即拷贝到堆内存，ByteBuf 在 I/O 线程释放
 * 3. 业务逻辑在虚拟线程池执行
 * 4. 响应写回由 I/O 线程完成
 * 5. 信号量限流保护
 *
 * 性能优化：
 * - 预序列化静态响应（健康检查、错误响应）
 * - 使用 ObjectWriter 避免重复序列化配置
 *
 * 注意：@Sharable 允许一个实例处理多个 Channel，所有状态都是线程安全的
 */
@ChannelHandler.Sharable
public class NettyRequestHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(NettyRequestHandler.class);

    private final RadixRouter router;
    private final ExecutorService businessExecutor;
    private final Semaphore concurrencyLimit;
    private final ObjectWriter objectWriter;

    // 预序列化的静态响应 - 避免重复序列化
    private static final byte[] NOT_FOUND_RESPONSE = "{\"error\":\"Not Found\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVICE_UNAVAILABLE_RESPONSE = "{\"error\":\"Service Unavailable\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INTERNAL_ERROR_RESPONSE = "{\"error\":\"Internal Server Error\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PAYLOAD_TOO_LARGE_RESPONSE = "{\"error\":\"Payload Too Large\"}".getBytes(StandardCharsets.UTF_8);

    // 预序列化的健康检查响应
    private static final byte[] HEALTH_UP_RESPONSE = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEALTH_LIVE_RESPONSE = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HEALTH_READY_RESPONSE = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);

    // 指标
    private final HttpMetrics metrics;

    public NettyRequestHandler(RadixRouter router, int maxConcurrentRequests, HttpMetrics metrics) {
        this.router = router;
        this.businessExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.concurrencyLimit = new Semaphore(maxConcurrentRequests);
        this.metrics = metrics;

        ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectWriter = objectMapper.writer();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest req)) {
            ReferenceCountUtil.release(msg);
            return;
        }

        try {
            // 1. 在 I/O 线程中安全提取请求体到堆内存
            byte[] bodyBytes = readRequestBodyToHeap(req);

            // 2. 提取请求头到 Map
            Map<String, String> headers = extractHeaders(req);

            // 3. 获取方法和 URI
            HttpMethod method = req.method();
            String uri = req.uri();

            // 查询字符串处理
            String path = extractPath(uri);

            // 4. 背压控制：信号量限流
            if (!concurrencyLimit.tryAcquire()) {
                metrics.recordLimitExceeded();
                writeResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, SERVICE_UNAVAILABLE_RESPONSE);
                return;
            }

            // 5. 提交业务逻辑到虚拟线程
            businessExecutor.execute(() -> {
                long startTime = System.nanoTime();
                try {
                    // 匹配路由
                    RouteMatch match = router.match(method, path);

                    if (match == null) {
                        ctx.executor().execute(() -> {
                            writeResponse(ctx, HttpResponseStatus.NOT_FOUND, NOT_FOUND_RESPONSE);
                            concurrencyLimit.release();
                        });
                        metrics.recordRequest(System.nanoTime() - startTime, 404);
                        return;
                    }

                    // 执行业务处理器
                    Object result = match.handler().handle(method, uri, bodyBytes, headers, match.pathParams());

                    // 6. 将响应写回交给 I/O 线程
                    ctx.executor().execute(() -> {
                        try {
                            // 处理特殊响应类型
                            HttpResponseStatus responseStatus = HttpResponseStatus.OK;
                            Object responseBody = result;

                            if (result instanceof IntentHandler.CreatedResponse created) {
                                responseStatus = HttpResponseStatus.valueOf(created.status());
                                responseBody = created.body();
                            } else if (result instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) result;
                                Object statusObj = map.get("status");
                                if (statusObj instanceof Number) {
                                    int statusCode = ((Number) statusObj).intValue();
                                    responseStatus = HttpResponseStatus.valueOf(statusCode);
                                }
                            }

                            // 零拷贝序列化：DirectSerializedResponse 直接写入 ByteBuf
                            if (responseBody instanceof DirectSerializedResponse directResp) {
                                ByteBuf buf = ctx.alloc().ioBuffer(directResp.estimateSize());
                                try {
                                    directResp.writeTo(buf);
                                    writeByteBufResponse(ctx, responseStatus, buf);
                                    metrics.recordRequest(System.nanoTime() - startTime, responseStatus.code());
                                } catch (Exception e) {
                                    buf.release();
                                    throw e;
                                }
                            } else {
                                // Jackson 回退
                                byte[] bytes = objectWriter.writeValueAsBytes(responseBody);
                                writeResponse(ctx, responseStatus, bytes);
                                metrics.recordRequest(System.nanoTime() - startTime, responseStatus.code());
                            }
                        } catch (Exception e) {
                            logger.error("Failed to serialize response", e);
                            writeResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, INTERNAL_ERROR_RESPONSE);
                            metrics.recordRequest(System.nanoTime() - startTime, 500);
                        } finally {
                            concurrencyLimit.release();
                        }
                    });

                } catch (Exception e) {
                    logger.error("Business handler error", e);
                    ctx.executor().execute(() -> {
                        try {
                            byte[] errorBody = buildErrorBody(e);
                            writeResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, errorBody);
                            metrics.recordRequest(System.nanoTime() - startTime, 500);
                        } finally {
                            concurrencyLimit.release();
                        }
                    });
                }
            });

        } catch (Exception e) {
            logger.error("Request handling error", e);
            writeResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, INTERNAL_ERROR_RESPONSE);
        }
    }

    /**
     * 在 I/O 线程中安全提取请求体到堆内存
     */
    private byte[] readRequestBodyToHeap(FullHttpRequest req) {
        ByteBuf content = req.content();
        if (!content.isReadable()) {
            return new byte[0];
        }
        byte[] body = new byte[content.readableBytes()];
        content.getBytes(content.readerIndex(), body);
        return body;
    }

    /**
     * 提取请求头到 Map
     */
    private Map<String, String> extractHeaders(FullHttpRequest req) {
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, String> entry : req.headers()) {
            headers.put(entry.getKey(), entry.getValue());
        }
        return headers;
    }

    /**
     * 提取路径（去除查询字符串）
     */
    private String extractPath(String uri) {
        int queryIndex = uri.indexOf('?');
        if (queryIndex >= 0) {
            return uri.substring(0, queryIndex);
        }
        return uri;
    }

    /**
     * 写入响应
     */
    private void writeResponse(ChannelHandlerContext ctx, HttpResponseStatus status, byte[] body) {
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            status,
            Unpooled.wrappedBuffer(body)
        );
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        ctx.writeAndFlush(response);
    }

    /**
     * 写入响应（零拷贝，直接使用 ByteBuf）
     */
    private void writeByteBufResponse(ChannelHandlerContext ctx, HttpResponseStatus status, ByteBuf body) {
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            status,
            body
        );
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.readableBytes());
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        ctx.writeAndFlush(response);
    }

    /**
     * 构建错误响应体
     */
    private byte[] buildErrorBody(Exception e) {
        String message = e.getMessage();
        if (message == null) message = "Unknown error";
        message = message.replace("\"", "\\\"").replace("\n", "\\n");
        return ("{\"error\":\"" + message + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Channel exception", cause);
        ctx.close();
    }

    /**
     * 关闭处理器
     */
    public void shutdown() {
        businessExecutor.shutdown();
        try {
            if (!businessExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                businessExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            businessExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public int getAvailablePermits() {
        return concurrencyLimit.availablePermits();
    }
}
