package com.loomq.http.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import com.loomq.http.json.JsonCodec;
import com.loomq.metrics.LoomQMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
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
    private final JsonCodec jsonCodec;
    private static final byte[] EMPTY_BODY = new byte[0];

    // 预序列化的静态响应 - 避免重复序列化
    private static final byte[] NOT_FOUND_RESPONSE = "{\"error\":\"Not Found\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERVICE_UNAVAILABLE_RESPONSE = "{\"error\":\"Service Unavailable\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INTERNAL_ERROR_RESPONSE = "{\"error\":\"Internal Server Error\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PAYLOAD_TOO_LARGE_RESPONSE = "{\"error\":\"Payload Too Large\"}".getBytes(StandardCharsets.UTF_8);

    // 指标
    private final HttpMetrics metrics;

    public NettyRequestHandler(RadixRouter router, int maxConcurrentRequests, HttpMetrics metrics) {
        this.router = router;
        this.businessExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.concurrencyLimit = new Semaphore(maxConcurrentRequests);
        this.metrics = metrics;

        this.jsonCodec = JsonCodec.instance();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest req)) {
            ReferenceCountUtil.release(msg);
            return;
        }

        boolean permitAcquired = false;
        try {
            // 1. 获取方法和 URI
            HttpMethod method = req.method();
            String uri = req.uri();

            // 2. 背压控制：信号量限流
            if (!concurrencyLimit.tryAcquire()) {
                metrics.recordLimitExceeded();
                writeResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, SERVICE_UNAVAILABLE_RESPONSE);
                return;
            }
            permitAcquired = true;

            // 3. 先做路由匹配，404 直接返回，避免无谓的 body/header 拷贝
            RouteMatch match = router.match(method, uri);
            if (match == null) {
                ctx.executor().execute(() -> {
                    writeResponse(ctx, HttpResponseStatus.NOT_FOUND, NOT_FOUND_RESPONSE);
                    concurrencyLimit.release();
                });
                metrics.recordRequest(0L, 404);
                return;
            }

            // 4. 仅在命中路由后，再把请求体和请求头复制到堆内存
            byte[] bodyBytes = readRequestBodyToHeap(req);
            Map<String, String> headers = Collections.emptyMap();

            // 5. 提交业务逻辑到虚拟线程
            businessExecutor.execute(() -> {
                long startTime = System.nanoTime();
                try {
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
                            } else if (result instanceof HttpErrorResponse errorResp) {
                                responseStatus = HttpResponseStatus.valueOf(errorResp.status());
                                responseBody = errorResp;
                            } else if (result instanceof LoomQMetrics.MetricsSnapshot snapshot) {
                                responseBody = snapshot;
                            } else if (result instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> map = (Map<String, Object>) result;
                                Object statusObj = map.get("status");
                                if (statusObj instanceof Number) {
                                    int statusCode = ((Number) statusObj).intValue();
                                    responseStatus = HttpResponseStatus.valueOf(statusCode);
                                }
                            }

                            // 原始字节响应可直接写回，避免二次 JSON 编码
                            if (responseBody instanceof byte[] rawBytes) {
                                writeResponse(ctx, responseStatus, rawBytes);
                                metrics.recordRequest(System.nanoTime() - startTime, responseStatus.code());
                            // /metrics 快路径：直接把 snapshot 写入 ByteBuf
                            } else if (responseBody instanceof LoomQMetrics.MetricsSnapshot snapshot) {
                                ByteBuf buf = ctx.alloc().ioBuffer(MetricsResponseSerializer.estimateSize(snapshot));
                                try {
                                    MetricsResponseSerializer.write(snapshot, buf);
                                    writeByteBufResponse(ctx, responseStatus, buf);
                                    metrics.recordRequest(System.nanoTime() - startTime, responseStatus.code());
                                } catch (Exception e) {
                                    buf.release();
                                    throw e;
                                }
                            // 零拷贝序列化：DirectSerializedResponse 直接写入 ByteBuf
                            } else if (responseBody instanceof DirectSerializedResponse directResp) {
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
                                byte[] bytes = jsonCodec.writeBytes(responseBody);
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
            if (permitAcquired) {
                concurrencyLimit.release();
            }
        }
    }

    /**
     * 在 I/O 线程中安全提取请求体到堆内存
     */
    private byte[] readRequestBodyToHeap(FullHttpRequest req) {
        ByteBuf content = req.content();
        if (!content.isReadable()) {
            return EMPTY_BODY;
        }
        return ByteBufUtil.getBytes(content, content.readerIndex(), content.readableBytes(), false);
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
