package com.loomq.http.netty;

import io.netty.handler.codec.http.HttpMethod;

import java.util.Map;

/**
 * 请求处理器函数式接口
 *
 * 所有 API 端点实现此接口
 */
@FunctionalInterface
public interface Handler {

    /**
     * 处理 HTTP 请求
     *
     * @param method HTTP 方法
     * @param uri 请求 URI
     * @param body 请求体（已拷贝到堆内存）
     * @param headers 请求头
     * @param pathParams 路径参数（如 {intentId}）
     * @return 响应对象（将被序列化为 JSON）
     * @throws Exception 处理异常
     */
    Object handle(HttpMethod method, String uri, byte[] body, Map<String, String> headers,
                  Map<String, String> pathParams) throws Exception;
}
