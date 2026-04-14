package com.loomq.http.netty;

import java.util.Map;

/**
 * 路由匹配结果
 *
 * @param handler 匹配的处理器
 * @param pathParams 路径参数映射
 */
public class RouteMatch {
    private final Handler handler;
    private final Map<String, String> pathParams;

    public RouteMatch(Handler handler, Map<String, String> pathParams) {
        this.handler = handler;
        this.pathParams = pathParams != null ? pathParams : Map.of();
    }

    public Handler handler() {
        return handler;
    }

    public Map<String, String> pathParams() {
        return pathParams;
    }

    public boolean isFound() {
        return handler != null;
    }
}
