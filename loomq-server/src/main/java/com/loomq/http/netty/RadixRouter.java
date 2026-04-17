package com.loomq.http.netty;

import io.netty.handler.codec.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;

/**
 * Radix Tree 路由器
 *
 * 特性：
 * - O(k) 路径匹配（k = 路径长度）
 * - 支持路径参数 {param}
 *
 * 性能：百万级路由/秒
 */
public class RadixRouter {

    private final RouteNode root = new RouteNode();

    /**
     * 添加路由
     *
     * @param method HTTP 方法
     * @param path 路径模式（支持 {param} 参数）
     * @param handler 处理器
     */
    public void add(HttpMethod method, String path, Handler handler) {
        String[] segments = splitPath(path);
        String key = method.name();
        root.insert(segments, 0, key, handler, path);
    }

    /**
     * 匹配路由
     *
     * @param method HTTP 方法
     * @param path 请求路径
     * @return 匹配结果，包含处理器和路径参数
     */
    public RouteMatch match(HttpMethod method, String path) {
        String[] segments = splitPath(path);
        Map<String, String> params = new HashMap<>();

        Handler handler = root.match(segments, 0, method.name(), params);
        if (handler != null) {
            return new RouteMatch(handler, params);
        }
        return null;
    }

    private String[] splitPath(String path) {
        if (path == null || path.isEmpty()) {
            return new String[0];
        }
        path = path.startsWith("/") ? path.substring(1) : path;
        path = path.endsWith("/") && path.length() > 1 ? path.substring(0, path.length() - 1) : path;
        return path.isEmpty() ? new String[0] : path.split("/");
    }

    /**
     * 路由节点
     */
    private static class RouteNode {
        private final Map<String, RouteNode> staticChildren = new HashMap<>();
        private RouteNode paramChild; // 参数子节点
        private String paramName; // 参数名
        private final Map<String, Handler> handlers = new HashMap<>(); // 方法 -> 处理器映射

        void insert(String[] segments, int index, String method, Handler handler, String originalPath) {
            if (index >= segments.length) {
                // 到达终点，设置处理器
                this.handlers.put(method, handler);
                return;
            }

            String segment = segments[index];

            if (segment.startsWith("{") && segment.endsWith("}")) {
                // 参数节点
                String paramName = segment.substring(1, segment.length() - 1);

                if (paramChild == null) {
                    paramChild = new RouteNode();
                    this.paramName = paramName;
                }

                // 更新参数名（可能多个路径共享同一参数节点）
                this.paramName = paramName;

                paramChild.insert(segments, index + 1, method, handler, originalPath);
            } else {
                // 静态节点
                RouteNode child = staticChildren.computeIfAbsent(segment, k -> new RouteNode());
                child.insert(segments, index + 1, method, handler, originalPath);
            }
        }

        Handler match(String[] segments, int index, String method, Map<String, String> params) {
            if (index >= segments.length) {
                // 到达终点，检查方法是否匹配
                return handlers.get(method);
            }

            String segment = segments[index];

            // 优先匹配静态路径
            RouteNode staticChild = staticChildren.get(segment);
            if (staticChild != null) {
                Handler result = staticChild.match(segments, index + 1, method, params);
                if (result != null) {
                    return result;
                }
            }

            // 尝试参数匹配
            if (paramChild != null) {
                params.put(paramName, segment);
                Handler result = paramChild.match(segments, index + 1, method, params);
                if (result != null) {
                    return result;
                }
                params.remove(paramName);
            }

            return null;
        }
    }
}
