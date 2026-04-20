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
     * @param uri 请求 URI
     * @return 匹配结果，包含处理器和路径参数
     */
    public RouteMatch match(HttpMethod method, String uri) {
        if (uri == null || uri.isEmpty()) {
            return root.match(uri, 0, 0, method.name(), null, new SegmentKey());
        }

        int queryIndex = uri.indexOf('?');
        int start = 0;
        int end = queryIndex >= 0 ? queryIndex : uri.length();

        while (start < end && uri.charAt(start) == '/') {
            start++;
        }
        while (end > start && uri.charAt(end - 1) == '/') {
            end--;
        }

        if (start >= end) {
            return root.match(uri, 0, 0, method.name(), null, new SegmentKey());
        }

        return root.match(uri, start, end, method.name(), null, new SegmentKey());
    }

    private String[] splitPath(String path) {
        if (path == null || path.isEmpty()) {
            return new String[0];
        }

        int start = 0;
        int end = path.length();

        while (start < end && path.charAt(start) == '/') {
            start++;
        }
        while (end > start && path.charAt(end - 1) == '/') {
            end--;
        }

        if (start >= end) {
            return new String[0];
        }

        int segmentCount = 1;
        for (int i = start; i < end; i++) {
            if (path.charAt(i) == '/') {
                segmentCount++;
            }
        }

        String[] segments = new String[segmentCount];
        int segmentIndex = 0;
        int segmentStart = start;
        for (int i = start; i < end; i++) {
            if (path.charAt(i) == '/') {
                segments[segmentIndex++] = path.substring(segmentStart, i);
                segmentStart = i + 1;
            }
        }
        segments[segmentIndex] = path.substring(segmentStart, end);
        return segments;
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

        RouteMatch match(String path, int index, int end, String method, Map<String, String> params, SegmentKey segmentKey) {
            if (index >= end) {
                // 到达终点，检查方法是否匹配
                Handler handler = handlers.get(method);
                if (handler == null) {
                    return null;
                }
                return new RouteMatch(handler, params);
            }

            int segmentEnd = index;
            while (segmentEnd < end && path.charAt(segmentEnd) != '/') {
                segmentEnd++;
            }

            // 优先匹配静态路径
            segmentKey.reset(path, index, segmentEnd);
            RouteNode staticChild = staticChildren.get(segmentKey);
            if (staticChild != null) {
                int nextIndex = segmentEnd < end ? segmentEnd + 1 : end;
                RouteMatch result = staticChild.match(path, nextIndex, end, method, params, segmentKey);
                if (result != null) {
                    return result;
                }
            }

            // 尝试参数匹配
            if (paramChild != null) {
                if (params == null) {
                    params = new HashMap<>();
                }
                params.put(paramName, path.substring(index, segmentEnd));
                int nextIndex = segmentEnd < end ? segmentEnd + 1 : end;
                RouteMatch result = paramChild.match(path, nextIndex, end, method, params, segmentKey);
                if (result != null) {
                    return result;
                }
                params.remove(paramName);
                if (params.isEmpty()) {
                    params = null;
                }
            }

            return null;
        }
    }

    private static final class SegmentKey implements CharSequence {
        private String path;
        private int start;
        private int end;
        private int hash;

        void reset(String path, int start, int end) {
            this.path = path;
            this.start = start;
            this.end = end;
            this.hash = 0;
        }

        @Override
        public int length() {
            return end - start;
        }

        @Override
        public char charAt(int index) {
            return path.charAt(start + index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return path.subSequence(this.start + start, this.start + end);
        }

        @Override
        public int hashCode() {
            int h = hash;
            if (h == 0) {
                for (int i = start; i < end; i++) {
                    h = 31 * h + path.charAt(i);
                }
                hash = h != 0 ? h : 1;
                return hash;
            }
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            int len = end - start;
            if (obj instanceof CharSequence other) {
                if (other.length() != len) {
                    return false;
                }
                for (int i = 0; i < len; i++) {
                    if (path.charAt(start + i) != other.charAt(i)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
    }
}
