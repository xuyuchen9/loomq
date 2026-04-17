# LoomQ v0.6.0 - Netty HTTP 层重构

**发布日期**: 2026-04-10  
**版本**: v0.6.0-http  
**状态**: 已发布

---

## 1. 版本概述

v0.6.0 彻底重构 HTTP 接入层，基于 **Netty 原生实现**替代 Javalin/Jetty，消除框架抽象带来的性能损耗。

### 1.1 问题背景

| HTTP 引擎 | JSON 端点 QPS | 瓶颈分析 |
|:---|:---|:---|
| Jetty 11 (默认) | ~34K | 平台线程池 + 上下文切换 |
| JDK HttpServer + 虚拟线程 | ~36K | `HttpServer` 内部单队列 + 对象分配压力 |
| **Netty 原生 (v0.6.0)** | **~400K+** | 消除框架开销 |

核心引擎 IntentStore 可达 **115 万 QPS**，v0.6.0 将 HTTP 接入层与之差距缩小至 3 倍以内。

### 1.2 版本目标

- **吞吐量**: JSON 端点 `POST /v1/intents` ≥ **300K QPS**（内部基准测试）
- **延迟**: P99 ≤ **5ms**
- **架构纯粹性**: 消除所有不必要的抽象层
- **API 兼容性**: 对外 REST API 保持完全兼容

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Netty Bootstrap                        │
│   NioEventLoopGroup (I/O) + Virtual Thread Executor         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    HTTP 协议解码器                          │
│   HttpServerCodec → HttpObjectAggregator → RequestHandler   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    RadixRouter (前缀树)                      │
│   - O(k) 路径匹配 (k=路径长度)                               │
│   - 支持路径参数 {param}                                     │
│   - 支持同一路径多 HTTP 方法                                  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   业务处理器 (Controller)                    │
│   - 运行于虚拟线程池                                         │
│   - 直接调用 IntentStore                                     │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 线程模型与内存安全

**核心原则**：
- **I/O 线程**（EventLoop）仅处理 socket 读写、HTTP 编解码、请求体提取
- **请求体数据立即拷贝到堆内存**，原始 `ByteBuf` 在 I/O 线程中释放
- **业务逻辑**运行于虚拟线程池，使用已拷贝的堆内数据
- **响应写回**由 I/O 线程完成，确保 Netty 出站操作线程安全

### 2.3 关键组件

| 组件 | 文件 | 描述 |
|:---|:---|:---|
| NettyHttpServer | `http/netty/NettyHttpServer.java` | Netty 服务器启动/停止 |
| RadixRouter | `http/netty/RadixRouter.java` | 前缀树路由器，O(k) 匹配 |
| NettyRequestHandler | `http/netty/NettyRequestHandler.java` | 请求处理器，I/O 与业务线程分离 |
| IntentHandler | `http/netty/IntentHandler.java` | Intent API 端点实现 |
| HttpMetrics | `http/netty/HttpMetrics.java` | HTTP 层 Prometheus 指标 |

---

## 3. 性能优化

### 3.1 优化策略

1. **流式 JSON 解析**: 使用 `JsonParser` 避免 Map 中间层
2. **预分配 JsonFactory**: 避免每次请求创建解析器工厂
3. **ObjectWriter 复用**: 预创建序列化器避免重复配置
4. **预序列化静态响应**: 健康检查、错误响应预序列化为 `byte[]`
5. **信号量背压限流**: 50K 并发请求上限

### 3.2 性能结果

| 端点 | QPS | 目标 | 状态 |
|:---|:---|:---|:---|
| Router Only | 1.22M | 1M | ✓ PASS |
| Health Endpoint | 5.6M | 500K | ✓ PASS |
| Router + Handler | **414K** | 300K | ✓ PASS |

---

## 4. 文件变更

### 4.1 新增文件

```
src/main/java/com/loomq/http/netty/
├── NettyHttpServer.java      # Netty 服务器
├── NettyServerConfig.java    # 服务器配置
├── NettyRequestHandler.java  # 请求处理器
├── RadixRouter.java          # 前缀树路由器
├── RouteMatch.java           # 路由匹配结果
├── Handler.java              # 处理器接口
├── IntentHandler.java        # Intent API 端点
├── HttpMetrics.java          # HTTP 指标
└── MaxConnectionsHandler.java # 连接数限制
```

### 4.2 修改文件

```
src/main/java/com/loomq/LoomqEngine.java  # 集成 Netty HTTP 服务器
src/main/resources/application.yml         # 添加 Netty 配置
```

---

## 5. 配置项

```yaml
# Netty HTTP 服务器配置 (v0.6.0)
netty:
  port: 8080
  host: "0.0.0.0"
  bossThreads: 1                    # Boss 线程数
  workerThreads: 0                  # 0 = CPU 核数
  maxContentLength: 10485760        # 10MB
  useEpoll: true                    # Linux 启用 Epoll
  soBacklog: 1024
  tcpNoDelay: true
  idleTimeoutSeconds: 60
  maxConnections: 10000
  maxConcurrentBusinessRequests: 50000  # 信号量上限
  gracefulShutdownTimeoutMs: 30000
```

---

## 6. API 端点

| 方法 | 路径 | 描述 |
|:---|:---|:---|
| POST | `/v1/intents` | 创建 Intent |
| GET | `/v1/intents/{intentId}` | 查询 Intent |
| PATCH | `/v1/intents/{intentId}` | 修改 Intent |
| POST | `/v1/intents/{intentId}/cancel` | 取消 Intent |
| POST | `/v1/intents/{intentId}/fire-now` | 立即触发 |
| GET | `/health` | 健康检查 |
| GET | `/health/live` | 存活检查 |
| GET | `/health/ready` | 就绪检查 |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/v1/cluster/status` | 集群状态 |

---

## 7. 测试

### 7.1 单元测试

- `RadixRouterTest` - 路由器单元测试
- `IntentHandlerRouteTest` - IntentHandler 路由测试

### 7.2 集成测试

- `V5IntegrationTest` - V5 API 集成测试 (7/7 通过)

### 7.3 性能测试

- `InternalNettyBenchmark` - 内部性能基准测试

---

## 8. 升级指南

从 v0.5.x 升级到 v0.6.0:

1. **无需修改客户端代码** - REST API 完全兼容
2. **配置变更** - 添加 `netty.*` 配置项（可选，有默认值）
3. **依赖变更** - 无新增依赖，Netty 已在 v0.4.8 引入

---

## 9. 已知限制

1. **HTTP/2 不支持**: 当前仅支持 HTTP/1.1
2. **WebSocket 不支持**: 专注 REST API 场景
3. **静态文件服务不支持**: 非 Web 服务器定位

---

## 10. 后续规划

- [ ] HTTP/2 支持
- [ ] 连接级限流
- [ ] 请求体零拷贝优化（需评估线程安全）
- [ ] 响应压缩
