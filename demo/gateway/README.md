# LoomQ Reference Gateway Demo

本 Demo 展示完整的投递链路：**LoomQ → gRPC 流 → Gateway → WebSocket → 浏览器**

## 架构

```
浏览器 ←WebSocket→ Gateway ←gRPC 流→ LoomQ Server
                        ↓
                   转发 DeliveryEvent 到所有 WebSocket 客户端
```

## 前置条件

- Java 25+
- Maven 3.9+

## 快速开始

### 1. 构建 LoomQ Server

```bash
# 在项目根目录
mvn clean package -DskipTests
```

### 2. 启动 LoomQ Server（gRPC 流投递模式）

```bash
java -Dloomq.delivery.handler=grpc-stream \
     -Dloomq.grpc.enabled=true \
     -jar loomq-server/target/loomq-server-0.9.2.jar
```

### 3. 启动 Reference Gateway

```bash
cd demo/gateway
mvn compile exec:java
```

### 4. 打开浏览器

```
http://localhost:8080
```

### 5. 创建 Intent

```bash
# 创建一个 5 秒后触发的 Intent
curl -X POST http://localhost:7928/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "intentId": "demo-001",
    "executeAt": "2026-05-29T12:00:00Z",
    "precisionTier": "ULTRA",
    "callback": {
      "url": "http://localhost:8080/webhook"
    }
  }'
```

浏览器将实时显示投递事件。

## 配置

### Gateway 环境变量

| 变量 | 默认值 | 说明 |
|:---|:---|:---|
| `LOOMQ_HOST` | `localhost` | LoomQ Server 地址 |
| `LOOMQ_GRPC_PORT` | `8928` | LoomQ gRPC 端口 |
| `GATEWAY_WS_PORT` | `8080` | Gateway WebSocket 端口 |

### LoomQ Server 配置

| 配置 | 说明 |
|:---|:---|
| `loomq.delivery.handler=grpc-stream` | 启用 gRPC 流投递 |
| `loomq.grpc.enabled=true` | 启用 gRPC 服务器 |
| `loomq.delivery.ack.mode=auto` | ACK 模式（auto/manual） |

## 测试场景

### 场景 1：单个 Intent 投递

```bash
curl -X POST http://localhost:7928/v1/intents \
  -H "Content-Type: application/json" \
  -d '{"intentId":"test-001","executeAt":"2026-05-29T12:00:00Z","precisionTier":"ULTRA","callback":{"url":"http://localhost:8080/webhook"}}'
```

### 场景 2：批量 Intent 投递

```bash
# 创建 100 个同时到期的 Intent
for i in $(seq 1 100); do
  curl -X POST http://localhost:7928/v1/intents \
    -H "Content-Type: application/json" \
    -d "{\"intentId\":\"batch-$i\",\"executeAt\":\"2026-05-29T12:00:00Z\",\"precisionTier\":\"ULTRA\",\"callback\":{\"url\":\"http://localhost:8080/webhook\"}}"
done
```

### 场景 3：不同档位

```bash
# ULTRA 档位（10ms 精度）
curl -X POST http://localhost:7928/v1/intents \
  -H "Content-Type: application/json" \
  -d '{"intentId":"ultra-001","executeAt":"2026-05-29T12:00:00Z","precisionTier":"ULTRA","callback":{"url":"http://localhost:8080/webhook"}}'

# ECONOMY 档位（1000ms 精度）
curl -X POST http://localhost:7928/v1/intents \
  -H "Content-Type: application/json" \
  -d '{"intentId":"economy-001","executeAt":"2026-05-29T12:00:00Z","precisionTier":"ECONOMY","callback":{"url":"http://localhost:8080/webhook"}}'
```

## 性能数据

基于 5,000 个 ULTRA Intent 同时到期的压力测试：

| 指标 | gRPC 流模式 | HTTP 批量 | HTTP 单请求 |
|:---|:---|:---|:---|
| E2E P50 | **45ms** | 457ms | 1,157ms |
| E2E P99 | **57ms** | 948ms | 1,563ms |
| 吞吐量 | **3,290 QPS** | 2,456 QPS | 536 QPS |
| 完成率 | **100%** | 100% | 97.5% |

## 故障排除

### Gateway 无法连接 LoomQ

检查：
1. LoomQ Server 是否启动且 gRPC 端口（8928）可访问
2. 是否启用了 gRPC：`-Dloomq.grpc.enabled=true`
3. 是否启用了 gRPC 流投递：`-Dloomq.delivery.handler=grpc-stream`

### 浏览器无法连接 Gateway

检查：
1. Gateway 是否启动且 WebSocket 端口（8080）可访问
2. 浏览器控制台是否有 WebSocket 错误

### 投递事件不显示

检查：
1. Intent 的 `executeAt` 是否在未来
2. Intent 的 `precisionTier` 是否正确
3. LoomQ Server 日志是否有错误

## 相关文档

- [投递模式选择指南](../../docs/architecture/delivery-modes.md)
- [配置说明](../../docs/operations/CONFIGURATION.md)
- [API 文档](../../docs/development/API.md)
