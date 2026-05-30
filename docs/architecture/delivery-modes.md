# 投递模式选择指南

LoomQ 支持三种投递模式，适用于不同的场景和性能需求。本文档帮助你根据业务场景选择最合适的模式。

---

## 概述

| 模式 | 实现类 | 核心机制 |
|:---|:---|:---|
| HTTP 单请求 | `NettyHttpDeliveryHandler` | 每个 Intent 触发一次 HTTP POST |
| HTTP 批量 | `BatchedHttpDeliveryHandler` | 多个 Intent 聚合为一次 HTTP POST |
| gRPC 流 | `GrpcStreamDeliveryHandler` | 通过持久 gRPC 流推送 Intent |

---

## 性能对比

基于 5,000 个 ULTRA Intent 同时到期的压力测试（Mock Server 延迟 50ms）：

| 指标 | HTTP 单请求 | HTTP 批量 | gRPC 流 |
|:---|:---|:---|:---|
| **HTTP 请求数** | 4,877 | 111 | **0** |
| **完成率** | 97.5% | 100% | **100%** |
| **吞吐量** | 536 QPS | 2,456 QPS | **3,290 QPS** |
| **E2E P50** | 1,157ms | 457ms | **45ms** |
| **E2E P99** | 1,563ms | 948ms | **57ms** |
| **投递延迟 P50** | 20ms | 1ms | **1ms** |
| **投递延迟 P99** | 169ms | 16ms | **8ms** |

> **关键发现**：gRPC 流模式的 E2E 延迟比 HTTP 单请求模式快 **26 倍**，吞吐量高 **514%**。

---

## 三种模式详解

### 1. HTTP 单请求模式（默认）

**工作原理**：
```
PrecisionScheduler → deliverAsync(intent) → HTTP POST → Webhook Server → 2xx 响应
```

**优点**：
- 最简单的实现，易于调试
- 兼容所有 HTTP Webhook 服务器
- 无需额外基础设施

**缺点**：
- 每个 Intent 一次 HTTP 请求，协议开销大
- 受限于 Webhook 服务器响应时间
- 高并发时连接池成为瓶颈

**适用场景**：
- 低并发（< 100 QPS）
- Webhook 服务器响应快（< 10ms）
- 传统 Webhook 集成

### 2. HTTP 批量模式

**工作原理**：
```
PrecisionScheduler → deliverAsync(intent) → 批量队列 → 聚合 → HTTP POST (JSON Array) → Webhook Server
```

**批量配置**：
- `maxBatchSize`：单次批量请求的最大 Intent 数（默认 200）
- `flushIntervalMs`：批量队列的 flush 间隔（默认 10ms）

**优点**：
- HTTP 请求数减少 98%+
- 吞吐量提升 3-5 倍
- 完成率提升（减少超时丢失）

**缺点**：
- 增加批次驻留延迟（平均 10-100ms）
- Webhook 服务器需要支持批量格式
- 低并发时优势不明显

**适用场景**：
- 高吞吐（> 1000 QPS）
- Webhook 服务器处理慢（> 10ms）
- 公网高延迟网络
- 批量任务处理、定时通知

### 3. gRPC 流模式

**工作原理**：
```
PrecisionScheduler → deliverAsync(intent) → gRPC stream push → Gateway → 客户端
```

**优点**：
- E2E 延迟最低（P50 = 45ms）
- 吞吐量最高（3,290 QPS）
- 无 HTTP 协议开销
- 天然支持背压和流控

**缺点**：
- 需要 Gateway 基础设施
- 需要 gRPC 客户端库
- 调试相对复杂

**适用场景**：
- 实时推送（游戏、AI 代理、金融）
- 极高吞吐（> 5000 QPS）
- 延迟敏感（< 100ms）

---

## 配置方式

### 环境变量

```bash
# HTTP 单请求模式（默认）
export LOOMQ_DELIVERY_HANDLER=single

# HTTP 批量模式
export LOOMQ_DELIVERY_HANDLER=batched
export LOOMQ_DELIVERY_BATCH_SIZE=200
export LOOMQ_DELIVERY_FLUSH_INTERVAL_MS=10

# gRPC 流模式
export LOOMQ_DELIVERY_HANDLER=grpc-stream
export LOOMQ_GRPC_ENABLED=true
export LOOMQ_DELIVERY_ACK_MODE=auto  # auto 或 manual
```

### 系统属性

```bash
# HTTP 单请求模式（默认）
java -Dloomq.delivery.handler=single -jar loomq-server.jar

# HTTP 批量模式
java -Dloomq.delivery.handler=batched \
     -Dloomq.delivery.batch.size=200 \
     -Dloomq.delivery.flush.interval.ms=10 \
     -jar loomq-server.jar

# gRPC 流模式
java -Dloomq.delivery.handler=grpc-stream \
     -Dloomq.grpc.enabled=true \
     -jar loomq-server.jar
```

### 配置文件（application.yml）

```yaml
loomq:
  delivery:
    handler: batched  # single | batched | grpc-stream
    batch:
      size: 200
      flush-interval-ms: 10
      max-connections: 500
    ack-mode: auto  # auto | manual
  grpc:
    enabled: true
    host: 0.0.0.0
    port: 7929
```

---

## 场景选择指南

| 场景 | 推荐模式 | 原因 |
|:---|:---|:---|
| 游戏服务器、实时 AI 代理 | gRPC 流 | E2E 延迟 < 50ms，实时推送 |
| 金融交易、实时竞价 | gRPC 流 | 延迟敏感，高吞吐 |
| 批量任务处理 | HTTP 批量 | 高吞吐，减少 HTTP 请求 |
| 定时通知、邮件发送 | HTTP 批量 | 容忍少量延迟，高吞吐 |
| 传统 Webhook 集成 | HTTP 单请求 | 兼容性好，调试简单 |
| 低频触发（< 100/分钟） | HTTP 单请求 | 简单够用 |

---

## 架构图

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                    LoomQ Core                          │
                    │  ┌─────────────────────────────────────────────────┐   │
                    │  │          PrecisionScheduler                     │   │
                    │  │  (调度唤醒延迟 ~1ms)                              │   │
                    │  └────────────────────┬────────────────────────────┘   │
                    │                       │ deliverAsync(intent)           │
                    │  ┌────────────────────┼────────────────────────────┐   │
                    │  │                    ▼                            │   │
                    │  │  ┌─────────────────────────────────────────┐   │   │
                    │  │  │         DeliveryHandler SPI             │   │   │
                    │  │  └─────────────────────────────────────────┘   │   │
                    │  └────────────────────┬────────────────────────────┘   │
                    └───────────────────────┼───────────────────────────────┘
                                            │
              ┌─────────────────────────────┼─────────────────────────────┐
              │                             │                             │
              ▼                             ▼                             ▼
   ┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
   │   HTTP 单请求模式    │     │   HTTP 批量模式      │     │   gRPC 流模式        │
   │                     │     │                     │     │                     │
   │  HTTP POST          │     │  HTTP POST          │     │  gRPC Stream Push   │
   │  (每个 Intent)      │     │  (JSON Array)       │     │  (持久连接)          │
   │                     │     │                     │     │                     │
   │  P99: 1,563ms       │     │  P99: 948ms         │     │  P99: 57ms          │
   │  吞吐: 536 QPS      │     │  吞吐: 2,456 QPS    │     │  吞吐: 3,290 QPS    │
   └─────────────────────┘     └─────────────────────┘     └─────────────────────┘
```

---

## 相关文档

- [架构概览](core-model.md) - Intent 模型和状态机
- [Pipeline 完整链路](pipeline-map.md) - 9 个环节的详细说明
- [配置参考](../operations/CONFIGURATION.md) - 所有配置项
- [基准测试指南](../engineering/benchmark-checklist.md) - 如何运行基准测试
