# LoomQ — 面向未来事件的持久化时间内核

[![JDK](https://img.shields.io/badge/JDK-25%2B-green.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/Maven%20Central-0.8.x-blue.svg)](https://central.sonatype.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tests](https://img.shields.io/badge/Tests-471%20passed-brightgreen.svg)]()

**基于 Java 25 虚拟线程，让未来事件可靠发生。**

LoomQ 是一个面向分布式系统的持久化时间内核——调度、持久化、恢复、重试编排和 deadline 处理。可以作为库嵌入，也可以作为独立服务运行。

## LoomQ 是什么 / 不是什么

**LoomQ 是：**

- 面向未来事件（称为 **Intent**）的持久化调度内核
- 可嵌入（`loomq-core`，零 HTTP/JSON 依赖）或独立部署（`loomq-server`，Netty HTTP）
- 通过 SPI 钩子扩展投递、回调和重试决策

**LoomQ 不是：**

- 通用消息队列
- 工作流引擎
- 锁或租约服务（这些属于内核之上的层次）

## 能力成熟度

| 类别 | 示例 |
|------|------|
| **稳定** | 持久化延迟执行、持久化+恢复、精度档位调度、重试编排、指标 |
| **Beta** | 集群配套、复制、分片路由、故障转移 |
| **尚未承诺** | 分布式协调原语、锁/租约语义、Leader 选举 |

---

## 快速开始

### 环境要求

- JDK 25+
- Maven 3.9+

### Maven 依赖

```xml
<dependency>
    <groupId>com.loomq</groupId>
    <artifactId>loomq-core</artifactId>
    <version>0.8.0-SNAPSHOT</version>
</dependency>
```

### 从源码构建

```bash
git clone https://github.com/loomq/loomq.git
cd loomq
mvn clean package -DskipTests
```

### 启动服务

```bash
java -jar loomq-server/target/loomq-server-0.8.0-SNAPSHOT.jar
```

服务默认监听 `http://localhost:8080`。

### 创建第一个 Intent

```bash
curl -X POST http://localhost:8080/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "executeAt": "'$(date -u -d "+30 seconds" +%Y-%m-%dT%H:%M:%SZ)'",
    "precisionTier": "STANDARD",
    "callback": {
      "url": "http://your-server/callback",
      "method": "POST"
    }
  }'
```

### 查询 Intent 状态

```bash
curl http://localhost:8080/v1/intents/{intentId}
```

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **五档精度调度** | ULTRA(10ms)、FAST(50ms)、HIGH(100ms)、STANDARD(500ms)、ECONOMY(1000ms) |
| **持久化保证** | 内存映射 WAL，支持 ASYNC/DURABLE/REPLICATED 三级确认，CRC32 校验 |
| **虚拟线程原生** | 零线程池调优；所有投递和调度均在虚拟线程上运行 |
| **Cohort 批量唤醒** | CSA 风格批量唤醒替代逐 Intent 虚拟线程休眠，高效处理长延迟调度 |
| **Arrow 跨层借用** | 高优档位在突发时从低优档位借用空闲 slot，带 AdapTBF 约束边界 |
| **动态并发控制** | 每档位 `ResizableSemaphore` 支持运行时调整并发上限，无需重启 |
| **崩溃恢复** | 快照 + WAL 回放管线，每 5 分钟 gzip 压缩二进制快照 |
| **可观测性** | 按档位延迟直方图(P50–P99.9)、RTT 指标、Prometheus 导出、借用统计 |

---

## v0.8.0 新特性

v0.8.0 在 v0.7.0 模块拆分的基础上，使并发控制变得自适应和可观测。

| 组件 | 功能 |
|------|------|
| **CohortManager** | 将相近 executeAt 的 Intent 分批——一个守护线程唤醒数千 Intent，替代逐 Intent 虚拟线程休眠 |
| **ResizableSemaphore** | 继承 `Semaphore`，零开销 acquire/tryAcquire；支持运行时 `resize()` 渐进调整许可数 |
| **Arrow 借用** | 档位自身 slot 耗尽时，消费者从低优空闲档位借用（100ms 超时） |
| **AdapTBF 约束** | 限界 Arrow 借用：每档位最多借出 50% slot，防止低优档位饿死 |
| **RTT 指标** | 按档位的出队→webhook 接收延迟(p50/p95/p99)，独立于调度精度 |
| **BorrowStats** | `own_direct`、`own_blocking`、`borrowed`、`borrow_timeouts`、`borrow_rate`——完整借用可见性 |

---

## 精度档位

| 档位 | 窗口 | 并发 | 批量 | 消费者 | WAL 模式 | 适用场景 |
|------|------|------|------|--------|----------|----------|
| **ULTRA** | 10ms | 200 | 1×5ms | 16 | ASYNC | 心跳、亚 50ms deadline |
| **FAST** | 50ms | 150 | 1×10ms | 12 | ASYNC | 消息重试、退避 |
| **HIGH** | 100ms | 50 | 5×50ms | 4 | BATCH | 通用场景 |
| **STANDARD** | 500ms | 50 | 20×100ms | 3 | DURABLE | **推荐**，订单超时 |
| **ECONOMY** | 1000ms | 50 | 25×300ms | 2 | DURABLE | 长延迟 Intent、批量调度 |

**选型指南：**
- 亚 50ms deadline：**ULTRA** 或 **FAST**
- 订单超时和定时通知：**STANDARD**（吞吐与延迟最佳平衡）
- 海量批量调度（>1 小时延迟）：**ECONOMY**（资源效率最高）

---

## 性能基准

**测试环境：** JDK 25、NVMe SSD、16 核、localhost、Netty mock server

### 全量测试（10 万 Intent，每档位 2 万）

| 档位 | E2E p50 | E2E p95 | E2E p99 | RTT p50 | RTT p95 |
|------|---------|---------|---------|---------|---------|
| ULTRA | 1,675ms | 2,967ms | 3,076ms | 1ms | 16ms |
| FAST | 3,606ms | 5,979ms | 6,210ms | 1ms | 16ms |
| HIGH | 11,272ms | 20,055ms | 20,800ms | 1ms | 16ms |
| STANDARD | 18,192ms | 30,894ms | 31,503ms | 1ms | 16ms |
| ECONOMY | 15,470ms | 27,987ms | 29,122ms | 15ms | 17ms |

**系统 QPS：** 2,476（10 万 Intent 约 40 秒完成）

### Arrow 借用效率

| 指标 | 数值 |
|------|------|
| 直接获取 | 164,413 |
| 借用获取 | 18,190 (9.6%) |
| 阻塞回退 | 6,768 |
| 借用超时 | 11,047 |

> **核心结论：** Arrow 借用处理了 9.6% 的获取请求且无需阻塞。AdapTBF 50% 借出上限防止高优档位饿死 ECONOMY，同时仍能显著吸收突发流量。

---

## 嵌入模式（无需 HTTP）

```java
import com.loomq.LoomqEngine;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;

LoomqEngine engine = LoomqEngine.builder()
    .walDir(Path.of("./data"))
    .deliveryHandler(intent -> {
        System.out.println("Intent 触发: " + intent.getIntentId());
        return DeliveryResult.SUCCESS;
    })
    .build();

engine.start();

// 5 秒后调度
Intent intent = new Intent();
intent.setExecuteAt(Instant.now().plusSeconds(5));
intent.setPrecisionTier(PrecisionTier.STANDARD);
engine.createIntent(intent, AckMode.ASYNC);

// 完成后：
engine.close();
```

嵌入模式适用场景：单节点应用、集成测试、资源受限环境、基于 LoomQ 核心的自定义壳。

---

## 架构

```
loomq-server (Netty HTTP + JSON + webhook 投递)
    ├── IntentHandler        — REST API 路由 (RadixTree)
    ├── NettyHttpServer      — epoll + 池化分配器 + 信号量背压
    └── HttpDeliveryHandler  — Reactor Netty HTTP 客户端

loomq-core (可嵌入内核，零 HTTP/JSON 依赖)
    ├── LoomqEngine           — Builder 模式入口
    ├── PrecisionScheduler    — 时间轮分桶，按档位扫描 + 批量消费
    │   ├── CohortManager     — CSA 风格批量唤醒
    │   ├── BucketGroupManager — 按档位时间桶存储
    │   └── ResizableSemaphore — 运行时可调并发（继承 Semaphore）
    ├── IntentStore           — 内存 ConcurrentHashMap 存储
    ├── SimpleWalWriter       — 内存映射 WAL，FFM API，~100ns/记录
    ├── RecoveryPipeline      — 重启时快照 + WAL 回放
    └── SPI 接口              — DeliveryHandler, CallbackHandler, RedeliveryDecider
```

### 调度器设计

**批量消费者（fire-and-forget）：** 每档位运行 N 个虚拟线程消费者。消费者获取许可（自身档位或借用），从投递队列出队，调用 `deliveryHandler.deliverAsync()`。许可在异步回调中释放——消费者线程永不阻塞于 HTTP。

**Cohort 唤醒（CSA 风格）：** 延迟超过精度窗口的 Intent 按 cohort key 分组。单个守护线程在每个 cohort 到期时唤醒，将所有 Intent 刷入桶中。替代逐 Intent 虚拟线程休眠——一个线程处理数千 Intent。

**Arrow 借用（AdapTBF 限界）：** 档位自身信号量耗尽时，消费者对每个低优档位尝试 `tryAcquire(100ms)`。每个档位最多借出 50% slot。归还的许可在出借方信号量上释放，并追踪 `borrowedCount`。

---

## API 参考

| 方法 | 端点 | 说明 |
|------|------|------|
| POST | `/v1/intents` | 创建 Intent |
| GET | `/v1/intents/{id}` | 查询 Intent |
| PATCH | `/v1/intents/{id}` | 更新 Intent 字段 |
| POST | `/v1/intents/{id}/cancel` | 取消 Intent |
| POST | `/v1/intents/{id}/fire-now` | 立即触发 |
| GET | `/health` | 健康检查 |
| GET | `/health/live` | 存活探针 |
| GET | `/health/ready` | 就绪探针（含 WAL 健康） |
| GET | `/metrics` | JSON 指标快照 |

### 创建 Intent 请求

```json
{
  "executeAt": "2026-05-01T10:30:00Z",
  "deadline": "2026-05-01T11:00:00Z",
  "precisionTier": "STANDARD",
  "ackLevel": "DURABLE",
  "idempotencyKey": "req-abc-123",
  "callback": {
    "url": "https://example.com/webhook",
    "method": "POST",
    "headers": {"X-Request-Id": "123"}
  },
  "redelivery": {
    "maxAttempts": 5,
    "backoff": "exponential",
    "initialDelayMs": 1000,
    "maxDelayMs": 60000
  },
  "tags": {"tenant": "demo"}
}
```

**必填：** `executeAt` | **可选：** `intentId`、`deadline`、`expiredAction`、`precisionTier`、`shardKey`、`ackLevel`、`callback`、`redelivery`、`idempotencyKey`、`tags`

### 错误响应

| 状态码 | 场景 |
|--------|------|
| **404** | Intent 不存在 |
| **409** | 幂等键冲突（重复且已终态） |
| **422** | 校验错误（时间无效、状态不可修改等） |
| **429** | 背压——根据响应中 `retryAfterMs` 重试 |

---

## SPI 扩展点

| 接口 | 方法 | 用途 |
|------|------|------|
| `DeliveryHandler` | `deliverAsync(Intent)` → `CompletableFuture<DeliveryResult>` | 投递方式（HTTP、MQ、本地） |
| `CallbackHandler` | `onIntentEvent(Intent, EventType, Throwable)` | 生命周期事件通知 |
| `RedeliveryDecider` | `shouldRedeliver(DeliveryContext)` → `boolean` | 自定义重试策略 |

`DeliveryResult` 枚举：`SUCCESS`、`RETRY`、`DEAD_LETTER`、`EXPIRED`

---

## 配置

配置加载优先级（从高到低）：
1. JVM 系统属性（`-Dloomq.xxx`）
2. 外部 `./config/application.yml`
3. Classpath `application.yml`
4. `@DefaultValue` 注解

主要配置组：`server.*`、`netty.*`、`wal.*`、`scheduler.*`、`dispatcher.*`、`retry.*`、`recovery.*`

完整配置项参见 [配置说明](docs/operations/CONFIGURATION.md)。

---

## 发展路线

### v0.8.0（当前）
- [x] Cohort 批量唤醒（CSA 风格）
- [x] Arrow 跨层 slot 借用
- [x] AdapTBF 借出约束
- [x] ResizableSemaphore（运行时并发调整）
- [x] RTT 按档位指标
- [ ] 插件化存储引擎（RocksDB、LevelDB）

### v0.9.0
- 基于 Raft 的多节点集群
- Web 管理控制台
- **Loomqex**：基于稳定内核边界的锁/租约语义

### 未来
- Kubernetes Operator
- 多区域复制
- 回调负载 Schema 注册中心

---

## 开发

- [发布清单](docs/engineering/release-checklist.md)
- [Benchmark 清单](docs/engineering/benchmark-checklist.md)
- [配置说明](docs/operations/CONFIGURATION.md)
- [架构详情](docs/development/ARCHITECTURE.md)
- [核心模型](docs/architecture/core-model.md)

## 参与贡献

欢迎提交 Issue 和 PR。详见 [CONTRIBUTING.md](CONTRIBUTING.md)。

---

## 许可证

```
Copyright 2026 LoomQ Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
