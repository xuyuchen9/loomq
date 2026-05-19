# LoomQ — 面向未来事件的持久化时间内核

[![JDK](https://img.shields.io/badge/JDK-25%2B-green.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/Maven%20Central-0.9.x-blue.svg)](https://central.sonatype.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tests](https://img.shields.io/badge/Tests-622%20tests-brightgreen.svg)]()

**基于 Java 25 虚拟线程，让未来事件可靠发生。**

LoomQ 是一个面向分布式系统的持久化时间内核，负责调度、持久化、恢复、重试编排和 deadline 处理。它既可以作为库嵌入，也可以作为独立服务运行。

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
| **稳定** | 持久化延迟执行、持久化 + 恢复、精度档位调度、重试编排、指标、可插拔存储（内存 + RocksDB）、WAL 段轮转与快照压缩、IntentObserver 生命周期钩子、Raft 领导选举 + 日志复制 + 快照追赶、leader-authoritative 读写、Raft 可观测性、Token 认证、错误恢复建议、叙事式健康检查、死信复活 |
| **Beta** | 动态 Raft 成员管理、Kubernetes Operator |
| **尚未承诺** | 分布式协调原语、锁/租约语义 |

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
    <version>0.9.1</version>
</dependency>
```

### 从源码构建

```bash
git clone https://github.com/xuyuchen9/loomq.git
cd loomq
mvn clean package -DskipTests
```

### 常用 Makefile 命令

```bash
make format       # 应用 Spotless 格式化
make check-format # 校验 Spotless 格式
make test         # 运行默认测试套件
```

### 启动服务

```bash
java -jar loomq-server/target/loomq-server-0.9.1.jar
```

服务默认监听 `http://localhost:7928`。

### 创建第一个 Intent

```bash
curl -X POST http://localhost:7928/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "executeAt": "2026-05-13T12:00:00Z",
    "precisionTier": "STANDARD",
    "callback": {
      "url": "http://your-server/callback",
      "method": "POST"
    }
  }'
```

### 查询 Intent 状态

```bash
curl http://localhost:7928/v1/intents/{intentId}
```

### Raft 模式说明

当 `LOOMQ_RAFT_ENABLED=true` 时，独立服务会切换到 Raft 模式：

- 读写均需 leader 授权 — `GET /v1/intents/{intentId}` 和 `POST/PATCH /v1/intents` 都必须在 leader 上执行
- follower 返回 `503`，错误码 `50301`（读重定向）、`50302`（写重定向）、`50303`（写 backpressure）；`retryable=true`，已知 leader 时在 `details` 中返回 leader id
- `RaftWriteCoordinator` 提供有界 backpressure、请求去重、以及基于 `X-LoomQ-Expected-Revision` 头的乐观并发控制
- `/health` 和 `/metrics` 暴露 Raft 信号：角色、leader id、term、commit index、commit lag、replication lag、peer 可达性、leader 读写接纳状态
- `/health/deep` 在 Raft 视图之上附加各精度档位的 backpressure 数据
- `/health/ready` 在未就绪时返回 `503` 并附带 Raft 原因码

### LoomQ CLI — 交互式时间探索器

```bash
# 构建 CLI
mvn package -pl loomq-cli -am -DskipTests

# 运行（默认连接 http://localhost:8080，可通过 LOOMQ_URL 环境变量覆盖）
java -jar loomq-cli/target/loomq-cli-0.9.1.jar

# 或指定自定义服务地址
$env:LOOMQ_URL="http://localhost:7928"; java -jar loomq-cli/target/loomq-cli-0.9.1.jar
```

交互命令：`schedule`、`get`、`list`、`chronoscope`、`timeline`、`dead-letters`、`revive`、`health`、`follow`、`help`、`exit`。

---

## 核心能力

| 特性 | 说明 |
|------|------|
| **五档精度调度** | ULTRA（10ms）、FAST（50ms）、HIGH（100ms）、STANDARD（500ms）、ECONOMY（1000ms）五档精度 |
| **持久化保证** | 内存映射 WAL，支持 ASYNC / BATCH_DEFERRED / DURABLE 模式；分档差异化写入策略 |
| **虚拟线程原生** | 投递和调度均使用虚拟线程，零线程池调优 |
| **Cohort 批量唤醒** | CSA 风格批量唤醒 — 5 个守护线程替代 100,000 个 VT sleep（100% VT 减少） |
| **批量投递** | 100,000 个 Intent 仅需 2,231 次 HTTP 请求 — 44.8x 减少 |
| **Arrow 跨层借用** | 高优档位可从低优档位借用空闲 slot，AdapTBF 限制出借上限防止饥饿 |
| **动态并发控制** | 每档位 `ResizableSemaphore` 支持运行时调整并发上限，无需重启 |
| **冷热交换** | 长延迟 Intent（>1h）自动换出/换入，降低 72.5% 堆内存占用 |
| **分段 WAL** | 多段 WAL 文件自动轮转；快照后自动截断；1.25M ops/sec ASYNC 吞吐 |
| **可插拔存储** | `IntentStore` 接口 + `ConcurrentIntentStore`（内存）+ `RocksDBIntentStore`（持久化） |
| **IntentObserver SPI** | 生命周期钩子（onScheduled/onDelivered/onDeadLettered/onExpired/onDeliveryFailed） |
| **崩溃恢复** | 快照 + WAL 回放管线，gzip 压缩二进制快照，每 5 分钟自动生成 |
| **SLO 档位推荐** | `TierAdvisor` 根据 maxTardinessMs + Reliability 自动推荐最优档位，含 2× 安全余量 |
| **叙事式健康检查** | `HealthNarrator` 将二元 UP/DOWN 升级为"是否准时"的叙事式诊断，含结构化的生命体征和异常检测 |
| **时间线预测** | `TimelineService` 提供未来 N 分钟的 cohort 唤醒预览和调度器活动预测 |
| **WAL 时间旅行** | `WalReplayService` 支持通过 WAL 回放重建 Intent 生命周期或任意时间点的系统状态 |
| **Intent 列表查询** | `GET /v1/intents` 支持按状态过滤，分页返回 |
| **死信复活** | `POST /v1/intents/{id}/revive` 复活死信 Intent，可指定新的执行时间 |
| **错误恢复建议** | `ErrorRecoveryAdvisor` 为每个错误码提供可操作的恢复建议：出了什么问题、下一步怎么做、暂态等待时间 |
| **Token 认证** | `SecurityConfig` 可选 `X-LoomQ-Token` 头认证，恒定时间比较，健康端点豁免 |
| **LoomQ CLI** | 交互式时间探索器（`loomq-cli`），10+ 命令：schedule、get、list、chronoscope、timeline、dead-letters、revive、health、follow |

---

## 性能摘要

在当前基准环境下，完整套件维持约 3,504.5 QPS，调度 p50 约 3.6s，投递 p95 约 13ms。详细表格和运行方式见英文 README，以及 `benchmark/results/` 下的历史结果。

---

## 嵌入模式（无需 HTTP）

```java
import com.loomq.LoomqEngine;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;

LoomqEngine engine = LoomqEngine.builder()
    .walDir(Path.of("./data"))
    .defaultTier(PrecisionTier.FAST)
    .deliveryHandler(intent -> {
        System.out.println("Intent 触发: " + intent.getIntentId());
        return DeliveryResult.SUCCESS;
    })
    .build();

engine.start();

Intent intent = new Intent();
intent.setExecuteAt(Instant.now().plusSeconds(5));
intent.setPrecisionTier(PrecisionTier.STANDARD);
engine.createIntent(intent, AckMode.ASYNC);

engine.close();
```

---

## 部署与运维

- 生产部署、Docker、Kubernetes 和 Raft 说明见 [部署指南](docs/operations/DEPLOYMENT.md)
- API 和配置细节见 `docs/development/` 与 `docs/operations/`
- 运维诊断工具：`LoomQ CLI`（交互式终端）、`/v1/admin/chronoscope`（调度器 X 光快照）、`/v1/admin/timeline`（时间线预测）、`/health/deep`（深度健康检查）
- 发版前建议先跑完整测试，再打 GitHub release

---

## 路线图

### v0.9.1（当前版本）
- [x] Cohort 批量唤醒、Arrow 跨层借用、ResizableSemaphore
- [x] 冷热交换（72.5% 内存优化）、分段 WAL、可插拔存储
- [x] IntentObserver / WalAccessor SPI
- [x] Raft 领导选举、日志复制、快照追赶、leader-authoritative 读写
- [x] Raft 写协调：有界 backpressure、请求去重、乐观并发
- [x] LoomQ CLI（交互式时间探索器，10+ 命令）
- [x] HealthNarrator（叙事式健康检查）、ErrorRecoveryAdvisor（错误恢复建议）
- [x] TimelineService（时间线预测）、TierAdvisor（SLO 档位推荐）
- [x] WalReplayService（WAL 时间旅行调试）
- [x] 死信复活、Intent 列表查询、Token 认证
- [x] ChronoscopeSnapshot（调度器内部状态 X 光快照）

### v0.9.2（规划中）
- [ ] 动态 Raft 成员管理
- [ ] Kubernetes Operator
- [ ] Web 管理控制台
- [ ] gRPC 投递处理器
