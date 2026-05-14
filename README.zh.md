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
| **稳定** | 持久化延迟执行、持久化 + 恢复、精度档位调度、重试编排、指标、可插拔存储（内存 + RocksDB）、WAL 段轮转与快照压缩、IntentObserver 生命周期钩子 |
| **Beta** | Raft 领导选举、日志复制、快照追赶 |
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
    <version>0.9.0</version>
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
java -jar loomq-server/target/loomq-server-0.9.0.jar
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

---

## 核心能力

| 特性 | 说明 |
|------|------|
| **五档精度调度** | ULTRA、FAST、HIGH、STANDARD、ECONOMY 五档精度 |
| **持久化保证** | 内存映射 WAL，支持 ASYNC / BATCH_DEFERRED / DURABLE 模式 |
| **虚拟线程原生** | 投递和调度均使用虚拟线程 |
| **Cohort 批量唤醒** | 将长延迟 Intent 聚合到 cohort 中批量唤醒 |
| **批量投递** | 通过批量聚合减少 HTTP 请求数 |
| **Arrow 跨层借用** | 高优档位可从低优档位借用空闲 slot |
| **动态并发控制** | 每档位 `ResizableSemaphore` 支持运行时调整并发上限 |
| **冷热交换** | 长延迟 Intent 自动换出/换入，降低堆内存占用 |
| **崩溃恢复** | 快照 + WAL 回放管线，支持重启恢复 |
| **可观测性** | 延迟直方图、RTT 指标、Prometheus 导出 |
| **可插拔存储** | `ConcurrentIntentStore` 和 `RocksDBIntentStore` |

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
- 发版前建议先跑完整测试，再打 GitHub release
