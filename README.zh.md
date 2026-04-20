# LoomQ - 面向未来事件的持久化时间内核

[![JDK](https://img.shields.io/badge/JDK-25%2B-green.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/Maven%20Central-0.7.x-blue.svg)](https://central.sonatype.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tests](https://img.shields.io/badge/Tests-367%20passed-brightgreen.svg)]()

**基于 Java 25 虚拟线程，让未来事件可靠发生。**

LoomQ 是一个面向分布式系统的持久化时间内核，关注的是调度、重调度、过期、恢复、重试编排和 deadline 处理。

> **注意：** `Intent` 是当前公开模型。旧文档里可能还会出现 `task` 的说法，但新内容应优先使用 `Intent`。

## LoomQ 是什么 / 不是什么

**LoomQ 是：**

- 面向未来事件的持久化调度内核
- 可嵌入、可独立部署的恢复友好时间层
- 带有 SPI 钩子的壳友好核心

**LoomQ 不是：**

- 通用消息队列
- 工作流引擎
- 把锁 / 租约语义写死在核心里的产品

## 能力成熟度

| 类别 | 示例 |
|------|------|
| **稳定** | 持久化延迟执行、持久化 + 恢复、调度、重试编排、基础指标 |
| **Beta** | 集群配套、存储插件接口、复制相关实验能力 |
| **尚未承诺** | 分布式协调原语、锁 / 租约语义、Leader 选举 |

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **五档精度调度** | ULTRA(10ms)、FAST(50ms)、HIGH(100ms)、STANDARD(500ms)、ECONOMY(1000ms)——根据场景选择合适精度 |
| **持久化保证** | ASYNC 模式 RPO < 100ms，DURABLE 模式 RPO = 0 |
| **虚拟线程原生** | 零线程池调优，轻松处理百万级并发 Intent |
| **O(1) 过期复杂度** | 时间轮分桶过期回收，常数时间复杂度 |
| **Netty HTTP 层** | 高性能 HTTP 服务 + 内存映射零拷贝 WAL |
| **可观测性** | Prometheus 指标 + HdrHistogram 高精度延迟统计 |

---

## 性能基准

**测试环境：** JDK 25、NVMe SSD、16 核、localhost

### 写入吞吐量

| 模式 | QPS | 说明 |
|------|-----|------|
| ASYNC | **424,077** | 发布后立即返回，RPO < 100ms |
| DURABLE | **>150,000** | WAL 刷盘后返回，RPO = 0 |

### 端到端触发性能（50 并发线程）

| 档位 | 窗口 | QPS | 资源效率 | P99 延迟 | 适用场景 |
|------|------|-----|---------|----------|----------|
| ULTRA | 10ms | 45,949 | 2.5% | ≤15ms | 高频心跳 / 短 deadline |
| FAST | 50ms | 44,718 | 5% | ≤60ms | 消息重试、退避 |
| HIGH | 100ms | 40,710 | 10% | ≤120ms | 默认档位 |
| STANDARD | 500ms | 39,974 | 25% | ≤550ms | **推荐**，订单超时 |
| ECONOMY | 1000ms | 42,295 | 25% | ≤1100ms | 海量长延迟 Intent |

**关键结论：** ECONOMY 档位的单线程效率是 ULTRA 的 10 倍，适合海量长延迟 Intent 场景。

---

## 快速开始

### 环境要求

- JDK 25+（无需 `--enable-preview`）
- Maven 3.9+

### Maven 依赖

```xml
<dependency>
    <groupId>com.loomq</groupId>
    <artifactId>loomq-core</artifactId>
    <version>0.7.0-SNAPSHOT</version>
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
java -jar loomq-server/target/loomq-server-0.7.0-SNAPSHOT.jar
```

服务默认监听 `http://localhost:8080`。

### 创建第一个 Intent

```bash
# 创建 30 秒后触发的 Intent
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

## 精度档位说明

| 档位 | 窗口 | 最大延迟 | 推荐并发 | 批量策略 | 适用场景 |
|------|------|---------|---------|---------|----------|
| ULTRA | 10ms | <1分钟 | 10-50 | 单 Intent | 心跳、短 deadline |
| FAST | 50ms | <5分钟 | 50-100 | 小批量 | 消息重试、指数退避 |
| HIGH | 100ms | <30分钟 | 100-500 | 中批量 | 默认选择，通用场景 |
| STANDARD | 500ms | <24小时 | 500-2000 | 大批量 | **推荐**，订单超时、定时通知 |
| ECONOMY | 1000ms | >24小时 | 2000+ | 海量批量 | 长延迟 Intent、数据保留策略 |

**选型指南：**
- 高频 deadline 或心跳场景：**ULTRA** 或 **FAST**
- 订单超时场景：**STANDARD**（最佳平衡）
- 海量长延迟 Intent：**ECONOMY**

---

## 架构亮点

LoomQ 基于第一性原理设计，追求极致性能：

### 虚拟线程休眠替代优先级队列

传统调度器使用优先队列（堆），插入复杂度 O(log n)，且需要中心化锁。LoomQ 使用虚拟线程休眠直到执行时间——无锁、无堆操作，纯粹依赖操作系统调度。

```
传统方案:  insert(heap) → O(log n) + 锁竞争
LoomQ:    virtualThread.sleep(until) → O(1), 无锁
```

### MemorySegment + StripedCondition

等待过程零对象分配。使用 `MemorySegment` 直接内存访问，`StripedCondition` 分段条件变量——彻底消除唤醒时的"惊群效应"。

### 批量投递与资源隔离

低精度档位（STANDARD、ECONOMY）自动享受批量收集和投递，单线程效率达到高精度档位的 10 倍。

---

## API 参考

| 方法 | 端点 | 说明 |
|------|------|------|
| POST | `/v1/intents` | 创建 Intent |
| GET | `/v1/intents/{id}` | 查询 Intent 状态 |
| PATCH | `/v1/intents/{id}` | 更新 Intent |
| POST | `/v1/intents/{id}/cancel` | 取消 Intent |
| POST | `/v1/intents/{id}/fire-now` | 立即触发 |
| GET | `/health` | 健康检查 |
| GET | `/metrics` | Prometheus 指标 |

### 请求体示例

```json
{
  "executeAt": "2024-01-15T10:30:00Z",
  "deadline": "2024-01-15T11:00:00Z",
  "precisionTier": "STANDARD",
  "ackLevel": "DURABLE",
  "callback": {
    "url": "http://your-server/callback",
    "method": "POST",
    "headers": {"X-Request-Id": "123"},
    "body": "{\"event\": \"timeout\"}"
  }
}
```

---

## 已知限制

LoomQ v0.6.x 定位为单机版，存在以下限制：

| 限制 | 说明 |
|------|------|
| **无分布式复制** | REPLICATED ack 级别尚未实现。高可用场景可在负载均衡后部署多个独立实例 |
| **内存容量限制** | Intent 容量受堆内存限制。约 1000 万 Intent 需要 8GB 堆 |
| **无 Web 管理界面** | 通过 REST API 或 CLI 操作 |
| **单节点** | v0.6.x 不支持集群和故障转移 |

---

## 发展路线

### v0.7.0
- 拆分为 `loomq-core`（嵌入式内核）+ `loomq-server`（独立服务）
- 插件化存储引擎（支持 RocksDB、LevelDB）
- REST API v2 + OpenAPI 规范

### v0.8.0
- **Loomqex**：基于 LoomQ 的外置 lease / lock 壳，前提是内核边界已经稳定验证
- 基于 Raft 的多节点集群
- Web 管理控制台

### 未来规划
- 云原生部署（Kubernetes Operator）
- 多区域复制
- 回调负载 Schema 注册中心

---

## 开发与发布

当前工程基线请看这些文档：

- [发布清单](docs/engineering/release-checklist.md)
- [Benchmark 清单](docs/engineering/benchmark-checklist.md)
- [配置说明](docs/operations/CONFIGURATION.md)
- [核心模型](docs/architecture/core-model.md)

这些文档是我们描述当前内核和发布方式的源头。

## 贡献指南

欢迎参与贡献！详见 [CONTRIBUTING.md](CONTRIBUTING.md)。

- **问题反馈**：在 [GitHub Issues](https://github.com/loomq/loomq/issues) 提交 Bug 或功能请求
- **代码贡献**：Fork 后在 feature 分支开发，提交 PR 至 `main` 分支

---

## 许可证

```
Copyright 2024 LoomQ Authors

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
