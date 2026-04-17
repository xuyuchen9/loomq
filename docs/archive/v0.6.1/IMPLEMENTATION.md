# v0.6.1 实施细节文档

## 1. 新增组件清单

### 1.1 核心组件

| 组件 | 包路径 | 行数 | 说明 |
|------|--------|------|------|
| SimpleWalWriter | `com.loomq.wal` | ~280 | 极简 WAL 写入器，8字节头部 + MemorySegment 零拷贝 |
| IntentBinaryCodec | `com.loomq.wal` | ~420 | 二进制序列化器，~100ns/条 |
| IntentWalV2 | `com.loomq.wal.v2` | ~180 | 简化版 WAL 集成层 |
| BatchDispatcher | `com.loomq.dispatcher` | ~400 | 批量同步投递器 + 背压机制 |
| WalDumpTool | `com.loomq.wal` | ~200 | WAL 调试工具，解析二进制为 JSON |
| IntentHandlerV2 | `com.loomq.http.netty` | ~340 | 简化版 HTTP 处理器 |
| LoomqEngineV2 | `com.loomq` | ~480 | 简化版引擎入口 |

### 1.2 测试组件

| 组件 | 包路径 | 说明 |
|------|--------|------|
| IntentBinaryCodecTest | `com.loomq.wal` | 编解码器单元测试（6 个用例） |
| SimpleWalBenchmark | `com.loomq.wal` | WAL 性能基准测试 |

---

## 2. 代码变更统计

### 2.1 新增文件

```
src/main/java/com/loomq/
├── wal/
│   ├── SimpleWalWriter.java          # +280 行
│   ├── IntentBinaryCodec.java        # +420 行
│   └── WalDumpTool.java              # +200 行
├── wal/v2/
│   └── IntentWalV2.java              # +180 行
├── dispatcher/
│   └── BatchDispatcher.java          # +400 行
├── http/netty/
│   └── IntentHandlerV2.java          # +340 行
└── LoomqEngineV2.java                # +480 行

src/test/java/com/loomq/wal/
├── IntentBinaryCodecTest.java        # +120 行
└── SimpleWalBenchmark.java           # +140 行

总计新增: ~2,560 行
```

### 2.2 修改文件

```
src/main/java/com/loomq/
└── LoomqApplication.java             # 修改：支持 V1/V2 引擎切换
```

### 2.3 代码量对比

| 模块 | V1 实现 | V2 实现 | 变化 |
|------|---------|---------|------|
| WAL 层 | ~1,300 行 | ~880 行 | **-32%** |
| 投递层 | ~450 行 | ~400 行 | **-11%** |
| 引擎入口 | ~475 行 | ~480 行 | +1% |
| **总计** | ~2,225 行 | ~1,760 行 | **-21%** |

---

## 3. 组件详解

### 3.1 SimpleWalWriter

**职责**: 极简 WAL 写入，只做字节搬运

**核心设计**:
- 8字节固定开销（Length 4B + CRC32 4B）
- MemorySegment 零拷贝写入
- 独立刷盘线程，批量摊平 fsync
- StripedCondition 精确唤醒等待者

**关键方法**:
```java
// 异步写入 - 不等待刷盘
CompletableFuture<Long> writeAsync(byte[] payload);

// DURABLE 写入 - 等待刷盘完成
CompletableFuture<Long> writeDurable(byte[] payload);
```

### 3.2 IntentBinaryCodec

**职责**: Intent 二进制序列化/反序列化

**格式设计**:
```
| 字段数量 (1B) |
| 字段类型 (1B) | 字段长度 (4B) | 字段值 (N) | (重复字段数次)
```

**字段类型定义**:
- 0x01: intentId (String)
- 0x02: status (byte ordinal)
- 0x05: executeAt (long epochMillis)
- 0x06: deadline (long epochMillis)
- 0x08: precisionTier (byte ordinal)
- 0x0C: callback (嵌套结构)
- ...

**关键特性**:
- 可扩展：新字段类型可被旧代码跳过
- 紧凑：平均 Intent ~144 字节

### 3.3 BatchDispatcher

**职责**: 批量同步投递 + 背压

**核心流程**:
```
1. submit(Intent) -> 入队（队列满则拒绝）
2. 消费者循环：
   a. drainTo(batch, 100) - 批量取出
   b. 虚拟线程并行发送
   c. allOf(futures).join() - 等待完成
   d. 失败则重调度
```

**配置参数**:
- BATCH_SIZE = 100
- QUEUE_CAPACITY = 10000
- BATCH_TIMEOUT_MS = 30000

### 3.4 LoomqEngineV2

**职责**: 简化版引擎入口

**启动流程**:
```
1. 初始化存储层（快照 + WAL V2）
2. 初始化租约协调器
3. 初始化复制管理器
4. 初始化故障转移控制器
5. 启动调度器
6. 启动投递器
7. 启动 HTTP 服务
```

---

## 4. API 兼容性

### 4.1 HTTP API（完全兼容）

| 端点 | 方法 | 兼容性 |
|------|------|--------|
| `/v1/intents` | POST | ✅ 兼容 |
| `/v1/intents/{id}` | GET | ✅ 兼容 |
| `/v1/intents/{id}` | PATCH | ✅ 兼容 |
| `/v1/intents/{id}/cancel` | POST | ✅ 兼容 |
| `/v1/intents/{id}/fire-now` | POST | ✅ 兼容 |

### 4.2 配置项（完全兼容）

所有 `application.yml` 配置项保持兼容。

### 4.3 不兼容项

| 项目 | 说明 |
|------|------|
| WAL 文件格式 | 二进制格式，不兼容 V1 的 JSON 格式 |
| IntentWal | 推荐使用 IntentWalV2 |

---

## 5. 引擎切换

### 5.1 命令行切换

```bash
# V1 引擎（默认，稳定版）
java --enable-preview -jar loomq.jar

# V2 引擎（简化版，目标 200K+ QPS）
java -Dloomq.engine=v2 --enable-preview -jar loomq.jar
```

### 5.2 代码切换

```java
// V1
LoomqEngine engine = new LoomqEngine(nodeId, shardId, dataDir, port);

// V2
LoomqEngineV2 engine = new LoomqEngineV2(nodeId, shardId, dataDir, port);
```

---

## 6. 测试覆盖

### 6.1 单元测试

| 测试类 | 用例数 | 状态 |
|--------|--------|------|
| IntentBinaryCodecTest | 6 | ✅ 全部通过 |

### 6.2 测试用例详情

```
testEncodeDecode_basicIntent        ✅
testEncodeDecode_withCallback       ✅
testEncodeDecode_withTags           ✅
testEncodeDecode_allPrecisionTiers  ✅
testEncode_sizeEstimate             ✅ (144 bytes)
testEncodeDecode_multipleAttempts   ✅
```

---

## 7. 调试工具

### 7.1 WalDumpTool

**用途**: 将二进制 WAL 文件解析为可读 JSON

**用法**:
```bash
# 基本用法
java --enable-preview -cp loomq.jar com.loomq.wal.WalDumpTool ./data/wal/shard-0/wal.bin

# JSON 输出
java --enable-preview -cp loomq.jar com.loomq.wal.WalDumpTool ./data/wal/shard-0/wal.bin --json

# 限制记录数
java --enable-preview -cp loomq.jar com.loomq.wal.WalDumpTool ./data/wal/shard-0/wal.bin --limit 100
```

**输出示例**:
```
Record #0 @ 0
  Payload length: 144
  CRC valid: true
  Intent ID: intent_abc123
  Status: SCHEDULED
  Execute At: 2026-04-10T10:00:00Z
  Callback URL: http://localhost:8080/callback
```
