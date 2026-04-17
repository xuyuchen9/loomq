# LoomQ 配置文档

## 配置文件

配置文件位于 `src/main/resources/application.yml`。

## 服务器配置

```yaml
loomq:
  server:
    host: "0.0.0.0"      # 监听地址
    port: 8080           # 监听端口
```

## WAL 配置

```yaml
loomq:
  wal:
    data-dir: "./data/wal"           # WAL 数据目录
    segment-size-mb: 64              # 单个段文件大小
    flush-strategy: "async"          # 刷盘策略: per_record | batch | async
    batch-flush-interval-ms: 100     # 批量刷盘间隔
    checkpoint-interval: 100000      # 检查点间隔（记录数）
```

### 刷盘策略说明

| 策略 | 说明 | RPO | 性能 |
|------|------|-----|------|
| `per_record` | 每条记录 fsync | 0 | 低 |
| `batch` | 批量 fsync | <100ms | 中 |
| `async` | 异步刷盘 | <1s | 高 |

## 调度器配置

```yaml
loomq:
  scheduler:
    bucket-granularity-ms: 100      # 时间桶粒度（毫秒）
    max-pending-tasks: 1000000      # 最大待处理任务数
```

### 时间桶粒度说明

| 粒度 | 吞吐量 | 精度 | 适用场景 |
|------|--------|------|----------|
| 1ms | ~50K QPS | ±1ms | 高精度场景 |
| 10ms | ~200K QPS | ±10ms | 中等精度 |
| 100ms | ~600K QPS | ±100ms | 通用场景（默认） |
| 1000ms | ~800K QPS | ±1s | 大批量低精度 |

## 重试配置

```yaml
loomq:
  retry:
    policy: "fixed"                 # 策略: fixed | exponential
    max-retries: 3                  # 最大重试次数
    fixed-interval-ms: 5000         # 固定间隔（fixed 策略）
    initial-delay-ms: 1000          # 初始延迟（exponential 策略）
    base: 2.0                       # 指数基数（exponential 策略）
    max-delay-ms: 60000             # 最大延迟（exponential 策略）
```

### 重试策略说明

**固定间隔 (fixed)**
```
重试延迟 = fixed-interval-ms
例: 5s, 5s, 5s, ...
```

**指数退避 (exponential)**
```
重试延迟 = min(initial-delay-ms * base^(n-1), max-delay-ms)
例: 1s, 2s, 4s, 8s, 16s, 32s, 60s (达到上限)
```

## 分发配置

```yaml
loomq:
  dispatcher:
    http-timeout-ms: 3000           # HTTP 超时时间
    max-concurrent-dispatches: 1000 # 最大并发分发音
```

## 恢复配置

```yaml
loomq:
  recovery:
    batch-size: 1000                # 恢复批处理大小
    concurrency-limit: 100          # 恢复并发限制
    safe-mode: false                # 安全模式（详细日志）
```

## 集群配置

```yaml
loomq:
  cluster:
    enabled: false                  # 是否启用集群模式
    shard-count: 4                  # 分片数量
    heartbeat-interval-ms: 5000     # 心跳间隔
    failure-timeout-ms: 15000       # 故障超时
    virtual-nodes: 150              # 虚拟节点数（一致性哈希）
```

### 集群启动参数

```bash
# 节点 1
java -Dloomq.shard.id=0 \
     -Dloomq.shard.count=2 \
     -Dloomq.cluster.enabled=true \
     --enable-preview -jar loomq.jar

# 节点 2
java -Dloomq.shard.id=1 \
     -Dloomq.shard.count=2 \
     -Dloomq.cluster.enabled=true \
     --enable-preview -jar loomq.jar
```

## 环境变量覆盖

所有配置都可通过环境变量覆盖：

```bash
# 格式: LOOMQ_<SECTION>_<KEY>
export LOOMQ_SERVER_PORT=9090
export LOOMQ_WAL_DATA_DIR=/data/wal
export LOOMQ_RETRY_POLICY=exponential
```

## 完整配置示例

```yaml
loomq:
  server:
    host: "0.0.0.0"
    port: 8080

  wal:
    data-dir: "./data/wal"
    segment-size-mb: 64
    flush-strategy: "async"
    batch-flush-interval-ms: 100
    checkpoint-interval: 100000

  scheduler:
    bucket-granularity-ms: 100
    max-pending-tasks: 1000000

  dispatcher:
    http-timeout-ms: 3000
    max-concurrent-dispatches: 1000

  retry:
    policy: "fixed"
    max-retries: 3
    fixed-interval-ms: 5000
    initial-delay-ms: 1000
    base: 2.0
    max-delay-ms: 60000

  recovery:
    batch-size: 1000
    concurrency-limit: 100
    safe-mode: false

  cluster:
    enabled: false
    shard-count: 4
    heartbeat-interval-ms: 5000
    failure-timeout-ms: 15000
    virtual-nodes: 150
```
