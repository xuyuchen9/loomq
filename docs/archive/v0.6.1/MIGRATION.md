# v0.6.1 迁移指南

## 1. 升级概述

v0.6.1 是一次**非兼容性升级**，主要变更：

- ✅ HTTP API 完全兼容
- ✅ 配置项兼容
- ⚠️ WAL 格式不兼容（需清空 WAL 目录）

---

## 2. 升级步骤

### 2.1 标准升级流程

```bash
# 1. 停止服务
pkill -f loomq  # Linux
# 或通过服务管理器停止

# 2. 备份数据（可选）
cp -r ./data ./data.backup

# 3. 清空 WAL 目录（必须）
rm -rf ./data/wal/*

# 4. 启动新版本
java -Dloomq.engine=v2 --enable-preview -jar loomq-0.6.1.jar
```

### 2.2 Windows 升级流程

```powershell
# 1. 停止服务
# Ctrl+C 或关闭终端

# 2. 备份数据
Copy-Item -Recurse .\data .\data.backup

# 3. 清空 WAL 目录
Remove-Item -Recurse -Force .\data\wal\*

# 4. 启动新版本
java -Dloomq.engine=v2 --enable-preview -jar loomq-0.6.1.jar
```

---

## 3. 兼容性说明

### 3.1 完全兼容

| 项目 | 说明 |
|------|------|
| HTTP API | 所有端点保持不变 |
| 请求/响应格式 | JSON 格式不变 |
| 配置文件 | `application.yml` 完全兼容 |
| Intent 结构 | 业务字段不变 |

### 3.2 部分兼容

| 项目 | 说明 |
|------|------|
| 引擎入口 | 新增 `-Dloomq.engine=v2` 参数 |
| WAL 实现 | `IntentWal` → `IntentWalV2` |

### 3.3 不兼容

| 项目 | 说明 | 处理方式 |
|------|------|----------|
| WAL 文件格式 | 二进制 vs JSON | 清空 WAL 目录升级 |
| WAL 段文件命名 | `wal.bin` vs `00000000.wal` | 清空后重新生成 |

---

## 4. 回滚方案

### 4.1 回滚到 V1

```bash
# 1. 停止 V2 服务
pkill -f loomq

# 2. 清空 WAL（V2 格式不兼容 V1）
rm -rf ./data/wal/*

# 3. 启动 V1 引擎
java --enable-preview -jar loomq-0.6.0.jar
```

### 4.2 从备份恢复

```bash
# 1. 停止服务
pkill -f loomq

# 2. 恢复备份
rm -rf ./data
cp -r ./data.backup ./data

# 3. 启动原版本
java --enable-preview -jar loomq-0.6.0.jar
```

---

## 5. 配置变更

### 5.1 新增配置项

无新增配置项，所有配置保持兼容。

### 5.2 推荐配置

```yaml
# application.yml
loomq:
  engine: v2  # 使用简化引擎（可选，默认 v1）

wal:
  data_dir: ./data/wal
  memory_segment:
    initial_size_mb: 64
    max_size_mb: 1024
    flush_threshold_kb: 64
    flush_interval_ms: 10
```

---

## 6. 监控变更

### 6.1 新增指标

| 指标 | 说明 |
|------|------|
| `loomq_wal_writes_total` | WAL 写入次数 |
| `loomq_wal_flushes_total` | WAL 刷盘次数 |

### 6.2 指标端点

```
GET /metrics
```

输出示例：
```
# HELP loomq_wal_writes_total WAL writes
loomq_wal_writes_total 10000

# HELP loomq_wal_flushes_total WAL flushes
loomq_wal_flushes_total 100
```

---

## 7. 调试工具

### 7.1 WAL 调试

```bash
# 解析 WAL 文件
java --enable-preview -cp loomq.jar \
    com.loomq.wal.WalDumpTool ./data/wal/shard-0/wal.bin --json
```

### 7.2 输出示例

```json
[
  {
    "position": 0,
    "index": 0,
    "payloadLength": 144,
    "crcValid": true,
    "intent": {
      "intentId": "intent_abc123",
      "status": "SCHEDULED",
      "executeAt": "2026-04-10T10:00:00Z",
      "callbackUrl": "http://localhost:8080/callback"
    }
  }
]
```

---

## 8. 常见问题

### Q1: 为什么需要清空 WAL？

**A**: V2 使用二进制格式，与 V1 的 JSON 格式不兼容。升级时需清空 WAL 目录，服务会重新生成。

### Q2: 数据会丢失吗？

**A**: 不会。WAL 只是增量日志，主要用于崩溃恢复。正常关闭服务后，数据已持久化到快照。

### Q3: 如何验证升级成功？

```bash
# 检查健康状态
curl http://localhost:8080/health

# 检查引擎版本
curl http://localhost:8080/v1/cluster/status
# 输出应包含 "engine": "simplified-v0.6.1"
```

### Q4: V1 和 V2 可以同时运行吗？

**A**: 不建议。两者共享相同的数据目录，可能导致冲突。建议完全切换到 V2。

---

## 9. 检查清单

升级前确认：

- [ ] 已停止服务
- [ ] 已备份数据（可选）
- [ ] 已清空 WAL 目录
- [ ] 已下载新版本 JAR
- [ ] 已阅读 CHANGELOG

升级后验证：

- [ ] 服务正常启动
- [ ] 健康检查通过
- [ ] API 调用正常
- [ ] 指标端点正常
