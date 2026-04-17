# LoomQ API 文档

## 基础信息

- **Base URL**: `http://localhost:8080/api/v1`
- **Content-Type**: `application/json`

## 错误码

| 错误码 | 说明 |
|--------|------|
| 0 | 成功 |
| 1 | 参数无效 |
| 2 | 内部错误 |
| 100 | 任务不存在 |
| 101 | 任务已存在 |
| 103 | 任务已终态 |
| 104 | 任务无法取消 |
| 105 | 任务无法修改 |
| 106 | 版本冲突 |
| 108 | 任务已完成 |
| 200 | 幂等键冲突 |
| 400 | WAL 写入错误 |
| 500 | 调度器已满 |
| 600 | Webhook 调用失败 |
| 601 | Webhook 超时 |

## API 端点

### 1. 创建任务

```
POST /api/v1/tasks
```

**请求体**

```json
{
  "webhookUrl": "https://example.com/webhook",
  "delayMs": 60000,
  "wakeTime": null,
  "bizKey": "order-123",
  "idempotencyKey": "req-abc",
  "method": "POST",
  "headers": {
    "X-Custom-Header": "value"
  },
  "payload": {
    "orderId": "123",
    "action": "cancel"
  },
  "maxRetry": 3,
  "timeoutMs": 3000,
  "durable": true
}
```

**参数说明**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| webhookUrl | string | ✅ | 回调地址 |
| delayMs | long | ❌ | 延迟毫秒数（与 wakeTime 二选一） |
| wakeTime | long | ❌ | 计划触发时间戳（与 delayMs 二选一） |
| bizKey | string | ❌ | 业务键，用于业务层检索 |
| idempotencyKey | string | ❌ | 幂等键，用于请求去重 |
| method | string | ❌ | HTTP 方法，默认 POST |
| headers | map | ❌ | 请求头 |
| payload | object | ❌ | 请求体 |
| maxRetry | int | ❌ | 最大重试次数，默认 3 |
| timeoutMs | long | ❌ | 超时毫秒数，默认 3000 |
| durable | boolean | ❌ | 是否持久化，默认 true |

**响应**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "taskId": "task_1704067200000_abc123",
    "status": "PENDING",
    "wakeTime": 1704067260000,
    "isDuplicate": false,
    "isDurable": true
  }
}
```

---

### 2. 查询任务

```
GET /api/v1/tasks/{taskId}
GET /api/v1/tasks?bizKey={bizKey}
GET /api/v1/tasks?idempotencyKey={idempotencyKey}
GET /api/v1/tasks?status={status}
```

**响应**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "taskId": "task_1704067200000_abc123",
    "bizKey": "order-123",
    "idempotencyKey": "req-abc",
    "status": "SUCCESS",
    "wakeTime": 1704067260000,
    "webhookUrl": "https://example.com/webhook",
    "method": "POST",
    "headers": {},
    "payload": "{\"orderId\":\"123\"}",
    "maxRetryCount": 3,
    "retryCount": 0,
    "timeoutMs": 3000,
    "version": 5,
    "createTime": 1704067200000,
    "updateTime": 1704067260100,
    "lastError": null
  }
}
```

**状态查询响应**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "count": 10,
    "tasks": [...]
  }
}
```

---

### 3. 取消任务

```
DELETE /api/v1/tasks/{taskId}
```

**响应**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "taskId": "task_1704067200000_abc123",
    "status": "CANCELLED",
    ...
  }
}
```

**注意事项**

- 只有 PENDING、SCHEDULED、READY、RETRY_WAIT 状态可取消
- RUNNING 状态不可取消（任务可能已在执行）
- 下游服务需实现幂等以处理可能的重复调用

---

### 4. 修改任务

```
PATCH /api/v1/tasks/{taskId}
```

**请求体**

```json
{
  "version": 1,
  "wakeTime": 1704067300000,
  "webhookUrl": "https://example.com/new-webhook",
  "payload": {"orderId": "456"},
  "maxRetryCount": 5,
  "timeoutMs": 5000
}
```

**参数说明**

| 参数 | 类型 | 说明 |
|------|------|------|
| version | long | 乐观锁版本号（可选） |
| wakeTime | long | 新的计划触发时间 |
| webhookUrl | string | 新的回调地址 |
| payload | object | 新的请求体 |
| maxRetryCount | int | 新的最大重试次数 |
| timeoutMs | long | 新的超时时间 |

**响应**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "taskId": "task_1704067200000_abc123",
    "status": "SCHEDULED",
    "version": 2,
    ...
  }
}
```

---

### 5. 立即触发

```
POST /api/v1/tasks/{taskId}/fire-now
```

**响应**

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "taskId": "task_1704067200000_abc123",
    "status": "READY",
    ...
  }
}
```

---

### 6. 健康检查

```
GET /health
```

**响应**

```json
{
  "status": "UP",
  "components": {
    "wal": "UP",
    "scheduler": "UP",
    "store": "UP"
  }
}
```

---

### 7. 监控指标

```
GET /metrics
```

返回 Prometheus 格式的指标。

---

## 状态值

| 状态 | 说明 | 终态 |
|------|------|------|
| PENDING | 等待调度 | ❌ |
| SCHEDULED | 已入调度 | ❌ |
| READY | 已到期 | ❌ |
| RUNNING | 执行中 | ❌ |
| RETRY_WAIT | 等待重试 | ❌ |
| SUCCESS | 成功 | ✅ |
| FAILED | 失败 | ✅ |
| CANCELLED | 已取消 | ✅ |
| EXPIRED | 已过期 | ✅ |
| DEAD_LETTER | 死信 | ✅ |
