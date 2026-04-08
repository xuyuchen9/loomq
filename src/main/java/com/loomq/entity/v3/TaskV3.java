package com.loomq.entity.v3;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 任务实体 V3
 *
 * 完整任务数据模型，对齐需求文档字段定义。
 *
 * @author loomq
 * @since v0.4
 */
public class TaskV3 {

    // ========== 核心标识 ==========

    /**
     * 任务ID，全局唯一
     */
    private String taskId;

    /**
     * 业务键，用于业务层检索
     */
    private String bizKey;

    /**
     * 幂等键，用于请求去重
     */
    private String idempotencyKey;

    // ========== 执行参数 ==========

    /**
     * 回调地址
     */
    private String webhookUrl;

    /**
     * HTTP 方法
     */
    private String method;

    /**
     * 请求头
     */
    private Map<String, String> headers;

    /**
     * 请求体（JSON字符串）
     */
    private String payload;

    // ========== 调度参数 ==========

    /**
     * 延迟时间（毫秒）
     */
    private long delay;

    /**
     * 计划触发时间（毫秒时间戳）
     */
    private long wakeTime;

    /**
     * 超时时间（毫秒）
     */
    private long timeout;

    // ========== 重试参数 ==========

    /**
     * 当前重试次数
     */
    private int retryCount;

    /**
     * 最大重试次数
     */
    private int maxRetryCount;

    // ========== 状态信息 ==========

    /**
     * 生命周期管理器
     */
    private TaskLifecycleV3 lifecycle;

    /**
     * 乐观锁版本号
     */
    private long version;

    // ========== 时间戳 ==========

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 更新时间
     */
    private long updateTime;

    /**
     * 最后错误信息
     */
    private String lastError;

    // ========== 集群相关 ==========

    /**
     * 分片键
     */
    private String shardKey;

    /**
     * 分片ID
     */
    private int shardId;

    /**
     * 是否持久化
     */
    private boolean durable;

    // ========== 运行时字段（不持久化） ==========

    /**
     * 实际唤醒时间
     */
    private transient long actualWakeTime;

    /**
     * 执行开始时间
     */
    private transient long executionStartTime;

    public TaskV3() {
        this.headers = new HashMap<>();
        this.method = "POST";
        this.maxRetryCount = 3;
        this.retryCount = 0;
        this.timeout = 3000;
        this.version = 1;
        this.durable = true;
    }

    // ========== Builder Pattern ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TaskV3 task = new TaskV3();

        public Builder taskId(String taskId) {
            task.taskId = taskId;
            return this;
        }

        public Builder bizKey(String bizKey) {
            task.bizKey = bizKey;
            return this;
        }

        public Builder idempotencyKey(String idempotencyKey) {
            task.idempotencyKey = idempotencyKey;
            return this;
        }

        public Builder webhookUrl(String webhookUrl) {
            task.webhookUrl = webhookUrl;
            return this;
        }

        public Builder method(String method) {
            task.method = method;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            task.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
            return this;
        }

        public Builder header(String key, String value) {
            task.headers.put(key, value);
            return this;
        }

        public Builder payload(String payload) {
            task.payload = payload;
            return this;
        }

        public Builder delay(long delay) {
            task.delay = delay;
            return this;
        }

        public Builder wakeTime(long wakeTime) {
            task.wakeTime = wakeTime;
            return this;
        }

        public Builder timeout(long timeout) {
            task.timeout = timeout;
            return this;
        }

        public Builder maxRetryCount(int maxRetryCount) {
            task.maxRetryCount = maxRetryCount;
            return this;
        }

        public Builder retryCount(int retryCount) {
            task.retryCount = retryCount;
            return this;
        }

        public Builder version(long version) {
            task.version = version;
            return this;
        }

        public Builder createTime(long createTime) {
            task.createTime = createTime;
            return this;
        }

        public Builder updateTime(long updateTime) {
            task.updateTime = updateTime;
            return this;
        }

        public Builder lastError(String lastError) {
            task.lastError = lastError;
            return this;
        }

        public Builder shardKey(String shardKey) {
            task.shardKey = shardKey;
            return this;
        }

        public Builder shardId(int shardId) {
            task.shardId = shardId;
            return this;
        }

        public Builder durable(boolean durable) {
            task.durable = durable;
            return this;
        }

        public TaskV3 build() {
            Objects.requireNonNull(task.taskId, "taskId is required");
            Objects.requireNonNull(task.webhookUrl, "webhookUrl is required");

            if (task.createTime == 0) {
                task.createTime = System.currentTimeMillis();
            }
            if (task.updateTime == 0) {
                task.updateTime = task.createTime;
            }
            if (task.wakeTime == 0 && task.delay > 0) {
                task.wakeTime = task.createTime + task.delay;
            }

            // 创建生命周期管理器
            task.lifecycle = new TaskLifecycleV3(task.taskId);

            return task;
        }
    }

    // ========== 状态代理方法 ==========

    public TaskStatusV3 getStatus() {
        return lifecycle != null ? lifecycle.getStatus() : TaskStatusV3.PENDING;
    }

    public boolean transitionToScheduled() {
        return lifecycle != null && lifecycle.transitionToScheduled();
    }

    public boolean transitionToReady() {
        return lifecycle != null && lifecycle.transitionToReady();
    }

    public boolean transitionToRunning() {
        return lifecycle != null && lifecycle.transitionToRunning();
    }

    public boolean transitionToSuccess() {
        return lifecycle != null && lifecycle.transitionToSuccess();
    }

    public boolean transitionToRetryWait() {
        return lifecycle != null && lifecycle.transitionToRetryWait(maxRetryCount);
    }

    public boolean transitionToFailed(String error) {
        boolean result = lifecycle != null && lifecycle.transitionToFailed(error);
        if (result) {
            this.lastError = error;
        }
        return result;
    }

    public boolean transitionToDeadLetter(String error) {
        boolean result = lifecycle != null && lifecycle.transitionToDeadLetter(error);
        if (result) {
            this.lastError = error;
        }
        return result;
    }

    public boolean cancel() {
        return lifecycle != null && lifecycle.cancel();
    }

    public boolean expire() {
        return lifecycle != null && lifecycle.expire();
    }

    public boolean canRetry() {
        return retryCount < maxRetryCount && !getStatus().isTerminal();
    }

    public boolean isTerminal() {
        return getStatus().isTerminal();
    }

    public boolean isCancellable() {
        return getStatus().isCancellable();
    }

    // ========== Getters and Setters ==========

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getBizKey() {
        return bizKey;
    }

    public void setBizKey(String bizKey) {
        this.bizKey = bizKey;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getWebhookUrl() {
        return webhookUrl;
    }

    public void setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public long getWakeTime() {
        return wakeTime;
    }

    public void setWakeTime(long wakeTime) {
        this.wakeTime = wakeTime;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public TaskLifecycleV3 getLifecycle() {
        return lifecycle;
    }

    public void setLifecycle(TaskLifecycleV3 lifecycle) {
        this.lifecycle = lifecycle;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public String getShardKey() {
        return shardKey;
    }

    public void setShardKey(String shardKey) {
        this.shardKey = shardKey;
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public long getActualWakeTime() {
        return actualWakeTime;
    }

    public void setActualWakeTime(long actualWakeTime) {
        this.actualWakeTime = actualWakeTime;
    }

    public long getExecutionStartTime() {
        return executionStartTime;
    }

    public void setExecutionStartTime(long executionStartTime) {
        this.executionStartTime = executionStartTime;
    }

    public void incrementVersion() {
        this.version++;
        this.updateTime = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "TaskV3{" +
                "taskId='" + taskId + '\'' +
                ", bizKey='" + bizKey + '\'' +
                ", status=" + getStatus() +
                ", wakeTime=" + wakeTime +
                ", webhookUrl='" + webhookUrl + '\'' +
                ", retryCount=" + retryCount +
                ", version=" + version +
                '}';
    }
}
