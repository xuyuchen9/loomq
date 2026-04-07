package com.loomq.gateway.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loomq.common.ErrorCode;
import com.loomq.common.IdGenerator;
import com.loomq.common.MetricsCollector;
import com.loomq.common.Result;
import com.loomq.entity.v3.EventTypeV3;
import com.loomq.entity.v3.TaskStatusV3;
import com.loomq.entity.v3.TaskV3;
import com.loomq.retry.RetryPolicy;
import com.loomq.retry.RetryPolicyFactory;
import com.loomq.store.v3.IdempotencyResult;
import com.loomq.store.v3.TaskStoreV3;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 任务 API 控制器 V3
 *
 * 完整实现需求文档定义的 HTTP API。
 *
 * @author loomq
 * @since v0.4
 */
public class TaskControllerV3 {

    private static final Logger logger = LoggerFactory.getLogger(TaskControllerV3.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

    private final TaskStoreV3 taskStore;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RetryPolicy defaultRetryPolicy;

    public TaskControllerV3(TaskStoreV3 taskStore) {
        this.taskStore = taskStore;
        this.defaultRetryPolicy = RetryPolicyFactory.RetryConfig.defaultConfig().toRetryPolicy();
    }

    // ========== 任务创建 ==========

    /**
     * 创建任务
     * POST /api/v1/tasks
     */
    public void createTask(Context ctx) {
        try {
            CreateTaskRequest request = ctx.bodyAsClass(CreateTaskRequest.class);

            // 参数校验
            ValidationResult validation = validateCreateRequest(request);
            if (!validation.valid()) {
                ctx.json(Result.error(ErrorCode.INVALID_PARAM, validation.message()));
                return;
            }

            // 计算唤醒时间
            long wakeTime = calculateWakeTime(request);

            // 幂等检查（使用 idempotencyKey）
            if (request.idempotencyKey() != null && !request.idempotencyKey().isEmpty()) {
                IdempotencyResult result = taskStore.getByIdempotencyKey(request.idempotencyKey());
                if (result.exists()) {
                    if (result.isTerminal()) {
                        ctx.json(Result.error(ErrorCode.TASK_ALREADY_COMPLETED,
                                "Task already completed with status: " + result.getTask().getStatus()));
                        return;
                    }
                    // 返回已存在的活跃任务
                    ctx.json(Result.success(new CreateTaskResponse(
                            result.getTask().getTaskId(),
                            result.getTask().getStatus().name(),
                            result.getTask().getWakeTime(),
                            true,  // isDuplicate
                            false  // isDurable (从现有任务获取)
                    )));
                    return;
                }
            }

            // bizKey 幂等检查
            if (request.bizKey() != null && !request.bizKey().isEmpty()) {
                TaskV3 existing = taskStore.getByBizKey(request.bizKey());
                if (existing != null) {
                    if (existing.getStatus().isTerminal()) {
                        ctx.json(Result.error(ErrorCode.TASK_ALREADY_COMPLETED,
                                "Task with bizKey already completed with status: " + existing.getStatus()));
                        return;
                    }
                    ctx.json(Result.success(new CreateTaskResponse(
                            existing.getTaskId(),
                            existing.getStatus().name(),
                            existing.getWakeTime(),
                            true,
                            existing.isDurable()
                    )));
                    return;
                }
            }

            // 生成任务ID
            String taskId = IdGenerator.generateTaskId();

            // 创建任务
            TaskV3 task = TaskV3.builder()
                    .taskId(taskId)
                    .bizKey(request.bizKey())
                    .idempotencyKey(request.idempotencyKey())
                    .webhookUrl(request.webhookUrl())
                    .method(request.method() != null ? request.method() : "POST")
                    .headers(request.headers())
                    .payload(request.payload() != null ? toJson(request.payload()) : null)
                    .delay(request.delayMs() != null ? request.delayMs() : 0)
                    .wakeTime(wakeTime)
                    .timeout(request.timeoutMs() != null ? request.timeoutMs() : 3000)
                    .maxRetryCount(request.maxRetry() != null ? request.maxRetry() : 3)
                    .durable(request.durable() != null ? request.durable() : true)
                    .createTime(System.currentTimeMillis())
                    .build();

            // 存储任务
            taskStore.add(task);

            // 调度任务（实际调度逻辑由外部引擎处理）
            // scheduler.schedule(task);

            // 记录审计日志
            auditLogger.info("TASK_CREATE|{}|{}|{}|{}", taskId, request.bizKey(), request.webhookUrl(), wakeTime);

            // 记录指标
            MetricsCollector.getInstance().incrementTasksCreated();

            ctx.status(201).json(Result.success(new CreateTaskResponse(
                    taskId,
                    task.getStatus().name(),
                    wakeTime,
                    false,  // isDuplicate
                    task.isDurable()
            )));

        } catch (Exception e) {
            logger.error("Failed to create task", e);
            ctx.json(Result.error(ErrorCode.INTERNAL_ERROR, e.getMessage()));
        }
    }

    // ========== 任务查询 ==========

    /**
     * 查询任务（支持多种查询方式）
     * GET /api/v1/tasks/{taskId}
     * GET /api/v1/tasks?bizKey={bizKey}
     * GET /api/v1/tasks?idempotencyKey={idempotencyKey}
     * GET /api/v1/tasks?status={status}
     */
    public void getTask(Context ctx) {
        // 尝试从路径参数获取 taskId
        String taskId = ctx.pathParam("taskId");

        if (taskId != null && !taskId.isEmpty()) {
            // 按 taskId 查询
            TaskV3 task = taskStore.get(taskId);
            if (task == null) {
                ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
                return;
            }
            ctx.json(Result.success(toTaskResponse(task)));
            return;
        }

        // 检查查询参数
        String bizKey = ctx.queryParam("bizKey");
        String idempotencyKey = ctx.queryParam("idempotencyKey");
        String statusStr = ctx.queryParam("status");

        if (bizKey != null && !bizKey.isEmpty()) {
            // 按 bizKey 查询
            TaskV3 task = taskStore.getByBizKey(bizKey);
            if (task == null) {
                ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND, "Task not found with bizKey: " + bizKey));
                return;
            }
            ctx.json(Result.success(toTaskResponse(task)));
            return;
        }

        if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
            // 按 idempotencyKey 查询
            IdempotencyResult result = taskStore.getByIdempotencyKey(idempotencyKey);
            if (result.isNotFound()) {
                ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND, "Task not found with idempotencyKey: " + idempotencyKey));
                return;
            }
            ctx.json(Result.success(toTaskResponse(result.getTask())));
            return;
        }

        if (statusStr != null && !statusStr.isEmpty()) {
            // 按状态查询
            try {
                TaskStatusV3 status = TaskStatusV3.valueOf(statusStr.toUpperCase());
                List<TaskV3> tasks = taskStore.getByStatus(status);
                List<TaskResponse> responses = tasks.stream()
                        .map(this::toTaskResponse)
                        .collect(Collectors.toList());
                ctx.json(Result.success(new TaskListResponse(responses.size(), responses)));
                return;
            } catch (IllegalArgumentException e) {
                ctx.json(Result.error(ErrorCode.INVALID_PARAM, "Invalid status: " + statusStr));
                return;
            }
        }

        ctx.json(Result.error(ErrorCode.INVALID_PARAM,
                "Please provide taskId, bizKey, idempotencyKey, or status"));
    }

    // ========== 任务取消 ==========

    /**
     * 取消任务
     * DELETE /api/v1/tasks/{taskId}
     */
    public void cancelTask(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        TaskV3 task = taskStore.get(taskId);
        if (task == null) {
            ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
            return;
        }

        // 尝试取消
        if (!task.cancel()) {
            ctx.json(Result.error(ErrorCode.TASK_CANNOT_CANCEL,
                    "Task status is " + task.getStatus() + " and cannot be cancelled."));
            return;
        }

        task.incrementVersion();
        taskStore.update(task);

        // 记录审计日志
        auditLogger.info("TASK_CANCEL|{}|{}", taskId, task.getBizKey());

        // 记录指标
        MetricsCollector.getInstance().incrementTasksCancelled();

        ctx.json(Result.success(toTaskResponse(task)));
    }

    // ========== 任务修改 ==========

    /**
     * 修改任务
     * PATCH /api/v1/tasks/{taskId}
     */
    public void modifyTask(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        TaskV3 task = taskStore.get(taskId);
        if (task == null) {
            ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
            return;
        }

        if (!task.getStatus().isModifiable()) {
            ctx.json(Result.error(ErrorCode.TASK_CANNOT_MODIFY,
                    "Task status is " + task.getStatus() + " and cannot be modified"));
            return;
        }

        try {
            ModifyTaskRequest request = ctx.bodyAsClass(ModifyTaskRequest.class);

            // 版本校验
            if (request.version() != null && request.version() != task.getVersion()) {
                ctx.json(Result.error(ErrorCode.VERSION_CONFLICT,
                        "Version conflict: expected " + task.getVersion() + ", got " + request.version()));
                return;
            }

            // 应用修改
            if (request.wakeTime() != null) {
                task.setWakeTime(request.wakeTime());
            }
            if (request.webhookUrl() != null) {
                task.setWebhookUrl(request.webhookUrl());
            }
            if (request.payload() != null) {
                task.setPayload(toJson(request.payload()));
            }
            if (request.maxRetryCount() != null) {
                task.setMaxRetryCount(request.maxRetryCount());
            }
            if (request.timeoutMs() != null) {
                task.setTimeout(request.timeoutMs());
            }

            task.incrementVersion();
            taskStore.update(task);

            auditLogger.info("TASK_MODIFY|{}|{}", taskId, task.getBizKey());

            ctx.json(Result.success(toTaskResponse(task)));

        } catch (Exception e) {
            logger.error("Failed to modify task: {}", taskId, e);
            ctx.json(Result.error(ErrorCode.INTERNAL_ERROR, e.getMessage()));
        }
    }

    // ========== 立即触发 ==========

    /**
     * 立即触发任务
     * POST /api/v1/tasks/{taskId}/fire-now
     */
    public void fireNow(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        TaskV3 task = taskStore.get(taskId);
        if (task == null) {
            ctx.json(Result.error(ErrorCode.TASK_NOT_FOUND));
            return;
        }

        if (task.getStatus().isTerminal()) {
            ctx.json(Result.error(ErrorCode.TASK_ALREADY_TERMINATED,
                    "Task is already in terminal state: " + task.getStatus()));
            return;
        }

        // 设置立即执行
        task.setWakeTime(System.currentTimeMillis());
        task.transitionToReady();

        auditLogger.info("TASK_FIRE_NOW|{}|{}", taskId, task.getBizKey());

        ctx.json(Result.success(toTaskResponse(task)));
    }

    // ========== 辅助方法 ==========

    private ValidationResult validateCreateRequest(CreateTaskRequest request) {
        if (request.webhookUrl() == null || request.webhookUrl().isEmpty()) {
            return new ValidationResult(false, "webhookUrl is required");
        }

        if (request.delayMs() != null && request.delayMs() < 0) {
            return new ValidationResult(false, "delayMs cannot be negative");
        }

        if (request.wakeTime() != null && request.wakeTime() < System.currentTimeMillis()) {
            return new ValidationResult(false, "wakeTime cannot be in the past");
        }

        if (request.maxRetry() != null && request.maxRetry() < 0) {
            return new ValidationResult(false, "maxRetry cannot be negative");
        }

        if (request.timeoutMs() != null && request.timeoutMs() < 0) {
            return new ValidationResult(false, "timeoutMs cannot be negative");
        }

        if (request.idempotencyKey() != null && request.idempotencyKey().isEmpty()) {
            return new ValidationResult(false, "idempotencyKey cannot be empty string");
        }

        if (request.bizKey() != null && request.bizKey().isEmpty()) {
            return new ValidationResult(false, "bizKey cannot be empty string");
        }

        return new ValidationResult(true, null);
    }

    private long calculateWakeTime(CreateTaskRequest request) {
        if (request.wakeTime() != null && request.wakeTime() > 0) {
            return request.wakeTime();
        }
        if (request.delayMs() != null && request.delayMs() > 0) {
            return System.currentTimeMillis() + request.delayMs();
        }
        return System.currentTimeMillis();
    }

    private TaskResponse toTaskResponse(TaskV3 task) {
        return new TaskResponse(
                task.getTaskId(),
                task.getBizKey(),
                task.getIdempotencyKey(),
                task.getStatus().name(),
                task.getWakeTime(),
                task.getWebhookUrl(),
                task.getMethod(),
                task.getHeaders(),
                task.getPayload(),
                task.getMaxRetryCount(),
                task.getRetryCount(),
                task.getTimeout(),
                task.getVersion(),
                task.getCreateTime(),
                task.getUpdateTime(),
                task.getLastError()
        );
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return obj != null ? obj.toString() : null;
        }
    }

    // ========== DTOs ==========

    public record CreateTaskRequest(
            String bizKey,
            String idempotencyKey,
            Long delayMs,
            Long wakeTime,
            String webhookUrl,
            String method,
            Map<String, String> headers,
            Object payload,
            Integer maxRetry,
            Long timeoutMs,
            Boolean durable
    ) {}

    public record ModifyTaskRequest(
            Long version,
            Long wakeTime,
            String webhookUrl,
            Object payload,
            Integer maxRetryCount,
            Long timeoutMs
    ) {}

    public record CreateTaskResponse(
            String taskId,
            String status,
            long wakeTime,
            boolean isDuplicate,
            boolean isDurable
    ) {}

    public record TaskResponse(
            String taskId,
            String bizKey,
            String idempotencyKey,
            String status,
            long wakeTime,
            String webhookUrl,
            String method,
            Map<String, String> headers,
            String payload,
            int maxRetryCount,
            int retryCount,
            long timeoutMs,
            long version,
            long createTime,
            long updateTime,
            String lastError
    ) {}

    public record TaskListResponse(
            int count,
            List<TaskResponse> tasks
    ) {}

    public record ValidationResult(boolean valid, String message) {}
}
