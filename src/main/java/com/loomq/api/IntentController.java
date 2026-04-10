package com.loomq.api;

import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
import com.loomq.entity.v5.PrecisionTier;
import com.loomq.replication.AckLevel;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

import io.javalin.Javalin;

/**
 * Intent API Controller (v0.5)
 *
 * @author loomq
 * @since v0.5.0
 */
public class IntentController {

    private static final Logger logger = LoggerFactory.getLogger(IntentController.class);

    private final IntentStore intentStore;

    public IntentController(IntentStore intentStore) {
        this.intentStore = intentStore;
    }

    /**
     * 注册路由到 Javalin 应用
     */
    public void register(Javalin app) {
        app.post("/v1/intents", this::createIntent);
        app.get("/v1/intents/{intentId}", this::getIntent);
        app.patch("/v1/intents/{intentId}", this::patchIntent);
        app.post("/v1/intents/{intentId}/cancel", this::cancelIntent);
        app.post("/v1/intents/{intentId}/fire-now", this::fireNow);
    }

    /**
     * POST /v1/intents - 创建 Intent
     *
     * 幂等规则：
     * - 同一 idempotencyKey 在窗口期内重复请求：
     *   - 非终态 → 200 OK 返回已存在 Intent
     *   - 终态 → 409 Conflict
     * - 窗口期外视为新业务请求
     */
    public void createIntent(Context ctx) {
        try {
            CreateIntentRequest request = ctx.bodyAsClass(CreateIntentRequest.class);

            // 验证必填字段
            if (request.executeAt() == null) {
                ctx.status(HttpStatus.UNPROCESSABLE_CONTENT)
                   .json(ErrorResponse.of("42201", "executeAt is required"));
                return;
            }

            if (request.deadline() == null) {
                ctx.status(HttpStatus.UNPROCESSABLE_CONTENT)
                   .json(ErrorResponse.of("42202", "deadline is required"));
                return;
            }

            if (request.deadline().isBefore(request.executeAt())) {
                ctx.status(HttpStatus.UNPROCESSABLE_CONTENT)
                   .json(ErrorResponse.of("42202", "deadline must be after executeAt"));
                return;
            }

            if (request.executeAt().isBefore(Instant.now())) {
                ctx.status(HttpStatus.UNPROCESSABLE_CONTENT)
                   .json(ErrorResponse.of("42201", "executeAt must be in the future"));
                return;
            }

            if (request.shardKey() == null || request.shardKey().isBlank()) {
                ctx.status(HttpStatus.UNPROCESSABLE_CONTENT)
                   .json(ErrorResponse.of("42203", "shardKey is required"));
                return;
            }

            // 检查幂等性
            if (request.idempotencyKey() != null && !request.idempotencyKey().isBlank()) {
                IdempotencyResult idemResult = intentStore.checkIdempotency(request.idempotencyKey());

                if (idemResult.isDuplicateActive()) {
                    // 非终态重复请求 → 200 OK
                    logger.info("Duplicate active intent found for key={}, returning existing intent={}",
                        request.idempotencyKey(), idemResult.getIntent().getIntentId());
                    ctx.status(HttpStatus.OK)
                       .json(IntentResponse.from(idemResult.getIntent()));
                    return;
                }

                if (idemResult.isDuplicateTerminal()) {
                    // 终态重复请求 → 409 Conflict
                    logger.warn("Duplicate terminal intent for key={}, reject with 409",
                        request.idempotencyKey());
                    ctx.status(HttpStatus.CONFLICT)
                       .json(ErrorResponse.of("40901",
                           "Idempotency key conflict: intent already in terminal state",
                           Map.of("intentId", idemResult.getIntent().getIntentId())));
                    return;
                }
            }

            // 创建 Intent
            Intent intent = new Intent(request.intentId());
            intent.setExecuteAt(request.executeAt());
            intent.setDeadline(request.deadline());
            intent.setExpiredAction(request.expiredAction());
            intent.setPrecisionTier(request.precisionTier() != null ? request.precisionTier() : PrecisionTier.STANDARD);
            intent.setShardKey(request.shardKey());
            intent.setAckLevel(request.ackLevel() != null ? request.ackLevel() : AckLevel.DURABLE);
            intent.setCallback(request.callback());
            intent.setRedelivery(request.redelivery());
            intent.setIdempotencyKey(request.idempotencyKey());
            intent.setTags(request.tags());

            // 状态转换: CREATED -> SCHEDULED
            intent.transitionTo(IntentStatus.SCHEDULED);

            // 保存 Intent
            intentStore.save(intent);

            logger.info("Intent created: id={}, executeAt={}, ackLevel={}",
                intent.getIntentId(), intent.getExecuteAt(), intent.getAckLevel());

            ctx.status(HttpStatus.CREATED)
               .json(IntentResponse.from(intent));

        } catch (Exception e) {
            logger.error("Failed to create intent", e);
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR)
               .json(ErrorResponse.of("50001", "Internal server error"));
        }
    }

    /**
     * GET /v1/intents/{intentId} - 查询 Intent
     */
    public void getIntent(Context ctx) {
        String intentId = ctx.pathParam("intentId");

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            ctx.status(HttpStatus.NOT_FOUND)
               .json(ErrorResponse.of("40401", "Intent not found: " + intentId));
            return;
        }

        ctx.json(IntentResponse.from(intent));
    }

    /**
     * POST /v1/intents/{intentId}/cancel - 取消 Intent
     */
    public void cancelIntent(Context ctx) {
        String intentId = ctx.pathParam("intentId");

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            ctx.status(HttpStatus.NOT_FOUND)
               .json(ErrorResponse.of("40401", "Intent not found: " + intentId));
            return;
        }

        if (!intent.getStatus().isCancellable()) {
            ctx.status(HttpStatus.UNPROCESSABLE_CONTENT)
               .json(ErrorResponse.of("42204",
                   "Intent cannot be cancelled in state: " + intent.getStatus()));
            return;
        }

        intent.transitionTo(IntentStatus.CANCELED);
        intentStore.update(intent);

        ctx.json(IntentResponse.from(intent));
    }

    /**
     * POST /v1/intents/{intentId}/fire-now - 立即触发
     *
     * 如果 Lease 过期，返回 307 重定向
     */
    public void fireNow(Context ctx) {
        String intentId = ctx.pathParam("intentId");

        // TODO: 检查当前节点是否为 Primary，如果不是返回 307
        // if (!isPrimary()) {
        //     String newPrimary = getCurrentPrimary();
        //     ctx.status(HttpStatus.TEMPORARY_REDIRECT)
        //        .header("Location", "http://" + newPrimary + "/v1/intents/" + intentId + "/fire-now")
        //        .json(ErrorResponse.fencingExpired(intentId, newPrimary));
        //     return;
        // }

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            ctx.status(HttpStatus.NOT_FOUND)
               .json(ErrorResponse.of("40401", "Intent not found: " + intentId));
            return;
        }

        // 将执行时间改为现在
        intent.setExecuteAt(Instant.now());
        intent.transitionTo(IntentStatus.DUE);
        intentStore.update(intent);

        ctx.json(Map.of(
            "intentId", intentId,
            "status", IntentStatus.DISPATCHING.name()
        ));
    }

    /**
     * PATCH /v1/intents/{intentId} - 修改 Intent
     */
    public void patchIntent(Context ctx) {
        String intentId = ctx.pathParam("intentId");

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            ctx.status(HttpStatus.NOT_FOUND)
               .json(ErrorResponse.of("40401", "Intent not found: " + intentId));
            return;
        }

        if (!intent.getStatus().isModifiable()) {
            ctx.status(HttpStatus.UNPROCESSABLE_CONTENT)
               .json(ErrorResponse.of("42205",
                   "Intent cannot be modified in state: " + intent.getStatus()));
            return;
        }

        PatchIntentRequest request = ctx.bodyAsClass(PatchIntentRequest.class);

        if (request.executeAt() != null) {
            intent.setExecuteAt(request.executeAt());
        }
        if (request.deadline() != null) {
            intent.setDeadline(request.deadline());
        }
        if (request.expiredAction() != null) {
            intent.setExpiredAction(request.expiredAction());
        }
        if (request.redelivery() != null) {
            intent.setRedelivery(request.redelivery());
        }
        if (request.tags() != null) {
            intent.setTags(request.tags());
        }

        intentStore.update(intent);
        ctx.json(IntentResponse.from(intent));
    }
}
