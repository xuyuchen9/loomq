package com.loomq.grpc;

import com.loomq.LoomqEngine;
import com.loomq.common.IntentValidator;
import com.loomq.common.ValidationResult;
import com.loomq.application.scheduler.TierAdvisor;
import com.loomq.common.exception.BackPressureException;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.Reliability;
import com.loomq.grpc.converter.GrpcStatusAdapter;
import com.loomq.grpc.converter.ProtoConverter;
import com.loomq.grpc.gen.CreateIntentRequest;
import com.loomq.grpc.gen.GetIntentRequest;
import com.loomq.grpc.gen.HealthCheckRequest;
import com.loomq.grpc.gen.HealthCheckResponse;
import com.loomq.grpc.gen.IntentActionResponse;
import com.loomq.grpc.gen.IntentEvent;
import com.loomq.grpc.gen.ListIntentsRequest;
import com.loomq.grpc.gen.ListIntentsResponse;
import com.loomq.grpc.gen.LoomQServiceGrpc;
import com.loomq.grpc.gen.PatchIntentRequest;
import com.loomq.spi.RaftStatusProvider;
import com.loomq.raft.RaftWriteCoordinator;
import com.loomq.store.IdempotencyResult;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation that wraps {@link LoomqEngine}.
 *
 * <p>Mirrors the business logic of {@link com.loomq.http.netty.IntentHandler}
 * but communicates via Protobuf over HTTP/2.
 */
public class LoomqGrpcService extends LoomQServiceGrpc.LoomQServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(LoomqGrpcService.class);
    private static final PrecisionTier DEFAULT_TIER = PrecisionTierCatalog.defaultCatalog().defaultTier();

    private final LoomqEngine engine;
    private final RaftStatusProvider raftStatus;
    private final RaftWriteCoordinator raftWriteCoordinator;
    private final GlobalIntentObserver globalObserver;

    public LoomqGrpcService(LoomqEngine engine, RaftStatusProvider raftStatus,
                            RaftWriteCoordinator raftWriteCoordinator,
                            GlobalIntentObserver globalObserver) {
        this.engine = engine;
        this.raftStatus = raftStatus;
        this.raftWriteCoordinator = raftWriteCoordinator;
        this.globalObserver = globalObserver;
    }

    @Override
    public void createIntent(CreateIntentRequest request, StreamObserver<com.loomq.grpc.gen.IntentMessage> response) {
        try {
            if (isRaftWriteBlocked()) {
                throw GrpcStatusAdapter.unavailable("50302", "Write must be served by the Raft leader");
            }

            // Validation
            Instant executeAt = ProtoConverter.toDomain(request.hasExecuteAt() ? request.getExecuteAt() : null);
            Instant deadline = ProtoConverter.toDomain(request.hasDeadline() ? request.getDeadline() : null);
            ValidationResult validation = IntentValidator.validateCreate(executeAt, deadline, request.getShardKey());
            if (validation.isInvalid()) {
                throw GrpcStatusAdapter.invalidArgument(validation.errorCode(), validation.errorMessage());
            }
            if (executeAt != null) {
                ValidationResult futureValidation = IntentValidator.validateExecuteAtFuture(executeAt);
                if (futureValidation.isInvalid()) {
                    throw GrpcStatusAdapter.invalidArgument(futureValidation.errorCode(), futureValidation.errorMessage());
                }
            }

            // Idempotency check
            String idempotencyKey = request.getIdempotencyKey();
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                IdempotencyResult idemResult = engine.checkIdempotency(idempotencyKey);
                if (idemResult.isDuplicateActive()) {
                    response.onNext(ProtoConverter.toProto(idemResult.getIntent()));
                    response.onCompleted();
                    return;
                }
                if (idemResult.isDuplicateTerminal()) {
                    throw GrpcStatusAdapter.alreadyExists(idemResult.getIntent().getIntentId());
                }
            }

            // Build intent
            String intentId = request.getIntentId();
            if (intentId == null || intentId.isBlank()) {
                intentId = UUID.randomUUID().toString();
            }
            Intent intent = ProtoConverter.toDomain(request, intentId);

            // SLO-driven tier recommendation (overrides explicit precision_tier if provided)
            if (request.hasSlo()) {
                var slo = request.getSlo();
                Reliability reliability;
                try {
                    reliability = Reliability.valueOf(slo.getReliability().toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw GrpcStatusAdapter.invalidArgument("40003",
                        "Invalid reliability value: " + slo.getReliability());
                }
                TierAdvisor.Recommendation rec = TierAdvisor.recommend(slo.getMaxTardinessMs(), reliability);
                intent.setPrecisionTier(rec.tier());
                intent.setAckMode(reliability.toAckMode());
            }

            if (intent.getPrecisionTier() == null) {
                intent.setPrecisionTier(DEFAULT_TIER);
            }

            // Create (Raft or direct)
            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                Intent snapshot = intent.copy();
                snapshot.transitionTo(IntentStatus.SCHEDULED);
                Intent committed = raftWriteCoordinator.commitSnapshot(snapshot, "create", buildRequestKey(request));
                response.onNext(ProtoConverter.toProto(committed));
                response.onCompleted();
            } else {
                final Intent finalIntent = intent;
                engine.createIntent(finalIntent, finalIntent.getAckMode())
                    .thenAccept(seq -> {
                        response.onNext(ProtoConverter.toProto(finalIntent));
                        response.onCompleted();
                    })
                    .exceptionally(ex -> {
                        handleCompletionException(toCompletionException(ex), response);
                        return null;
                    });
            }

        } catch (StatusRuntimeException e) {
            response.onError(e);
        } catch (CompletionException e) {
            handleCompletionException(e, response);
        } catch (Exception e) {
            logger.error("Error in createIntent", e);
            response.onError(GrpcStatusAdapter.fromException(
                e instanceof Exception ex ? ex : new RuntimeException(e)));
        }
    }

    @Override
    public void getIntent(GetIntentRequest request, StreamObserver<com.loomq.grpc.gen.IntentMessage> response) {
        try {
            if (isRaftFollowerReadBlocked()) {
                throw GrpcStatusAdapter.unavailable("50301", "Read must be served by the Raft leader");
            }
            Optional<Intent> intent = engine.getIntent(request.getIntentId());
            if (intent.isEmpty()) {
                throw GrpcStatusAdapter.notFound(request.getIntentId());
            }
            response.onNext(ProtoConverter.toProto(intent.get()));
            response.onCompleted();
        } catch (StatusRuntimeException e) {
            response.onError(e);
        } catch (Exception e) {
            logger.error("Error in getIntent", e);
            response.onError(GrpcStatusAdapter.fromException(e));
        }
    }

    @Override
    public void listIntents(ListIntentsRequest request, StreamObserver<ListIntentsResponse> response) {
        try {
            if (isRaftFollowerReadBlocked()) {
                throw GrpcStatusAdapter.unavailable("50301", "Read must be served by the Raft leader");
            }
            String statusParam = request.getStatus();
            if (statusParam == null || statusParam.isBlank()) {
                throw GrpcStatusAdapter.invalidArgument("40002", "Query parameter 'status' is required");
            }
            IntentStatus status;
            try {
                status = IntentStatus.valueOf(statusParam.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw GrpcStatusAdapter.invalidArgument("40003", "Invalid status value: " + statusParam);
            }
            int limit = Math.max(1, Math.min(500, request.getLimit() > 0 ? request.getLimit() : 50));
            int offset = Math.max(0, request.getOffset());

            List<Intent> intents = engine.getIntentStore().findByStatus(status, offset, limit);
            long total = engine.getIntentStore().countByStatus(status);

            var protoIntents = intents.stream()
                .map(ProtoConverter::toProto)
                .toList();

            response.onNext(ListIntentsResponse.newBuilder()
                .addAllIntents(protoIntents)
                .setTotal(total)
                .setOffset(offset)
                .setLimit(limit)
                .build());
            response.onCompleted();
        } catch (StatusRuntimeException e) {
            response.onError(e);
        } catch (Exception e) {
            logger.error("Error in listIntents", e);
            response.onError(GrpcStatusAdapter.fromException(e));
        }
    }

    @Override
    public void patchIntent(PatchIntentRequest request, StreamObserver<com.loomq.grpc.gen.IntentMessage> response) {
        try {
            if (isRaftWriteBlocked()) {
                throw GrpcStatusAdapter.unavailable("50302", "Write must be served by the Raft leader");
            }
            String intentId = request.getIntentId();
            Optional<Intent> current = engine.getIntent(intentId);
            if (current.isEmpty()) {
                throw GrpcStatusAdapter.notFound(intentId);
            }
            if (!current.get().getStatus().isModifiable()) {
                throw GrpcStatusAdapter.invalidArgument("42205",
                    "Intent cannot be modified in state: " + current.get().getStatus());
            }

            Instant newExecuteAt = ProtoConverter.toDomain(request.hasExecuteAt() ? request.getExecuteAt() : null);
            if (newExecuteAt != null) {
                ValidationResult futureValidation = IntentValidator.validateExecuteAtFuture(newExecuteAt);
                if (futureValidation.isInvalid()) {
                    throw GrpcStatusAdapter.invalidArgument(futureValidation.errorCode(), futureValidation.errorMessage());
                }
            }

            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                long expectedRevision = request.getExpectedRevision();
                if (expectedRevision <= 0) {
                    throw GrpcStatusAdapter.invalidArgument("42801",
                        "expected_revision is required for Raft writes");
                }
                Intent committed = raftWriteCoordinator.commitMutation(
                    intentId, "patch", "", expectedRevision,
                    snapshot -> applyPatch(snapshot, request, newExecuteAt));
                response.onNext(ProtoConverter.toProto(committed));
            } else {
                applyPatch(current.get(), request, newExecuteAt);
                response.onNext(ProtoConverter.toProto(current.get()));
            }
            response.onCompleted();

        } catch (StatusRuntimeException e) {
            response.onError(e);
        } catch (Exception e) {
            logger.error("Error in patchIntent", e);
            response.onError(GrpcStatusAdapter.fromException(
                e instanceof Exception ex ? ex : new RuntimeException(e)));
        }
    }

    @Override
    public void cancelIntent(com.loomq.grpc.gen.CancelIntentRequest request,
                             StreamObserver<com.loomq.grpc.gen.IntentMessage> response) {
        try {
            if (isRaftWriteBlocked()) {
                throw GrpcStatusAdapter.unavailable("50302", "Write must be served by the Raft leader");
            }
            String intentId = request.getIntentId();
            Optional<Intent> current = engine.getIntent(intentId);
            if (current.isEmpty()) {
                throw GrpcStatusAdapter.notFound(intentId);
            }
            if (!current.get().getStatus().isCancellable()) {
                throw GrpcStatusAdapter.invalidArgument("42204",
                    "Intent cannot be cancelled in state: " + current.get().getStatus());
            }

            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                long expectedRevision = request.getExpectedRevision();
                if (expectedRevision <= 0) {
                    throw GrpcStatusAdapter.invalidArgument("42801",
                        "expected_revision is required for Raft writes");
                }
                Intent committed = raftWriteCoordinator.commitMutation(
                    intentId, "cancel", "", expectedRevision,
                    snapshot -> { snapshot.transitionTo(IntentStatus.CANCELED); return snapshot; });
                response.onNext(ProtoConverter.toProto(committed));
            } else {
                if (!engine.cancelIntent(intentId)) {
                    throw GrpcStatusAdapter.internal("Failed to cancel intent");
                }
                response.onNext(ProtoConverter.toProto(current.get()));
            }
            response.onCompleted();

        } catch (StatusRuntimeException e) {
            response.onError(e);
        } catch (Exception e) {
            logger.error("Error in cancelIntent", e);
            response.onError(GrpcStatusAdapter.fromException(
                e instanceof Exception ex ? ex : new RuntimeException(e)));
        }
    }

    @Override
    public void fireNow(com.loomq.grpc.gen.FireNowRequest request,
                        StreamObserver<IntentActionResponse> response) {
        try {
            if (isRaftWriteBlocked()) {
                throw GrpcStatusAdapter.unavailable("50302", "Write must be served by the Raft leader");
            }
            String intentId = request.getIntentId();
            Optional<Intent> current = engine.getIntent(intentId);
            if (current.isEmpty()) {
                throw GrpcStatusAdapter.notFound(intentId);
            }

            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                long expectedRevision = request.getExpectedRevision();
                if (expectedRevision <= 0) {
                    throw GrpcStatusAdapter.invalidArgument("42801",
                        "expected_revision is required for Raft writes");
                }
                Intent committed = raftWriteCoordinator.commitMutation(
                    intentId, "fire-now", "", expectedRevision,
                    snapshot -> { snapshot.setExecuteAt(Instant.now()); return snapshot; });
                response.onNext(IntentActionResponse.newBuilder()
                    .setIntentId(committed.getIntentId())
                    .setStatus(committed.getStatus().name())
                    .build());
            } else {
                if (!engine.fireNow(intentId)) {
                    throw GrpcStatusAdapter.internal("Failed to trigger intent immediately");
                }
                response.onNext(IntentActionResponse.newBuilder()
                    .setIntentId(intentId)
                    .setStatus(IntentStatus.DISPATCHING.name())
                    .build());
            }
            response.onCompleted();

        } catch (StatusRuntimeException e) {
            response.onError(e);
        } catch (Exception e) {
            logger.error("Error in fireNow", e);
            response.onError(GrpcStatusAdapter.fromException(
                e instanceof Exception ex ? ex : new RuntimeException(e)));
        }
    }

    @Override
    public void reviveIntent(com.loomq.grpc.gen.ReviveIntentRequest request,
                             StreamObserver<com.loomq.grpc.gen.IntentMessage> response) {
        try {
            if (isRaftWriteBlocked()) {
                throw GrpcStatusAdapter.unavailable("50302", "Write must be served by the Raft leader");
            }
            String intentId = request.getIntentId();
            Optional<Intent> current = engine.getIntent(intentId);
            if (current.isEmpty()) {
                throw GrpcStatusAdapter.notFound(intentId);
            }
            Intent intent = current.get();
            if (intent.getStatus() != IntentStatus.DEAD_LETTERED) {
                throw GrpcStatusAdapter.invalidArgument("42207",
                    "Intent can only be revived from DEAD_LETTERED state, current state: " + intent.getStatus());
            }

            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                // Raft path — work on a snapshot and commit through consensus
                Intent snapshot = intent.copy();
                applyReviveModifications(snapshot, request);
                snapshot.transitionTo(IntentStatus.SCHEDULED);
                Intent committed = raftWriteCoordinator.commitSnapshot(snapshot, "revive", intentId);
                response.onNext(ProtoConverter.toProto(committed));
                response.onCompleted();
            } else {
                // Non-Raft path — modify in place and write directly
                applyReviveModifications(intent, request);
                engine.createIntent(intent, intent.getAckMode())
                    .thenAccept(seq -> {
                        response.onNext(ProtoConverter.toProto(intent));
                        response.onCompleted();
                    })
                    .exceptionally(ex -> {
                        handleCompletionException(toCompletionException(ex), response);
                        return null;
                    });
            }

        } catch (StatusRuntimeException e) {
            response.onError(e);
        } catch (CompletionException e) {
            handleCompletionException(e, response);
        } catch (Exception e) {
            logger.error("Error in reviveIntent", e);
            response.onError(GrpcStatusAdapter.fromException(
                e instanceof Exception ex ? ex : new RuntimeException(e)));
        }
    }

    @Override
    public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> response) {
        try {
            var walHealth = engine.getWalHealth();
            var builder = HealthCheckResponse.newBuilder()
                .setStatus(walHealth.status())
                .putDetails("wal_status", walHealth.status())
                .putDetails("intent_count", String.valueOf(engine.getIntentStore().getPendingCount()));
            response.onNext(builder.build());
            response.onCompleted();
        } catch (Exception e) {
            response.onNext(HealthCheckResponse.newBuilder().setStatus("DOWN").build());
            response.onCompleted();
        }
    }

    @Override
    public void watchIntent(com.loomq.grpc.gen.WatchIntentRequest request,
                            StreamObserver<IntentEvent> response) {
        var serverObserver = (ServerCallStreamObserver<IntentEvent>) response;
        var entry = globalObserver.register(request, serverObserver);

        serverObserver.setOnReadyHandler(entry::drainQueue);
        serverObserver.setOnCancelHandler(() -> globalObserver.unregister(entry));
        serverObserver.setOnCloseHandler(() -> globalObserver.unregister(entry));
    }

    // ── Private helpers ──

    private boolean isRaftFollowerReadBlocked() {
        return raftStatus != null && raftStatus.isRaftEnabled() && !raftStatus.canServeLinearizableRead();
    }

    private boolean isRaftWriteBlocked() {
        return raftStatus != null && raftStatus.isRaftEnabled() && !raftStatus.isLeader();
    }

    private String buildRequestKey(CreateIntentRequest request) {
        String rid = request.getRequestId();
        if (rid != null && !rid.isBlank()) return rid;
        return "create:" + request.getIntentId() + ":" + request.getIdempotencyKey();
    }

    private static void applyReviveModifications(Intent intent, com.loomq.grpc.gen.ReviveIntentRequest request) {
        if (request.hasExecuteAt()) intent.setExecuteAt(ProtoConverter.toDomain(request.getExecuteAt()));
        if (request.hasDeadline()) intent.setDeadline(ProtoConverter.toDomain(request.getDeadline()));
        if (request.hasCallback()) intent.setCallback(ProtoConverter.toDomain(request.getCallback()));
        if (request.hasRedelivery()) intent.setRedelivery(ProtoConverter.toDomain(request.getRedelivery()));
        intent.setAttempts(0);

        Instant requestedExecuteAt = intent.getExecuteAt();
        if (requestedExecuteAt == null || requestedExecuteAt.isBefore(Instant.now())) {
            intent.setExecuteAt(Instant.now().plusMillis(intent.getPrecisionTier().getPrecisionWindowMs()));
        }
    }

    private static Intent applyPatch(Intent intent, PatchIntentRequest request, Instant newExecuteAt) {
        if (newExecuteAt != null) intent.setExecuteAt(newExecuteAt);
        if (request.hasDeadline()) intent.setDeadline(ProtoConverter.toDomain(request.getDeadline()));
        if (!request.getExpiredAction().isEmpty()) {
            var ea = ProtoConverter.parseExpiredAction(request.getExpiredAction());
            if (ea != null) intent.setExpiredAction(ea);
        }
        if (request.hasRedelivery()) intent.setRedelivery(ProtoConverter.toDomain(request.getRedelivery()));
        if (!request.getTagsMap().isEmpty()) intent.setTags(request.getTagsMap());
        return intent;
    }

    private void handleCompletionException(CompletionException e, StreamObserver<?> response) {
        Throwable cause = e.getCause();
        if (cause instanceof BackPressureException bpe) {
            response.onError(GrpcStatusAdapter.backPressure(bpe.getMessage(), bpe.getRetryAfterMs()));
        } else {
            response.onError(GrpcStatusAdapter.internal(
                cause != null ? cause.getMessage() : e.getMessage()));
        }
    }

    private static CompletionException toCompletionException(Throwable ex) {
        if (ex instanceof CompletionException ce) return ce;
        return new CompletionException(ex);
    }
}
