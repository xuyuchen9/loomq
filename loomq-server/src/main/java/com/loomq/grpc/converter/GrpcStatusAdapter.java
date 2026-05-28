package com.loomq.grpc.converter;

import com.loomq.common.exception.BackPressureException;
import com.loomq.raft.RaftWriteBackPressureException;
import com.loomq.raft.RaftWriteConflictException;
import com.loomq.raft.RaftWriteUnavailableException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Maps LoomQ business exceptions and error codes to gRPC {@link Status}
 * responses, with the application error code attached as trailing metadata.
 */
public final class GrpcStatusAdapter {

    private static final Metadata.Key<String> ERROR_CODE_KEY =
        Metadata.Key.of("loomq-error-code", Metadata.ASCII_STRING_MARSHALLER);

    private GrpcStatusAdapter() {}

    public static StatusRuntimeException notFound(String intentId) {
        return error(Status.NOT_FOUND, "40401", "Intent not found: " + intentId);
    }

    public static StatusRuntimeException invalidArgument(String code, String message) {
        return error(Status.INVALID_ARGUMENT, code, message);
    }

    public static StatusRuntimeException alreadyExists(String intentId) {
        return error(Status.ALREADY_EXISTS, "40901", "Idempotency key conflict for intent: " + intentId);
    }

    public static StatusRuntimeException conflict(String code, String message) {
        return error(Status.ABORTED, code, message);
    }

    public static StatusRuntimeException backPressure(String message, long retryAfterMs) {
        Metadata metadata = new Metadata();
        metadata.put(ERROR_CODE_KEY, "42900");
        metadata.put(Metadata.Key.of("retry-after-ms", Metadata.ASCII_STRING_MARSHALLER),
            String.valueOf(retryAfterMs));
        return Status.RESOURCE_EXHAUSTED
            .withDescription(message)
            .asRuntimeException(metadata);
    }

    public static StatusRuntimeException unavailable(String code, String message) {
        Metadata metadata = new Metadata();
        metadata.put(ERROR_CODE_KEY, code);
        return Status.UNAVAILABLE
            .withDescription(message)
            .asRuntimeException(metadata);
    }

    public static StatusRuntimeException internal(String message) {
        return error(Status.INTERNAL, "50000", message);
    }

    public static StatusRuntimeException fromException(Exception e) {
        if (e instanceof RaftWriteUnavailableException rwue) {
            return unavailable("50302", rwue.getMessage() != null ? rwue.getMessage() : "Raft write unavailable");
        }
        if (e instanceof RaftWriteBackPressureException rwbe) {
            return backPressure(rwbe.getMessage() != null ? rwbe.getMessage() : "Raft backpressure", rwbe.retryAfterMs());
        }
        if (e instanceof RaftWriteConflictException rwce) {
            return conflict("40902", rwce.getMessage() != null ? rwce.getMessage() : "Raft revision conflict");
        }
        if (e instanceof BackPressureException bpe) {
            return backPressure(bpe.getMessage() != null ? bpe.getMessage() : "Backpressure", bpe.getRetryAfterMs());
        }
        return internal(e.getMessage() != null ? e.getMessage() : "Internal server error");
    }

    private static StatusRuntimeException error(Status status, String code, String message) {
        Metadata metadata = new Metadata();
        metadata.put(ERROR_CODE_KEY, code);
        return status.withDescription(message).asRuntimeException(metadata);
    }
}
