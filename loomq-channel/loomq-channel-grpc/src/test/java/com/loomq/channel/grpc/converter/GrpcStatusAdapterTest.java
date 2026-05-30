package com.loomq.channel.grpc.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.exception.BackPressureException;
import com.loomq.raft.RaftWriteBackPressureException;
import com.loomq.raft.RaftWriteConflictException;
import com.loomq.raft.RaftWriteUnavailableException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class GrpcStatusAdapterTest {

    private static final Metadata.Key<String> ERROR_CODE_KEY =
        Metadata.Key.of("loomq-error-code", Metadata.ASCII_STRING_MARSHALLER);

    @Test
    void notFoundReturns404() {
        StatusRuntimeException ex = GrpcStatusAdapter.notFound("intent-1");
        assertEquals(Status.NOT_FOUND.getCode(), ex.getStatus().getCode());
        assertEquals("40401", getErrorCode(ex));
        assertTrue(ex.getStatus().getDescription().contains("intent-1"));
    }

    @Test
    void invalidArgumentReturns400() {
        StatusRuntimeException ex = GrpcStatusAdapter.invalidArgument("40001", "bad input");
        assertEquals(Status.INVALID_ARGUMENT.getCode(), ex.getStatus().getCode());
        assertEquals("40001", getErrorCode(ex));
    }

    @Test
    void alreadyExistsReturns409() {
        StatusRuntimeException ex = GrpcStatusAdapter.alreadyExists("intent-1");
        assertEquals(Status.ALREADY_EXISTS.getCode(), ex.getStatus().getCode());
        assertEquals("40901", getErrorCode(ex));
    }

    @Test
    void conflictReturnsAborted() {
        StatusRuntimeException ex = GrpcStatusAdapter.conflict("40902", "revision conflict");
        assertEquals(Status.ABORTED.getCode(), ex.getStatus().getCode());
        assertEquals("40902", getErrorCode(ex));
    }

    @Test
    void backPressureReturnsResourceExhausted() {
        StatusRuntimeException ex = GrpcStatusAdapter.backPressure("too busy", 5000);
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), ex.getStatus().getCode());
        assertEquals("42900", getErrorCode(ex));
        assertNotNull(getMetadata(ex, "retry-after-ms"));
        assertEquals("5000", getMetadata(ex, "retry-after-ms"));
    }

    @Test
    void unavailableReturns503() {
        StatusRuntimeException ex = GrpcStatusAdapter.unavailable("50302", "not leader");
        assertEquals(Status.UNAVAILABLE.getCode(), ex.getStatus().getCode());
        assertEquals("50302", getErrorCode(ex));
    }

    @Test
    void internalReturns500() {
        StatusRuntimeException ex = GrpcStatusAdapter.internal("something broke");
        assertEquals(Status.INTERNAL.getCode(), ex.getStatus().getCode());
        assertEquals("50000", getErrorCode(ex));
    }

    @Test
    void fromExceptionRaftUnavailable() {
        StatusRuntimeException ex = GrpcStatusAdapter.fromException(
            new RaftWriteUnavailableException("create", "intent-1", "no leader"));
        assertEquals(Status.UNAVAILABLE.getCode(), ex.getStatus().getCode());
        assertEquals("50302", getErrorCode(ex));
    }

    @Test
    void fromExceptionRaftBackPressure() {
        StatusRuntimeException ex = GrpcStatusAdapter.fromException(
            new RaftWriteBackPressureException("create", "intent-1", "overloaded", 3000));
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), ex.getStatus().getCode());
        assertEquals("42900", getErrorCode(ex));
    }

    @Test
    void fromExceptionRaftConflict() {
        StatusRuntimeException ex = GrpcStatusAdapter.fromException(
            new RaftWriteConflictException("patch", "intent-1", 1, 2, "conflict"));
        assertEquals(Status.ABORTED.getCode(), ex.getStatus().getCode());
        assertEquals("40902", getErrorCode(ex));
    }

    @Test
    void fromExceptionBackPressure() {
        StatusRuntimeException ex = GrpcStatusAdapter.fromException(
            new BackPressureException("busy", com.loomq.domain.intent.PrecisionTier.STANDARD, 2000));
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), ex.getStatus().getCode());
        assertEquals("42900", getErrorCode(ex));
    }

    @Test
    void fromExceptionGeneric() {
        StatusRuntimeException ex = GrpcStatusAdapter.fromException(
            new RuntimeException("unexpected"));
        assertEquals(Status.INTERNAL.getCode(), ex.getStatus().getCode());
        assertEquals("50000", getErrorCode(ex));
    }

    @Test
    void fromExceptionNullMessage() {
        StatusRuntimeException ex = GrpcStatusAdapter.fromException(
            new RuntimeException((String) null));
        assertEquals(Status.INTERNAL.getCode(), ex.getStatus().getCode());
    }

    private String getErrorCode(StatusRuntimeException ex) {
        Metadata trailers = ex.getTrailers();
        return trailers != null ? trailers.get(ERROR_CODE_KEY) : null;
    }

    private String getMetadata(StatusRuntimeException ex, String key) {
        Metadata trailers = ex.getTrailers();
        return trailers != null ? trailers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)) : null;
    }
}
