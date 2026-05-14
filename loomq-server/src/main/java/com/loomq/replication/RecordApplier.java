package com.loomq.replication;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * 复制记录应用器。
 *
 * 在 follower 侧接收 leader 发来的 ReplicationRecord，
 * 解码 payload 为 Intent 后写入 IntentStore 并重新调度。
 *
 * 作为 {@link ReplicationManager#setRecordApplier(Function)} 的实参注入。
 *
 * @author loomq
 */
public final class RecordApplier implements Function<ReplicationRecord, Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(RecordApplier.class);

    private final IntentStore intentStore;
    private volatile PrecisionScheduler scheduler;

    public RecordApplier(IntentStore intentStore) {
        this.intentStore = intentStore;
    }

    public void setScheduler(PrecisionScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Boolean apply(ReplicationRecord record) {
        if (record.getPayload() == null || record.getPayload().length == 0) {
            logger.warn("Empty payload in replication record, offset={}", record.getOffset());
            return false;
        }

        try {
            byte[] intentBytes;
            // Payload: eventType(1B) + intentBinary(N)
            if (record.getPayload()[0] == 0x01 && record.getPayload().length > 1) {
                int intentLen = record.getPayload().length - 1;
                intentBytes = new byte[intentLen];
                System.arraycopy(record.getPayload(), 1, intentBytes, 0, intentLen);
            } else {
                intentBytes = record.getPayload();
            }

            Intent intent = IntentBinaryCodec.decode(intentBytes);

            // Check for duplicate (idempotent apply)
            Intent existing = intentStore.findById(intent.getIntentId());
            if (existing != null && existing.getStatus().isTerminal()) {
                logger.debug("Skipping duplicate/terminal intent: {}", intent.getIntentId());
                return true; // Already processed, consider it success
            }

            intentStore.upsert(intent);
            logger.debug("Follower applied intent: id={}, status={}", intent.getIntentId(), intent.getStatus());

            // Schedule if scheduler is wired
            PrecisionScheduler sched = this.scheduler;
            if (sched != null) {
                sched.restore(intent);
            }

            return true;
        } catch (Exception e) {
            logger.error("Failed to apply replication record: offset={}", record.getOffset(), e);
            return false;
        }
    }
}
