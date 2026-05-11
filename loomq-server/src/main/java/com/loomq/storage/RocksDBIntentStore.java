package com.loomq.storage;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.store.IdempotencyRecord;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于 RocksDB 的 Intent 持久化存储。
 *
 * 单列族设计，通过 key 前缀区分数据类型：
 * <pre>
 *   i:{intentId}     → Intent (IntentBinaryCodec 编码)
 *   k:{key}          → IdempotencyRecord
 *   s:{status}#{id}  → empty (按状态索引)
 *   e:{epochMs}#{id} → empty (按执行时间索引)
 * </pre>
 *
 * 依赖 RocksDB JNI，需放在 loomq-server 而非 core。
 *
 * @author loomq
 * @since v0.9.0
 */
public class RocksDBIntentStore implements IntentStore {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBIntentStore.class);

    private static final byte[] INTENT_PREFIX = "i:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] IDEMPOTENCY_PREFIX = "k:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_PREFIX = "s:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EXECUTE_AT_PREFIX = "e:".getBytes(StandardCharsets.UTF_8);

    private final RocksDB db;
    private final WriteOptions writeOptions;
    private final Path dbPath;

    public RocksDBIntentStore(Path dbPath) throws IOException {
        this.dbPath = dbPath;
        RocksDB.loadLibrary();

        try {
            Files.createDirectories(dbPath);
            Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(false);

            this.db = RocksDB.open(options, dbPath.toString());
            this.writeOptions = new WriteOptions().setSync(true);

            logger.info("RocksDBIntentStore opened: path={}", dbPath);
        } catch (RocksDBException e) {
            throw new IOException("Failed to open RocksDB at " + dbPath, e);
        }
    }

    // ========== IntentStore 实现 ==========

    @Override
    public void save(Intent intent) {
        try (WriteBatch batch = new WriteBatch()) {
            upsertToBatch(batch, intent);
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to save intent: " + intent.getIntentId(), e);
        }
    }

    @Override
    public void update(Intent intent) {
        // 读取旧状态以正确维护索引
        Intent old = findById(intent.getIntentId());
        try (WriteBatch batch = new WriteBatch()) {
            if (old != null) {
                removeIndexEntries(batch, old, intent.getIntentId());
            }
            upsertToBatch(batch, intent);
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to update intent: " + intent.getIntentId(), e);
        }
    }

    @Override
    public Intent findById(String intentId) {
        try {
            byte[] key = intentKey(intentId);
            byte[] value = db.get(key);
            if (value == null) return null;
            return IntentBinaryCodec.decode(value);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to find intent: " + intentId, e);
        }
    }

    @Override
    public void delete(String intentId) {
        Intent existing = findById(intentId);
        if (existing == null) return;

        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(intentKey(intentId));
            removeIndexEntries(batch, existing, intentId);
            removeIdempotency(batch, existing);

            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to delete intent: " + intentId, e);
        }
    }

    @Override
    public Map<String, Intent> getAllIntents() {
        Map<String, Intent> result = new HashMap<>();
        try (RocksIterator it = db.newIterator()) {
            it.seek(INTENT_PREFIX);
            while (it.isValid()) {
                byte[] key = it.key();
                if (!startsWith(key, INTENT_PREFIX)) break;

                Intent intent = IntentBinaryCodec.decode(it.value());
                result.put(intent.getIntentId(), intent);
                it.next();
            }
        }
        return Map.copyOf(result);
    }

    @Override
    public long countByStatus(IntentStatus status) {
        long count = 0;
        byte[] prefix = statusPrefix(status);
        try (RocksIterator it = db.newIterator()) {
            it.seek(prefix);
            while (it.isValid()) {
                if (!startsWith(it.key(), prefix)) break;
                count++;
                it.next();
            }
        }
        return count;
    }

    @Override
    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        if (idempotencyKey == null) {
            return IdempotencyResult.newRequest();
        }

        try {
            byte[] value = db.get(idempotencyKey(idempotencyKey));
            if (value == null) {
                return IdempotencyResult.newRequest();
            }

            IdempotencyRecord record = IdempotencyRecord.fromBytes(value);
            if (!record.isInWindow()) {
                return IdempotencyResult.windowExpired();
            }

            Intent intent = findById(record.getIntentId());
            if (intent == null) {
                db.delete(idempotencyKey(idempotencyKey));
                return IdempotencyResult.newRequest();
            }

            if (intent.getStatus().isTerminal()) {
                return IdempotencyResult.duplicateTerminal(intent);
            }
            return IdempotencyResult.duplicateActive(intent);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to check idempotency: " + idempotencyKey, e);
        }
    }

    @Override
    public long getPendingCount() {
        long total = 0;
        for (IntentStatus status : IntentStatus.values()) {
            if (!status.isTerminal()) {
                total += countByStatus(status);
            }
        }
        return total;
    }

    @Override
    public void shutdown() {
        writeOptions.close();
        db.close();
        logger.info("RocksDBIntentStore closed: path={}", dbPath);
    }

    // ========== 内部方法 ==========

    private void upsertToBatch(WriteBatch batch, Intent intent) throws RocksDBException {
        byte[] intentBytes = IntentBinaryCodec.encode(intent);
        batch.put(intentKey(intent.getIntentId()), intentBytes);

        // 状态索引
        batch.put(statusKey(intent), new byte[0]);
        // 执行时间索引
        batch.put(executeAtKey(intent), new byte[0]);

        // 幂等记录
        String idempotencyKey = intent.getIdempotencyKey();
        if (idempotencyKey != null) {
            byte[] recordBytes = IdempotencyRecord.fromIntent(intent).toBytes();
            batch.put(idempotencyKey(idempotencyKey), recordBytes);
        }
    }

    private void removeIndexEntries(WriteBatch batch, Intent old, String intentId) throws RocksDBException {
        batch.delete(statusKey(old));
        batch.delete(executeAtKey(old));
    }

    private void removeIdempotency(WriteBatch batch, Intent intent) throws RocksDBException {
        String key = intent.getIdempotencyKey();
        if (key != null) {
            batch.delete(idempotencyKey(key));
        }
    }

    // ========== key 编码 ==========

    private static byte[] intentKey(String intentId) {
        byte[] prefix = INTENT_PREFIX;
        byte[] id = intentId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefix.length + id.length];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(id, 0, key, prefix.length, id.length);
        return key;
    }

    private static byte[] idempotencyKey(String idempotencyKey) {
        byte[] prefix = IDEMPOTENCY_PREFIX;
        byte[] k = idempotencyKey.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefix.length + k.length];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(k, 0, key, prefix.length, k.length);
        return key;
    }

    private static byte[] statusKey(Intent intent) {
        String raw = STATUS_PREFIX_STR + intent.getStatus().name() + "#" + intent.getIntentId();
        return raw.getBytes(StandardCharsets.UTF_8);
    }
    private static final String STATUS_PREFIX_STR = "s:";

    private static byte[] executeAtKey(Intent intent) {
        long ms = intent.getExecuteAt() != null ? intent.getExecuteAt().toEpochMilli() : 0L;
        String raw = EXECUTE_AT_PREFIX_STR + String.format("%020d", ms) + "#" + intent.getIntentId();
        return raw.getBytes(StandardCharsets.UTF_8);
    }
    private static final String EXECUTE_AT_PREFIX_STR = "e:";

    private static byte[] statusPrefix(IntentStatus status) {
        return (STATUS_PREFIX_STR + status.name()).getBytes(StandardCharsets.UTF_8);
    }

    private static boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) return false;
        }
        return true;
    }
}
