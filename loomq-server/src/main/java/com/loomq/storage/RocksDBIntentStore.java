package com.loomq.storage;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.store.IdempotencyRecord;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Map<IntentStatus, AtomicLong> statusCounts = new EnumMap<>(IntentStatus.class);
    private final AtomicLong pendingCount = new AtomicLong();

    /**
     * 创建 RocksDBIntentStore，默认不启用 RocksDB WAL sync。
     *
     * loomq 使用 SimpleWalWriter 提供持久化保证，RocksDB 作为
     * 可恢复的缓存层。syncWrites=false 避免双重 fsync 开销。
     */
    public RocksDBIntentStore(Path dbPath) throws IOException {
        this(dbPath, false);
    }

    /**
     * 创建 RocksDBIntentStore。
     *
     * @param dbPath     RocksDB 数据目录
     * @param syncWrites 是否每次写入同步刷盘。WAL 模式应设为 false；
     *                   仅在没有外部 WAL 的 standalone 场景设为 true
     */
    public RocksDBIntentStore(Path dbPath, boolean syncWrites) throws IOException {
        this.dbPath = dbPath;
        RocksDB.loadLibrary();

        try {
            Files.createDirectories(dbPath);
            Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(false);

            this.db = RocksDB.open(options, dbPath.toString());
            this.writeOptions = new WriteOptions().setSync(syncWrites);
            initializeCounters();

            logger.info("RocksDBIntentStore opened: path={}, syncWrites={}", dbPath, syncWrites);
        } catch (RocksDBException e) {
            throw new IOException("Failed to open RocksDB at " + dbPath, e);
        }
    }

    /**
     * 创建启用同步写入的 RocksDBIntentStore（standalone 模式）。
     * 仅在没有外部 WAL 兜底时使用此工厂方法。
     */
    public static RocksDBIntentStore withDurableWrites(Path dbPath) throws IOException {
        return new RocksDBIntentStore(dbPath, true);
    }

    // ========== IntentStore 实现 ==========

    private static byte[] prefixUpperBound(byte[] prefix) {
        byte[] upper = prefix.clone();
        upper[upper.length - 1]++;
        return upper;
    }

    @Override
    public void save(Intent intent) {
        try (WriteBatch batch = new WriteBatch()) {
            upsertToBatch(batch, intent);
            db.write(writeOptions, batch);
            incrementCounts(intent.getStatus());
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to save intent: " + intent.getIntentId(), e);
        }
    }

    @Override
    public void update(Intent intent) {
        // 读取旧状态以正确维护索引。
        // 注意：findById 和 upsertToBatch 之间没有快照隔离。
        // 但在单 writer-per-intent 场景（调度器保证）下，
        // WriteBatch 提供的原子写入足以保证正确性。
        Intent old = findById(intent.getIntentId());
        try (WriteBatch batch = new WriteBatch()) {
            if (old != null) {
                removeIndexEntries(batch, old, intent);
            }
            upsertToBatch(batch, intent);
            db.write(writeOptions, batch);
            if (old != null) {
                decrementCounts(old.getStatus());
            }
            incrementCounts(intent.getStatus());
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
    public void upsert(Intent intent) {
        Intent old = findById(intent.getIntentId());
        try (WriteBatch batch = new WriteBatch()) {
            if (old != null) {
                removeIndexEntries(batch, old, intent);
            }
            upsertToBatch(batch, intent);
            db.write(writeOptions, batch);
            if (old != null) {
                decrementCounts(old.getStatus());
            }
            incrementCounts(intent.getStatus());
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to upsert intent: " + intent.getIntentId(), e);
        }
    }

    @Override
    public void delete(String intentId) {
        Intent existing = findById(intentId);
        if (existing == null) return;

        try (WriteBatch batch = new WriteBatch()) {
            batch.delete(intentKey(intentId));
            removeIndexEntries(batch, existing, existing);
            removeIdempotency(batch, existing);

            db.write(writeOptions, batch);
            decrementCounts(existing.getStatus());
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
    public void clear() {
        try {
            db.deleteRange(writeOptions, INTENT_PREFIX, prefixUpperBound(INTENT_PREFIX));
            db.deleteRange(writeOptions, IDEMPOTENCY_PREFIX, prefixUpperBound(IDEMPOTENCY_PREFIX));
            db.deleteRange(writeOptions, STATUS_PREFIX, prefixUpperBound(STATUS_PREFIX));
            db.deleteRange(writeOptions, EXECUTE_AT_PREFIX, prefixUpperBound(EXECUTE_AT_PREFIX));
            resetCounters();
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to clear RocksDB intent store", e);
        }
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
    public long countByStatus(IntentStatus status) {
        AtomicLong counter = statusCounts.get(status);
        return counter != null ? counter.get() : 0L;
    }

    @Override
    public List<Intent> findByStatus(IntentStatus status, int offset, int limit) {
        // Collect extra keys to compensate for TOCTOU filtering.
        // Between the index scan and multiGet, an intent's status may change
        // concurrently. We over-fetch and filter to reduce the chance of
        // returning fewer results than requested. A full fix would require
        // RocksDB snapshot isolation across both operations.
        int fetchLimit = limit * 2;
        List<byte[]> intentKeys = new ArrayList<>(fetchLimit);
        byte[] prefix = statusPrefix(status);
        int skipped = 0;

        try (RocksIterator it = db.newIterator()) {
            it.seek(prefix);
            while (it.isValid() && intentKeys.size() < fetchLimit) {
                byte[] key = it.key();
                if (!startsWith(key, prefix)) {
                    break;
                }

                String keyStr = new String(key, StandardCharsets.UTF_8);
                int hashIdx = keyStr.indexOf('#');
                if (hashIdx < 0) {
                    it.next();
                    continue;
                }

                if (skipped < offset) {
                    skipped++;
                    it.next();
                    continue;
                }

                String intentId = keyStr.substring(hashIdx + 1);
                intentKeys.add(intentKey(intentId));
                it.next();
            }
        }

        if (intentKeys.isEmpty()) {
            return List.of();
        }

        List<Intent> result = new ArrayList<>(Math.min(limit, intentKeys.size()));
        try {
            List<byte[]> values = db.multiGetAsList(intentKeys);
            for (int i = 0; i < values.size() && result.size() < limit; i++) {
                byte[] value = values.get(i);
                if (value == null) {
                    continue;
                }
                Intent intent = IntentBinaryCodec.decode(value);
                if (intent.getStatus() == status) {
                    result.add(intent);
                }
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to batch-fetch intents by status: " + status, e);
        }
        return result;
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

    @Override
    public long getPendingCount() {
        return pendingCount.get();
    }

    private void removeIdempotency(WriteBatch batch, Intent intent) throws RocksDBException {
        String key = intent.getIdempotencyKey();
        if (key != null) {
            batch.delete(idempotencyKey(key));
        }
    }

    private void removeIndexEntries(WriteBatch batch, Intent old, Intent intent) throws RocksDBException {
        batch.delete(statusKey(old));
        batch.delete(executeAtKey(old));
        String oldKey = old.getIdempotencyKey();
        String newKey = intent.getIdempotencyKey();
        if (oldKey != null && !Objects.equals(oldKey, newKey)) {
            batch.delete(idempotencyKey(oldKey));
        }
    }

    private void incrementCounts(IntentStatus status) {
        if (status == null) {
            return;
        }
        AtomicLong counter = statusCounts.get(status);
        if (counter != null) {
            counter.incrementAndGet();
        }
        if (!status.isTerminal()) {
            pendingCount.incrementAndGet();
        }
    }

    private void decrementCounts(IntentStatus status) {
        if (status == null) {
            return;
        }
        AtomicLong counter = statusCounts.get(status);
        if (counter != null) {
            counter.decrementAndGet();
        }
        if (!status.isTerminal()) {
            pendingCount.decrementAndGet();
        }
    }

    private void initializeCounters() throws RocksDBException {
        for (IntentStatus status : IntentStatus.values()) {
            statusCounts.put(status, new AtomicLong(0));
        }
        pendingCount.set(0);
        try (RocksIterator it = db.newIterator()) {
            it.seek(INTENT_PREFIX);
            while (it.isValid()) {
                byte[] key = it.key();
                if (!startsWith(key, INTENT_PREFIX)) {
                    break;
                }

                Intent intent = IntentBinaryCodec.decode(it.value());
                incrementCounts(intent.getStatus());
                it.next();
            }
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

    private void resetCounters() {
        for (AtomicLong counter : statusCounts.values()) {
            counter.set(0);
        }
        pendingCount.set(0);
    }

    private static boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) return false;
        }
        return true;
    }
}
