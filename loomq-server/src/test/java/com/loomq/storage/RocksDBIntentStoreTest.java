package com.loomq.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.store.IdempotencyResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RocksDBIntentStoreTest {

    private Path dbPath;
    private RocksDBIntentStore store;

    @BeforeEach
    void setUp() throws Exception {
        dbPath = Files.createTempDirectory("rocksdb-test-");
        store = new RocksDBIntentStore(dbPath);
    }

    @AfterEach
    void tearDown() {
        if (store != null) store.shutdown();
    }

    @Test
    void shouldSaveAndFindById() {
        Intent intent = new Intent("test-1");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        store.save(intent);

        Intent found = store.findById("test-1");
        assertNotNull(found);
        assertEquals("test-1", found.getIntentId());
        assertEquals(PrecisionTier.STANDARD, found.getPrecisionTier());
    }

    @Test
    void shouldUpdateExistingIntent() {
        Intent intent = new Intent("test-2");
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        store.save(intent);

        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.setPrecisionTier(PrecisionTier.FAST);
        store.update(intent);

        Intent found = store.findById("test-2");
        assertEquals(PrecisionTier.FAST, found.getPrecisionTier());
        assertEquals(IntentStatus.SCHEDULED, found.getStatus());
    }

    @Test
    void shouldUpsertWithoutDoubleCounting() {
        Intent intent = new Intent("test-upsert");
        intent.transitionTo(IntentStatus.SCHEDULED);
        store.upsert(intent);

        assertEquals(1, store.countByStatus(IntentStatus.SCHEDULED));
        assertEquals(1, store.getPendingCount());

        intent.transitionTo(IntentStatus.DUE);
        store.upsert(intent);

        assertEquals(0, store.countByStatus(IntentStatus.SCHEDULED));
        assertEquals(1, store.countByStatus(IntentStatus.DUE));
        assertEquals(1, store.getPendingCount());

        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DELIVERED);
        intent.transitionTo(IntentStatus.ACKED);
        store.upsert(intent);

        assertEquals(1, store.countByStatus(IntentStatus.ACKED));
        assertEquals(0, store.getPendingCount());
    }

    @Test
    void shouldDeleteIntent() {
        Intent intent = new Intent("test-3");
        store.save(intent);
        assertNotNull(store.findById("test-3"));

        store.delete("test-3");
        assertNull(store.findById("test-3"));
    }

    @Test
    void shouldGetAllIntents() {
        for (int i = 0; i < 10; i++) {
            Intent intent = new Intent("bulk-" + i);
            intent.setExecuteAt(Instant.now().plusSeconds(60));
            store.save(intent);
        }

        Map<String, Intent> all = store.getAllIntents();
        assertEquals(10, all.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(all.containsKey("bulk-" + i));
        }
    }

    @Test
    void shouldCountByStatus() {
        Intent a = new Intent("a"); a.transitionTo(IntentStatus.SCHEDULED); store.save(a);
        Intent b = new Intent("b"); b.transitionTo(IntentStatus.SCHEDULED); store.save(b);
        Intent c = new Intent("c"); c.transitionTo(IntentStatus.SCHEDULED); c.transitionTo(IntentStatus.DUE); store.save(c);

        assertEquals(2, store.countByStatus(IntentStatus.SCHEDULED));
        assertEquals(1, store.countByStatus(IntentStatus.DUE));
        assertEquals(0, store.countByStatus(IntentStatus.ACKED));
    }

    @Test
    void shouldTrackPendingCount() {
        Intent a = new Intent("pending-a");
        Intent b = new Intent("pending-b");
        store.save(a);
        store.save(b);

        assertEquals(2, store.getPendingCount());

        store.delete("pending-a");
        assertEquals(1, store.getPendingCount());
    }

    @Test
    void shouldRestoreCountersAfterReopen() throws Exception {
        Intent a = new Intent("reopen-a");
        Intent b = new Intent("reopen-b");
        store.save(a);
        store.save(b);

        assertEquals(2, store.countByStatus(IntentStatus.CREATED));
        assertEquals(2, store.getPendingCount());

        store.shutdown();
        store = new RocksDBIntentStore(dbPath);

        assertEquals(2, store.countByStatus(IntentStatus.CREATED));
        assertEquals(2, store.getPendingCount());
    }

    @Test
    void shouldRemoveOldIdempotencyKeyOnUpdate() {
        Intent intent = new Intent("idem-update");
        intent.setIdempotencyKey("idem-old");
        store.save(intent);

        IdempotencyResult oldResult = store.checkIdempotency("idem-old");
        assertTrue(oldResult.isActive(), "old idempotency key should resolve before update");

        intent.setIdempotencyKey("idem-new");
        intent.transitionTo(IntentStatus.SCHEDULED);
        store.update(intent);

        IdempotencyResult staleResult = store.checkIdempotency("idem-old");
        assertTrue(staleResult.isNotFound(), "old idempotency key should be removed after update");

        IdempotencyResult newResult = store.checkIdempotency("idem-new");
        assertTrue(newResult.isActive(), "new idempotency key should be registered after update");
    }

    @Nested
    @DisplayName("findByStatus 分页查询")
    class FindByStatus {

        @Test
        @DisplayName("按状态过滤返回正确结果")
        void filtersByStatus() {
            Intent a = new Intent("fs-a"); a.transitionTo(IntentStatus.SCHEDULED); store.save(a);
            Intent b = new Intent("fs-b"); b.transitionTo(IntentStatus.SCHEDULED); b.transitionTo(IntentStatus.DUE); store.save(b);
            Intent c = new Intent("fs-c"); c.transitionTo(IntentStatus.SCHEDULED); store.save(c);

            List<Intent> scheduled = store.findByStatus(IntentStatus.SCHEDULED, 0, 10);
            assertEquals(2, scheduled.size());
            for (Intent i : scheduled) {
                assertEquals(IntentStatus.SCHEDULED, i.getStatus());
            }
        }

        @Test
        @DisplayName("offset/limit 分页正确")
        void offsetAndLimit() {
            for (int i = 0; i < 10; i++) {
                Intent intent = new Intent("page-" + i);
                intent.transitionTo(IntentStatus.SCHEDULED);
                store.save(intent);
            }

            List<Intent> page1 = store.findByStatus(IntentStatus.SCHEDULED, 0, 5);
            assertEquals(5, page1.size());

            List<Intent> page2 = store.findByStatus(IntentStatus.SCHEDULED, 5, 5);
            assertEquals(5, page2.size());

            List<Intent> edge = store.findByStatus(IntentStatus.SCHEDULED, 8, 5);
            assertEquals(2, edge.size());
        }

        @Test
        @DisplayName("空状态返回空列表")
        void emptyStatus() {
            List<Intent> result = store.findByStatus(IntentStatus.DEAD_LETTERED, 0, 10);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("offset 超出总数返回空")
        void offsetBeyondTotal() {
            Intent a = new Intent("offset-a"); a.transitionTo(IntentStatus.SCHEDULED); store.save(a);

            List<Intent> result = store.findByStatus(IntentStatus.SCHEDULED, 10, 5);
            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("只返回匹配状态的 intent，排除其他状态")
        void excludesOtherStatuses() {
            Intent a = new Intent("mixed-a"); a.transitionTo(IntentStatus.SCHEDULED); store.save(a);
            Intent b = new Intent("mixed-b"); b.transitionTo(IntentStatus.SCHEDULED); b.transitionTo(IntentStatus.DUE); store.save(b);
            Intent c = new Intent("mixed-c"); c.transitionTo(IntentStatus.SCHEDULED); c.transitionTo(IntentStatus.DEAD_LETTERED); store.save(c);
            Intent d = new Intent("mixed-d"); store.save(d);

            List<Intent> scheduled = store.findByStatus(IntentStatus.SCHEDULED, 0, 10);
            for (Intent i : scheduled) {
                assertEquals(IntentStatus.SCHEDULED, i.getStatus());
            }

            List<Intent> due = store.findByStatus(IntentStatus.DUE, 0, 10);
            assertEquals(1, due.size());
            assertEquals("mixed-b", due.getFirst().getIntentId());
        }
    }
}
