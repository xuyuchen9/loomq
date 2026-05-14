package com.loomq.storage;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.store.IdempotencyResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

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
}
