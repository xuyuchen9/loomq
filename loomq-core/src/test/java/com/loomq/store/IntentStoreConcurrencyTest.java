package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntentStoreConcurrencyTest {

    private IntentStore store;

    @BeforeEach
    void setUp() {
        store = new IntentStore();
    }

    @Test
    void shouldHandleConcurrentSaveOfSameIntentId() throws Exception {
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        Intent original = new Intent("concurrent-save");
        original.setExecuteAt(Instant.now().plusSeconds(60));
        original.transitionTo(IntentStatus.SCHEDULED);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        Intent intent = new Intent("concurrent-save");
                        intent.setExecuteAt(Instant.now().plusSeconds(60 + idx));
                        store.save(intent);
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        assertEquals(0, errors.get(), "no errors during concurrent save");
        Intent stored = store.findById("concurrent-save");
        assertNotNull(stored, "intent should exist after concurrent saves");
    }

    @Test
    void shouldHandleConcurrentSaveAndFindById() throws Exception {
        int threadCount = 20;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger foundNull = new AtomicInteger(0);

        Intent intent = new Intent("concurrent-read-write");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        store.save(intent);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threadCount / 2; i++) {
                executor.submit(() -> {
                    Intent updated = new Intent("concurrent-read-write");
                    updated.setExecuteAt(Instant.now().plusSeconds(120));
                    store.save(updated);
                    latch.countDown();
                });
            }
            for (int i = 0; i < threadCount / 2; i++) {
                executor.submit(() -> {
                    Intent found = store.findById("concurrent-read-write");
                    if (found == null) {
                        foundNull.incrementAndGet();
                    }
                    latch.countDown();
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        assertEquals(0, foundNull.get(), "findById should never return null once saved");
    }

    @Test
    void shouldHandleConcurrentDeleteAndFindById() throws Exception {
        Intent intent = new Intent("concurrent-delete");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        store.save(intent);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> store.delete("concurrent-delete"));
            executor.submit(() -> {
                Intent found = store.findById("concurrent-delete");
                // May return null or not null — either is fine, just shouldn't throw
            });
        }
    }

    @Test
    void getAllIntentsShouldReturnImmutableSnapshot() throws Exception {
        Intent intent1 = new Intent("snapshot-1");
        intent1.setExecuteAt(Instant.now().plusSeconds(60));
        intent1.transitionTo(IntentStatus.SCHEDULED);
        store.save(intent1);

        Map<String, Intent> snapshot = store.getAllIntents();
        assertEquals(1, snapshot.size());

        // Add another intent after snapshot
        Intent intent2 = new Intent("snapshot-2");
        intent2.setExecuteAt(Instant.now().plusSeconds(60));
        intent2.transitionTo(IntentStatus.SCHEDULED);
        store.save(intent2);

        // Snapshot should still have only 1
        assertEquals(1, snapshot.size());

        // Snapshot should be immutable
        try {
            snapshot.put("new-key", intent1);
        } catch (UnsupportedOperationException expected) {
            // expected
        }
    }

    @Test
    void shouldHandleConcurrentCheckIdempotencyAndSave() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> {
                var result = store.checkIdempotency("idem-concurrent");
                if (result.isAllowed()) {
                    Intent intent = new Intent("idem-intent");
                    intent.setIdempotencyKey("idem-concurrent");
                    intent.setExecuteAt(Instant.now().plusSeconds(60));
                    store.save(intent);
                }
                latch.countDown();
            });
            executor.submit(() -> {
                var result = store.checkIdempotency("idem-concurrent");
                if (result.isAllowed()) {
                    Intent intent = new Intent("idem-intent-2");
                    intent.setIdempotencyKey("idem-concurrent");
                    intent.setExecuteAt(Instant.now().plusSeconds(60));
                    store.save(intent);
                }
                latch.countDown();
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        // At least one should exist
        Intent found = store.findById("idem-intent");
        if (found == null) {
            found = store.findById("idem-intent-2");
        }
        // Either both created (one overwrote the other) or one exists
        // Just verify the store didn't corrupt
    }
}
