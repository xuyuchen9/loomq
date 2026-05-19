package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TimelineServiceTest {

    private static final DeliveryHandler NOOP = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    @Nested
    @DisplayName("Timeline 构建")
    class Build {

        @Test
        @DisplayName("空调度器应返回空 upcominWakes")
        void emptyScheduler() {
            IntentStore store = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new PrecisionScheduler(store, NOOP, null);
            try {
                scheduler.start();
                Instant from = Instant.now();
                Instant to = from.plusSeconds(600);

                Map<String, Object> timeline = TimelineService.build(
                    scheduler.getCohortManager(), scheduler, from, to);

                assertNotNull(timeline);
                @SuppressWarnings("unchecked")
                List<?> wakes = (List<?>) timeline.get("upcomingWakes");
                assertNotNull(wakes);
                assertTrue(wakes.isEmpty());
                assertNotNull(timeline.get("forecast"));
            } finally {
                scheduler.stop();
                store.shutdown();
            }
        }

        @Test
        @DisplayName("调度 Intent 后应出现在 upcomingWakes 中")
        void scheduledIntentAppears() {
            IntentStore store = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new PrecisionScheduler(store, NOOP, null);
            try {
                scheduler.start();

                Intent intent = new Intent("timeline-test-1");
                intent.setExecuteAt(Instant.now().plusSeconds(30));
                intent.setDeadline(Instant.now().plusSeconds(3600));
                intent.setPrecisionTier(PrecisionTier.HIGH);
                intent.transitionTo(IntentStatus.SCHEDULED);
                store.save(intent);
                scheduler.schedule(intent);

                try { Thread.sleep(50); } catch (InterruptedException ignored) {}

                Instant from = Instant.now().minusSeconds(5);
                Instant to = from.plusSeconds(120);

                Map<String, Object> timeline = TimelineService.build(
                    scheduler.getCohortManager(), scheduler, from, to);

                @SuppressWarnings("unchecked")
                List<?> wakes = (List<?>) timeline.get("upcomingWakes");
                assertTrue(wakes.size() > 0, "Scheduled intent should appear in upcomingWakes");

                @SuppressWarnings("unchecked")
                Map<String, Object> forecast = (Map<String, Object>) timeline.get("forecast");
                assertNotNull(forecast.get("backpressureRisk"));
                assertNotNull(forecast.get("peakLoadTier"));
            } finally {
                scheduler.stop();
                store.shutdown();
            }
        }

        @Test
        @DisplayName("from >= to 时 ConcurrentSkipListMap 抛出 IllegalArgumentException")
        void fromGtToThrowsIllegalArgument() {
            IntentStore store = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new PrecisionScheduler(store, NOOP, null);
            try {
                scheduler.start();
                Instant from = Instant.now();
                Instant to = from.minusSeconds(10);

                assertThrows(IllegalArgumentException.class, () ->
                    TimelineService.build(scheduler.getCohortManager(), scheduler, from, to),
                    "ConcurrentSkipListMap rejects invalid range; route handler validates bounds first");
            } finally {
                scheduler.stop();
                store.shutdown();
            }
        }

        @Test
        @DisplayName("空 CohortManager 应返回空 forecast")
        void emptyCohortManager() {
            IntentStore store = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new PrecisionScheduler(store, NOOP, null);
            try {
                scheduler.start();
                Instant from = Instant.now();
                Instant to = from.plusSeconds(600);

                Map<String, Object> timeline = TimelineService.build(
                    scheduler.getCohortManager(), scheduler, from, to);

                @SuppressWarnings("unchecked")
                List<?> wakes = (List<?>) timeline.get("upcomingWakes");
                assertTrue(wakes.isEmpty());

                @SuppressWarnings("unchecked")
                Map<String, Object> forecast = (Map<String, Object>) timeline.get("forecast");
                assertNotNull(forecast);
                assertEquals("low", forecast.get("backpressureRisk"));
            } finally {
                scheduler.stop();
                store.shutdown();
            }
        }
    }
}
