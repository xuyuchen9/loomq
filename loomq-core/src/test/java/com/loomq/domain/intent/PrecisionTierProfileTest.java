package com.loomq.domain.intent;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrecisionTierProfileTest {

    @Test
    void shouldCreateWithAllSixFields() {
        PrecisionTierProfile p = new PrecisionTierProfile(100, 50, 5, 50, 4, 800);
        assertEquals(100, p.precisionWindowMs());
        assertEquals(50, p.maxConcurrency());
        assertEquals(5, p.batchSize());
        assertEquals(50, p.batchWindowMs());
        assertEquals(4, p.consumerCount());
        assertEquals(800, p.dispatchQueueCapacity());
    }

    @Test
    void shouldDefaultQueueCapacityToMaxConcurrencyTimes16() {
        PrecisionTierProfile p = new PrecisionTierProfile(500, 50, 20, 100, 3);
        assertEquals(800, p.dispatchQueueCapacity());
    }

    @Test
    void shouldRejectZeroPrecisionWindowMs() {
        assertThrows(IllegalArgumentException.class, () ->
            new PrecisionTierProfile(0, 50, 1, 10, 4, 800));
    }

    @Test
    void shouldRejectNegativePrecisionWindowMs() {
        assertThrows(IllegalArgumentException.class, () ->
            new PrecisionTierProfile(-10, 50, 1, 10, 4, 800));
    }

    @Test
    void shouldRejectZeroMaxConcurrency() {
        assertThrows(IllegalArgumentException.class, () ->
            new PrecisionTierProfile(100, 0, 1, 10, 4, 800));
    }

    @Test
    void shouldRejectZeroBatchSize() {
        assertThrows(IllegalArgumentException.class, () ->
            new PrecisionTierProfile(100, 50, 0, 10, 4, 800));
    }

    @Test
    void shouldAllowZeroBatchWindowMs() {
        assertDoesNotThrow(() -> new PrecisionTierProfile(100, 50, 1, 0, 4, 800));
    }

    @Test
    void shouldRejectNegativeBatchWindowMs() {
        assertThrows(IllegalArgumentException.class, () ->
            new PrecisionTierProfile(100, 50, 1, -5, 4, 800));
    }

    @Test
    void shouldRejectZeroConsumerCount() {
        assertThrows(IllegalArgumentException.class, () ->
            new PrecisionTierProfile(100, 50, 1, 10, 0, 800));
    }

    @Test
    void shouldRejectZeroDispatchQueueCapacity() {
        assertThrows(IllegalArgumentException.class, () ->
            new PrecisionTierProfile(100, 50, 1, 10, 4, 0));
    }

    @Test
    void isBatchEnabledShouldReturnTrueWhenBatchSizeGreaterThanOne() {
        PrecisionTierProfile p = new PrecisionTierProfile(100, 50, 20, 100, 3);
        assertTrue(p.isBatchEnabled());
    }

    @Test
    void isBatchEnabledShouldReturnFalseWhenBatchSizeIsOne() {
        PrecisionTierProfile p = new PrecisionTierProfile(10, 200, 1, 5, 16);
        assertFalse(p.isBatchEnabled());
    }
}
