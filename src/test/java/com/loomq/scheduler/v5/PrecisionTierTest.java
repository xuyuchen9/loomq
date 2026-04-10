package com.loomq.scheduler.v5;

import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.PrecisionTier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PrecisionTier 精度档位测试
 *
 * @author loomq
 * @since v0.5.1
 */
class PrecisionTierTest {

    @Test
    @DisplayName("精度档位枚举值正确")
    void testPrecisionTierValues() {
        assertEquals(5, PrecisionTier.values().length);

        assertEquals(10, PrecisionTier.ULTRA.getPrecisionWindowMs());
        assertEquals(50, PrecisionTier.FAST.getPrecisionWindowMs());
        assertEquals(100, PrecisionTier.HIGH.getPrecisionWindowMs());
        assertEquals(500, PrecisionTier.STANDARD.getPrecisionWindowMs());
        assertEquals(1000, PrecisionTier.ECONOMY.getPrecisionWindowMs());
    }

    @Test
    @DisplayName("fromString 方法正确解析")
    void testFromString() {
        assertEquals(PrecisionTier.ULTRA, PrecisionTier.fromString("ULTRA"));
        assertEquals(PrecisionTier.ULTRA, PrecisionTier.fromString("ultra"));
        assertEquals(PrecisionTier.ULTRA, PrecisionTier.fromString("UlTrA"));

        assertEquals(PrecisionTier.FAST, PrecisionTier.fromString("FAST"));
        assertEquals(PrecisionTier.HIGH, PrecisionTier.fromString("HIGH"));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("STANDARD"));
        assertEquals(PrecisionTier.ECONOMY, PrecisionTier.fromString("ECONOMY"));
    }

    @Test
    @DisplayName("fromString 对 null 或空字符串返回默认值 STANDARD")
    void testFromStringDefaults() {
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString(null));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString(""));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("   "));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("INVALID"));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("unknown"));
    }

    @Test
    @DisplayName("Intent 默认精度档位为 STANDARD")
    void testIntentDefaultPrecisionTier() {
        Intent intent = new Intent();
        assertEquals(PrecisionTier.STANDARD, intent.getPrecisionTier());
    }

    @Test
    @DisplayName("Intent 可设置精度档位")
    void testIntentSetPrecisionTier() {
        Intent intent = new Intent();
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        assertEquals(PrecisionTier.ULTRA, intent.getPrecisionTier());

        intent.setPrecisionTier(PrecisionTier.ECONOMY);
        assertEquals(PrecisionTier.ECONOMY, intent.getPrecisionTier());
    }

    @Test
    @DisplayName("Intent 构造函数保留默认精度档位")
    void testIntentConstructorPreservesDefaultTier() {
        Intent intent = new Intent("test-intent-id");
        assertEquals(PrecisionTier.STANDARD, intent.getPrecisionTier());
    }
}
