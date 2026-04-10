package com.loomq.wal;

import com.loomq.entity.v5.*;
import com.loomq.replication.AckLevel;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Intent 二进制编解码器测试
 *
 * @author loomq
 * @since v0.6.1
 */
class IntentBinaryCodecTest {

    @Test
    void testEncodeDecode_basicIntent() {
        // 创建测试 Intent
        Intent original = new Intent();
        // 使用毫秒精度时间，避免纳秒精度丢失
        Instant executeAt = Instant.ofEpochMilli(System.currentTimeMillis() + 60000);
        Instant deadline = Instant.ofEpochMilli(System.currentTimeMillis() + 3600000);
        original.setExecuteAt(executeAt);
        original.setDeadline(deadline);
        original.setShardKey("test-shard");
        original.setPrecisionTier(PrecisionTier.STANDARD);
        original.setAckLevel(AckLevel.DURABLE);
        original.setIdempotencyKey("test-idem-123");
        original.transitionTo(IntentStatus.SCHEDULED);

        // 编码
        byte[] encoded = IntentBinaryCodec.encode(original);
        assertNotNull(encoded);
        assertTrue(encoded.length > 0);

        // 解码
        Intent decoded = IntentBinaryCodec.decode(encoded);

        // 验证
        assertEquals(original.getExecuteAt(), decoded.getExecuteAt());
        assertEquals(original.getDeadline(), decoded.getDeadline());
        assertEquals(original.getShardKey(), decoded.getShardKey());
        assertEquals(original.getPrecisionTier(), decoded.getPrecisionTier());
        assertEquals(original.getAckLevel(), decoded.getAckLevel());
        assertEquals(original.getIdempotencyKey(), decoded.getIdempotencyKey());
    }

    @Test
    void testEncodeDecode_withCallback() {
        Intent original = new Intent();
        original.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 30000));
        original.setDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 300000));
        original.setShardKey("callback-test");

        Callback callback = new Callback("http://localhost:8080/webhook");
        callback.setMethod("POST");
        callback.setHeaders(Map.of("X-Custom", "value"));
        callback.setBody("{\"test\": true}");
        original.setCallback(callback);

        original.transitionTo(IntentStatus.SCHEDULED);

        // 编解码
        byte[] encoded = IntentBinaryCodec.encode(original);
        Intent decoded = IntentBinaryCodec.decode(encoded);

        // 验证
        assertNotNull(decoded.getCallback());
        assertEquals("http://localhost:8080/webhook", decoded.getCallback().getUrl());
        assertEquals("POST", decoded.getCallback().getMethod());
        assertEquals("{\"test\": true}", decoded.getCallback().getBody().toString());
    }

    @Test
    void testEncodeDecode_withTags() {
        Intent original = new Intent();
        original.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 10000));
        original.setDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 100000));
        original.setShardKey("tags-test");
        original.setTags(Map.of(
            "env", "production",
            "service", "order-service"
        ));

        original.transitionTo(IntentStatus.SCHEDULED);

        // 编解码
        byte[] encoded = IntentBinaryCodec.encode(original);
        Intent decoded = IntentBinaryCodec.decode(encoded);

        // 验证
        assertNotNull(decoded.getTags());
        assertEquals(2, decoded.getTags().size());
        assertEquals("production", decoded.getTags().get("env"));
        assertEquals("order-service", decoded.getTags().get("service"));
    }

    @Test
    void testEncodeDecode_allPrecisionTiers() {
        for (PrecisionTier tier : PrecisionTier.values()) {
            Intent original = new Intent();
            original.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 10000));
            original.setDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 100000));
            original.setShardKey("tier-test-" + tier.name());
            original.setPrecisionTier(tier);
            original.transitionTo(IntentStatus.SCHEDULED);

            byte[] encoded = IntentBinaryCodec.encode(original);
            Intent decoded = IntentBinaryCodec.decode(encoded);

            assertEquals(tier, decoded.getPrecisionTier(),
                "Precision tier mismatch for: " + tier.name());
        }
    }

    @Test
    void testEncode_sizeEstimate() {
        Intent intent = new Intent();
        intent.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60000));
        intent.setDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 3600000));
        intent.setShardKey("size-test");
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.setAckLevel(AckLevel.DURABLE);
        intent.setIdempotencyKey("idem-key-12345");
        intent.transitionTo(IntentStatus.SCHEDULED);

        byte[] encoded = IntentBinaryCodec.encode(intent);

        // 预期大小约 100-200 字节
        System.out.println("Encoded size: " + encoded.length + " bytes");
        assertTrue(encoded.length < 500, "Encoded size should be compact");
    }

    @Test
    void testEncodeDecode_multipleAttempts() {
        Intent original = new Intent();
        original.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 10000));
        original.setDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 100000));
        original.setShardKey("attempts-test");

        // 模拟多次尝试
        original.incrementAttempts();
        original.incrementAttempts();
        original.incrementAttempts();

        original.transitionTo(IntentStatus.SCHEDULED);

        // 编解码
        byte[] encoded = IntentBinaryCodec.encode(original);
        Intent decoded = IntentBinaryCodec.decode(encoded);

        assertEquals(3, decoded.getAttempts());
    }
}
