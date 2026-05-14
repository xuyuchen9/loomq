package com.loomq.replication;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Ack protocol versioning backward/forward compatibility.
 *
 * v1 (0x01): offset + status + timestamp + replicaAppliedOffset + errorMsg
 * v2 (0x02): v1 + raftResponseLength + raftResponse
 */
class AckCodecVersionTest {

    private static byte[] encodeV1(Ack ack) {
        byte[] errorBytes = ack.getErrorMessage() != null ?
            ack.getErrorMessage().getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
        // v1: 27 + errorBytes (no version byte, no raftResponse)
        byte[] result = new byte[27 + errorBytes.length];
        int pos = 0;
        writeLong(result, pos, ack.getOffset());           pos += 8;
        result[pos++] = ack.getStatus().getCode();
        writeLong(result, pos, ack.getTimestamp().toEpochMilli()); pos += 8;
        writeLong(result, pos, ack.getReplicaAppliedOffset());     pos += 8;
        writeShort(result, pos, (short) errorBytes.length); pos += 2;
        System.arraycopy(errorBytes, 0, result, pos, errorBytes.length);
        return result;
    }

    private static void writeLong(byte[] buf, int pos, long value) {
        buf[pos] = (byte) (value >>> 56);
        buf[pos + 1] = (byte) (value >>> 48);
        buf[pos + 2] = (byte) (value >>> 40);
        buf[pos + 3] = (byte) (value >>> 32);
        buf[pos + 4] = (byte) (value >>> 24);
        buf[pos + 5] = (byte) (value >>> 16);
        buf[pos + 6] = (byte) (value >>> 8);
        buf[pos + 7] = (byte) value;
    }

    private static void writeShort(byte[] buf, int pos, short value) {
        buf[pos] = (byte) (value >>> 8);
        buf[pos + 1] = (byte) value;
    }

    @Test
    void v1EncodeShouldDecodeWithV2Decoder() {
        // Simulate a v1-encoded Ack (no raftResponse extension)
        Ack v1Ack = new Ack(100L, AckStatus.REPLICATED, Instant.now(), null, 100L);
        byte[] encoded = encodeV1(v1Ack);

        // V2 decoder should handle v1 data gracefully (no raftResponse bytes)
        Ack decoded = Ack.decode(encoded);
        assertEquals(100L, decoded.getOffset());
        assertEquals(AckStatus.REPLICATED, decoded.getStatus());
        assertNull(decoded.getRaftResponse(), "v1 data should yield null raftResponse");
    }

    // ========== manual v1 encode (simulates old format without version byte) ==========

    @Test
    void v2EncodeShouldRoundTrip() {
        // V2 encode with raftResponse
        byte[] raftPayload = new byte[]{0x01, 0x02, 0x03, 0x04};
        Ack v2Ack = Ack.raftResponse(200L, AckStatus.REPLICATED, raftPayload);
        byte[] encoded = v2Ack.encode();

        assertTrue(encoded.length >= 1 + 27 + raftPayload.length,
            "v2 encode should include version byte + raftResponse");

        Ack decoded = Ack.decode(encoded);
        assertEquals(200L, decoded.getOffset());
        assertEquals(AckStatus.REPLICATED, decoded.getStatus());
        assertNotNull(decoded.getRaftResponse(), "v2 data should have raftResponse");
        assertArrayEquals(raftPayload, decoded.getRaftResponse());
    }

    @Test
    void v2EncodeWithoutRaftResponseShouldDecode() {
        // V2 encode where raftResponse is null
        Ack ack = Ack.success(300L);
        byte[] encoded = ack.encode();

        // First byte should be 0x02 (v2)
        assertEquals(0x02, encoded[0] & 0xFF, "first byte should be v2 version");

        Ack decoded = Ack.decode(encoded);
        assertEquals(300L, decoded.getOffset());
        assertNull(decoded.getRaftResponse());
    }

    @Test
    void decodeShouldRejectTooShortData() {
        assertThrows(IllegalArgumentException.class,
            () -> Ack.decode(new byte[10]),
            "too-short data should throw");
    }
}
