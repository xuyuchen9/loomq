package com.loomq.common;

import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.spi.WalAccessor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Time-travel debugging over the WAL.
 *
 * Scans WAL segments to reconstruct an intent's lifecycle or
 * show the system state at a given point in time.
 */
public final class WalReplayService {

    private static final Logger logger = LoggerFactory.getLogger(WalReplayService.class);
    private static final int HEADER_SIZE = 4;
    private static final int CHECKSUM_SIZE = 4;
    private static final int RECORD_OVERHEAD = HEADER_SIZE + CHECKSUM_SIZE;

    private WalReplayService() {}

    public record IntentTransition(
        String timestamp,
        String intentId,
        String fromStatus,
        String toStatus,
        long walOffset,
        long segmentIndex
    ) {}

    /**
     * List all WAL segments with metadata.
     */
    public static Map<String, Object> listSegments(WalAccessor walAccessor) {
        List<Map<String, Object>> segmentList = new ArrayList<>();
        long totalSize = 0;
        for (var seg : walAccessor.listSegments()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("index", seg.index());
            entry.put("path", seg.path().toString());
            entry.put("startOffset", seg.startOffset());
            entry.put("endOffset", seg.endOffset());
            entry.put("size", seg.size());
            segmentList.add(entry);
            totalSize += seg.size();
        }

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("segments", segmentList);
        root.put("totalSegments", segmentList.size());
        root.put("totalSizeBytes", totalSize);
        root.put("writePosition", walAccessor.getWritePosition());
        root.put("flushedPosition", walAccessor.getFlushedPosition());
        return root;
    }

    /**
     * Replay all WAL records for a given intent, returning its lifecycle timeline.
     */
    public static Map<String, Object> replayByIntentId(WalAccessor walAccessor, String intentId) {
        List<IntentTransition> transitions = scanIntentTransitions(walAccessor, intentId);

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("intentId", intentId);
        root.put("transitions", transitions);
        root.put("transitionCount", transitions.size());

        if (!transitions.isEmpty()) {
            IntentTransition first = transitions.getFirst();
            IntentTransition last = transitions.getLast();
            root.put("firstSeenAtOffset", first.walOffset());
            root.put("lastSeenAt", last.timestamp());
            root.put("finalStatus", last.toStatus());
        }

        return root;
    }

    private static List<IntentTransition> scanIntentTransitions(WalAccessor walAccessor, String intentId) {
        List<IntentTransition> transitions = new ArrayList<>();
        var segments = walAccessor.listSegments();

        for (var seg : segments) {
            long offset = seg.startOffset();
            long endOffset = seg.endOffset();

            while (offset + RECORD_OVERHEAD <= endOffset) {
                try {
                    byte[] lengthField = walAccessor.readRecord(offset, HEADER_SIZE);
                    int payloadLength = readInt32BE(lengthField, 0);
                    if (payloadLength < 0 || payloadLength > endOffset - offset - RECORD_OVERHEAD) {
                        break; // corrupted or empty padding
                    }
                    int recordLength = RECORD_OVERHEAD + payloadLength;
                    byte[] payload = walAccessor.readRecord(offset, recordLength);
                    Intent intent = IntentBinaryCodec.decode(payload);
                    if (intent.getIntentId().equals(intentId)) {
                        transitions.add(new IntentTransition(
                            intent.getUpdatedAt() != null ? intent.getUpdatedAt().toString()
                                : Instant.ofEpochMilli(System.currentTimeMillis()).toString(),
                            intent.getIntentId(),
                            null, // WAL snapshot records don't store previous status
                            intent.getStatus().name(),
                            offset,
                            seg.index()
                        ));
                    }
                    offset += recordLength;
                } catch (Exception e) {
                    logger.debug("Failed to read WAL record at offset {}: {}", offset, e.getMessage());
                    offset += RECORD_OVERHEAD; // try next aligned position
                }
            }
        }

        return transitions;
    }

    private static int readInt32BE(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24)
            | ((data[offset + 1] & 0xFF) << 16)
            | ((data[offset + 2] & 0xFF) << 8)
            | (data[offset + 3] & 0xFF);
    }
}
