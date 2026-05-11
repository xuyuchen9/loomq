package com.loomq.recovery;

import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.spi.WalAccessor;
import com.loomq.spi.WalAccessor.WalSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * WAL 回放器。
 *
 * 负责从 WAL 文件中按顺序回放 Intent 的最新状态，作为快照之后的增量补齐。
 * 支持单文件和段文件两种模式。
 */
public final class WalReplayManager {

    private static final Logger logger = LoggerFactory.getLogger(WalReplayManager.class);
    private static final int RECORD_OVERHEAD = 8;

    /**
     * 从 WalAccessor（段文件模式）回放 WAL。
     *
     * @param walAccessor WAL 访问器
     * @param startOffset 起始全局偏移
     * @param consumer    每条 intent 的应用回调
     * @return 回放条数
     */
    public int replay(WalAccessor walAccessor, long startOffset, Consumer<Intent> consumer) {
        if (walAccessor == null) {
            return 0;
        }

        List<WalSegment> segments = walAccessor.listSegments();
        if (segments.isEmpty()) {
            return 0;
        }

        int restored = 0;
        long position = Math.max(0, startOffset);

        for (WalSegment seg : segments) {
            if (seg.endOffset() <= position) continue;  // 跳过已回放段

            // 段内起始位置
            long segStart = Math.max(position, seg.startOffset());
            long segEnd = seg.endOffset();

            try (FileChannel channel = FileChannel.open(seg.path(), StandardOpenOption.READ)) {
                long localPos = segStart - seg.startOffset();

                while (localPos + RECORD_OVERHEAD <= segEnd - seg.startOffset()) {
                    ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
                    if (!readFully(channel, localPos, lengthBuffer)) break;
                    lengthBuffer.flip();
                    int payloadLength = lengthBuffer.getInt();
                    if (payloadLength <= 0) break;

                    long recordSize = RECORD_OVERHEAD + payloadLength;
                    if (localPos + recordSize > segEnd - seg.startOffset()) break;

                    ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
                    if (!readFully(channel, localPos + Integer.BYTES, payloadBuffer)) break;

                    ByteBuffer crcBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
                    if (!readFully(channel, localPos + Integer.BYTES + payloadLength, crcBuffer)) break;
                    crcBuffer.flip();
                    int expectedCrc = crcBuffer.getInt();

                    byte[] payload = payloadBuffer.array();
                    int actualCrc = calculateCrc(payload);
                    if (actualCrc != expectedCrc) {
                        logger.warn("Stopping WAL replay due to CRC mismatch at segment {}, offset {}", seg.index(), seg.startOffset() + localPos);
                        break;
                    }

                    consumer.accept(IntentBinaryCodec.decode(payload));
                    restored++;
                    localPos += recordSize;
                    position = seg.startOffset() + localPos;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to replay WAL segment: " + seg.path(), e);
            }
        }

        return restored;
    }

    /**
     * 从单个 WAL 文件回放（向后兼容）。
     *
     * @param walPath     WAL 文件路径
     * @param startOffset 起始回放偏移
     * @return 回放得到的 Intent 列表
     */
    public List<Intent> replay(Path walPath, long startOffset) {
        if (walPath == null || !Files.exists(walPath)) {
            return List.of();
        }

        List<Intent> intents = new ArrayList<>();
        replay(walPath, startOffset, intents::add);
        return intents;
    }

    /**
     * 从单个 WAL 文件流式回放（向后兼容）。
     */
    public int replay(Path walPath, long startOffset, Consumer<Intent> consumer) {
        if (walPath == null || !Files.exists(walPath)) {
            return 0;
        }

        int restored = 0;

        try (FileChannel channel = FileChannel.open(walPath, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            long position = Math.max(0, startOffset);

            while (position + RECORD_OVERHEAD <= fileSize) {
                ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
                if (!readFully(channel, position, lengthBuffer)) break;
                lengthBuffer.flip();
                int payloadLength = lengthBuffer.getInt();
                if (payloadLength <= 0) break;

                long recordSize = RECORD_OVERHEAD + payloadLength;
                if (position + recordSize > fileSize) break;

                ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadLength);
                if (!readFully(channel, position + Integer.BYTES, payloadBuffer)) break;

                ByteBuffer crcBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
                if (!readFully(channel, position + Integer.BYTES + payloadLength, crcBuffer)) break;
                crcBuffer.flip();
                int expectedCrc = crcBuffer.getInt();

                byte[] payload = payloadBuffer.array();
                int actualCrc = calculateCrc(payload);
                if (actualCrc != expectedCrc) {
                    logger.warn("Stopping WAL replay due to CRC mismatch at offset {}", position);
                    break;
                }

                consumer.accept(IntentBinaryCodec.decode(payload));
                restored++;
                position += recordSize;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to replay WAL: " + walPath, e);
        }

        return restored;
    }

    private boolean readFully(FileChannel channel, long position, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) return false;
            position += read;
        }
        return true;
    }

    private int calculateCrc(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }
}
