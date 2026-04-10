package com.loomq.wal;

import com.loomq.entity.v5.Intent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * WAL 调试工具
 *
 * 将二进制 WAL 文件解析为可读 JSON 格式，用于调试和诊断。
 *
 * 用法：
 * java -cp loomq.jar com.loomq.wal.WalDumpTool <wal-file> [options]
 *
 * 选项：
 *   --limit N    最多解析 N 条记录
 *   --skip N     跳过前 N 条记录
 *   --json       输出为 JSON 格式
 *
 * @author loomq
 * @since v0.6.1
 */
public class WalDumpTool {

    private static final int HEADER_SIZE = 4;  // Length field
    private static final int CHECKSUM_SIZE = 4; // CRC32

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            printUsage();
            return;
        }

        String walFile = args[0];
        int limit = Integer.MAX_VALUE;
        int skip = 0;
        boolean jsonOutput = false;

        // 解析参数
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "--limit" -> limit = Integer.parseInt(args[++i]);
                case "--skip" -> skip = Integer.parseInt(args[++i]);
                case "--json" -> jsonOutput = true;
            }
        }

        Path path = Paths.get(walFile);
        if (!path.toFile().exists()) {
            System.err.println("File not found: " + walFile);
            return;
        }

        dumpWal(path, skip, limit, jsonOutput);
    }

    private static void printUsage() {
        System.out.println("Usage: WalDumpTool <wal-file> [options]");
        System.out.println("Options:");
        System.out.println("  --limit N    最多解析 N 条记录");
        System.out.println("  --skip N     跳过前 N 条记录");
        System.out.println("  --json       输出为 JSON 格式");
    }

    private static void dumpWal(Path path, int skip, int limit, boolean jsonOutput) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(fileSize, 16 * 1024 * 1024));

            long position = 0;
            int recordCount = 0;
            int skipped = 0;

            if (jsonOutput) {
                System.out.println("[");
            }

            boolean firstRecord = true;

            while (position < fileSize && recordCount < limit) {
                buffer.clear();
                int bytesRead = channel.read(buffer, position);
                if (bytesRead <= 0) break;
                buffer.flip();

                while (buffer.remaining() >= HEADER_SIZE && recordCount < limit) {
                    // 读取长度
                    int payloadLen = buffer.getInt();

                    if (payloadLen < 0 || payloadLen > 16 * 1024 * 1024) {
                        System.err.println("Invalid payload length at position " + position + ": " + payloadLen);
                        return;
                    }

                    if (buffer.remaining() < payloadLen + CHECKSUM_SIZE) {
                        // 需要更多数据
                        break;
                    }

                    // 读取 payload
                    byte[] payload = new byte[payloadLen];
                    buffer.get(payload);

                    // 读取并验证校验和
                    int storedCrc = buffer.getInt();
                    int computedCrc = calculateCrc(payload);

                    boolean crcValid = storedCrc == computedCrc;

                    if (skipped < skip) {
                        skipped++;
                    } else {
                        if (jsonOutput) {
                            if (!firstRecord) {
                                System.out.println(",");
                            }
                            firstRecord = false;
                            printJsonRecord(position, recordCount, payloadLen, crcValid, payload);
                        } else {
                            printHumanRecord(position, recordCount, payloadLen, crcValid, payload);
                        }
                        recordCount++;
                    }

                    position += HEADER_SIZE + payloadLen + CHECKSUM_SIZE;
                }

                // 如果缓冲区没有消耗完，需要调整位置
                if (buffer.hasRemaining()) {
                    position -= buffer.remaining();
                }
            }

            if (jsonOutput) {
                System.out.println("\n]");
            }

            System.out.println("\n--- Summary ---");
            System.out.println("Total records: " + recordCount);
            System.out.println("File size: " + fileSize + " bytes");
            System.out.println("Position: " + position);
        }
    }

    private static void printHumanRecord(long position, int index, int payloadLen, boolean crcValid, byte[] payload) {
        System.out.println("Record #" + index + " @ " + position);
        System.out.println("  Payload length: " + payloadLen);
        System.out.println("  CRC valid: " + crcValid);

        // 尝试解码为 Intent
        try {
            Intent intent = IntentBinaryCodec.decode(payload);
            System.out.println("  Intent ID: " + intent.getIntentId());
            System.out.println("  Status: " + intent.getStatus());
            System.out.println("  Execute At: " + intent.getExecuteAt());
            System.out.println("  Callback URL: " + (intent.getCallback() != null ? intent.getCallback().getUrl() : "null"));
        } catch (Exception e) {
            // 不是 Intent，尝试打印为字符串
            String asString = new String(payload, 0, Math.min(payload.length, 200));
            System.out.println("  Content (preview): " + asString.replace("\n", " "));
        }
        System.out.println();
    }

    private static void printJsonRecord(long position, int index, int payloadLen, boolean crcValid, byte[] payload) {
        System.out.print("  {");
        System.out.print("\"position\":" + position + ",");
        System.out.print("\"index\":" + index + ",");
        System.out.print("\"payloadLength\":" + payloadLen + ",");
        System.out.print("\"crcValid\":" + crcValid + ",");

        // 尝试解码为 Intent
        try {
            Intent intent = IntentBinaryCodec.decode(payload);
            System.out.print("\"intent\":");
            System.out.print("{");
            System.out.print("\"intentId\":\"" + escapeJson(intent.getIntentId()) + "\",");
            System.out.print("\"status\":\"" + intent.getStatus() + "\",");
            System.out.print("\"executeAt\":\"" + intent.getExecuteAt() + "\",");
            System.out.print("\"callbackUrl\":\"" + escapeJson(
                intent.getCallback() != null ? intent.getCallback().getUrl() : "") + "\"");
            System.out.print("}");
        } catch (Exception e) {
            // 作为 Base64 输出
            System.out.print("\"payloadBase64\":\"" + java.util.Base64.getEncoder().encodeToString(payload) + "\"");
        }

        System.out.print("}");
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private static int calculateCrc(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }
}
