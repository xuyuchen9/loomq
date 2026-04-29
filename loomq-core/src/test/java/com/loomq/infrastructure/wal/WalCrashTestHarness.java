package com.loomq.infrastructure.wal;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Subprocess harness for multi-process WAL crash-recovery testing.
 *
 * Spawned by WalMultiProcessCrashTest. Writes N intents in the requested mode,
 * prints READY with write/flush positions, then sleeps until killed.
 *
 * Usage: java WalCrashTestHarness <dataDir> <mode:ASYNC|DURABLE> <count>
 */
public final class WalCrashTestHarness {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WalCrashTestHarness <dataDir> <mode> <count>");
            System.exit(1);
        }

        Path dataDir = Path.of(args[0]);
        String mode = args[1].toUpperCase();
        int count = Integer.parseInt(args[2]);
        int flushIntervalMs = args.length > 3 ? Integer.parseInt(args[3]) : 1000;

        Files.createDirectories(dataDir);

        WalConfig config = new WalConfig(
            dataDir.toString(), 8, "batch", 100, false, "memory_segment",
            8, 32, 64, flushIntervalMs, 4, 1, false, false, "localhost", 9090, 30000, false
        );

        SimpleWalWriter writer = new SimpleWalWriter(config, "crash-test");
        writer.start();

        // Write intents
        for (int i = 0; i < count; i++) {
            Intent intent = new Intent("crash-" + mode.toLowerCase() + "-" + i);
            intent.setExecuteAt(Instant.now().plusSeconds(30));
            intent.setDeadline(Instant.now().plusSeconds(60));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.transitionTo(IntentStatus.SCHEDULED);
            byte[] payload = IntentBinaryCodec.encode(intent);

            if ("DURABLE".equals(mode)) {
                writer.writeDurable(payload).join();
            } else {
                writer.writeAsync(payload).join();
            }
        }

        // Flush stdout before signaling
        long writePos = writer.getWritePosition();
        long flushedPos = writer.getFlushedPosition();
        System.out.printf("READY|mode=%s|written=%d|write_pos=%d|flushed_pos=%d%n",
            mode, count, writePos, flushedPos);
        System.out.flush();

        // Sleep until killed — do NOT call close() (which would force() and ruin the crash simulation)
        Thread.sleep(Long.MAX_VALUE);
    }
}
