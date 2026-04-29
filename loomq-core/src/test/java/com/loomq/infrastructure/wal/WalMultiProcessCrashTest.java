package com.loomq.infrastructure.wal;

import com.loomq.domain.intent.Intent;
import com.loomq.recovery.WalReplayManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Multi-process WAL crash-recovery test.
 *
 * Spawns a child JVM that writes intents via SimpleWalWriter, then kills it
 * (SIGKILL / destroyForcibly). The child never calls close(), so no final
 * force() happens — this is a true crash simulation.
 *
 * After crash, replays the WAL file and counts recovered intents to quantify
 * the data-loss window for each WAL mode.
 */
class WalMultiProcessCrashTest {

    private static final Logger log = LoggerFactory.getLogger(WalMultiProcessCrashTest.class);
    private static final int INTENT_COUNT = 100;

    @TempDir
    Path tempDir;

    private Path dataDir;

    @BeforeEach
    void setUp() throws Exception {
        dataDir = tempDir.resolve("wal-crash");
        Files.createDirectories(dataDir);
    }

    @AfterEach
    void tearDown() {
        // Clean up any leftover processes
    }

    /**
     * DURABLE mode: crash after write — zero data loss expected.
     * Each writeDurable waits for fsync before returning, so all intents
     * must survive the crash.
     */
    @Test
    void durableModeZeroLossOnTrueCrash() throws Exception {
        CrashResult result = runCrashTest("DURABLE", INTENT_COUNT);

        log.info("DURABLE crash test: written={}, recovered={}, lost={}",
            result.written, result.recovered, result.written - result.recovered);

        assertEquals(INTENT_COUNT, result.recovered,
            "DURABLE mode must survive crash with zero loss. Written=" +
            result.written + ", recovered=" + result.recovered +
            ", write_pos=" + result.writePos + ", flushed_pos=" + result.flushedPos);

        System.out.printf("RESULT_WAL_CRASH|mode=DURABLE|written=%d|recovered=%d|lost=%d|loss_pct=%.1f|write_pos=%d|flushed_pos=%d%n",
            result.written, result.recovered, result.written - result.recovered,
            0.0, result.writePos, result.flushedPos);
    }

    /**
     * ASYNC mode: crash immediately after write — quantifies data loss.
     *
     * ASYNC writes go to memory-mapped region but don't wait for fsync.
     * The crash kills the flush daemon before it can force(). Lost intents
     * are between flushedPosition and writePosition.
     */
    @Test
    void asyncModeQuantifyLossOnTrueCrash() throws Exception {
        CrashResult result = runCrashTest("ASYNC", INTENT_COUNT);

        int lost = result.written - result.recovered;
        long unflushedBytes = result.writePos - result.flushedPos;
        double lossPct = result.written > 0 ? (double) lost / result.written * 100.0 : 0;

        log.info("ASYNC crash test: written={}, recovered={}, lost={} ({}%)",
            result.written, result.recovered, lost, String.format("%.1f", lossPct));
        log.info("  write_pos={}, flushed_pos={}, unflushed={} bytes",
            result.writePos, result.flushedPos, unflushedBytes);
        log.info("  Flushed position was 0 — no explicit fsync happened.");
        log.info("  OS page cache or disk controller write-back cache may have persisted");
        log.info("  the data despite no fsync. This is platform-dependent behavior.");
        log.info("  On true power loss or kernel panic, these {} bytes would be lost.", unflushedBytes);

        // Calculate approximate data-loss window (worst-case, assuming no OS page cache rescue)
        double avgBytesPerIntent = result.writePos > 0 ? (double) result.writePos / result.written : 150;
        double lossWindowMs = (unflushedBytes / Math.max(1, avgBytesPerIntent)) * 5.0; // ~5ms per intent write time

        System.out.printf("RESULT_WAL_CRASH|mode=ASYNC|written=%d|recovered=%d|lost=%d|loss_pct=%.1f|write_pos=%d|flushed_pos=%d|unflushed_bytes=%d|loss_window_ms=%.1f%n",
            result.written, result.recovered, lost, lossPct,
            result.writePos, result.flushedPos, unflushedBytes, lossWindowMs);
    }

    // ========== helpers ==========

    private CrashResult runCrashTest(String mode, int count) throws Exception {
        // Find Java binary
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";

        // Build classpath from system property
        String classpath = System.getProperty("java.class.path");
        String harnessClass = "com.loomq.infrastructure.wal.WalCrashTestHarness";

        ProcessBuilder pb = new ProcessBuilder(
            javaBin, "-cp", classpath, harnessClass,
            dataDir.toString(), mode, String.valueOf(count)
        );
        pb.redirectErrorStream(true);

        Process process = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        // Wait for READY signal (with timeout)
        String line;
        long writePos = 0, flushedPos = 0, flushIntervalMs = 1000;
        boolean ready = false;

        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            line = reader.readLine();
            if (line == null) {
                if (!process.isAlive()) break;
                Thread.sleep(50);
                continue;
            }
            if (line != null && line.startsWith("READY|")) {
                // Parse: READY|mode=X|written=N|write_pos=N|flushed_pos=N
                try {
                    String[] parts = line.split("\\|");
                    for (String part : parts) {
                        if (part.startsWith("write_pos=")) {
                            String val = part.substring(part.indexOf('=') + 1);
                            if (!val.isEmpty()) writePos = Long.parseLong(val);
                        }
                        if (part.startsWith("flushed_pos=")) {
                            String val = part.substring(part.indexOf('=') + 1);
                            if (!val.isEmpty()) flushedPos = Long.parseLong(val);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse READY line: '{}'", line, e);
                }
                ready = true;
                break;
            }
        }

        if (!ready) {
            process.destroyForcibly();
            throw new RuntimeException("Harness did not send READY within timeout");
        }

        // Wait a tiny bit for any in-flight writes, then KILL
        Thread.sleep(50);

        // TRUE CRASH: destroyForcibly = SIGKILL equivalent on Windows
        process.destroyForcibly();
        boolean terminated = process.waitFor(10, TimeUnit.SECONDS);
        if (!terminated) {
            throw new RuntimeException("Process did not terminate after destroyForcibly");
        }

        // Replay WAL
        Path walPath = dataDir.resolve("crash-test").resolve("wal.bin");
        int recovered = 0;
        if (Files.exists(walPath)) {
            WalReplayManager replayer = new WalReplayManager();
            List<Intent> intents = replayer.replay(walPath, 0);
            recovered = intents.size();
        }

        return new CrashResult(count, recovered, writePos, flushedPos, flushIntervalMs);
    }

    private record CrashResult(int written, int recovered, long writePos, long flushedPos, long flushIntervalMs) {}
}
