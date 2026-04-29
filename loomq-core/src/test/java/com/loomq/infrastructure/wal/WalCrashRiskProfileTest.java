package com.loomq.infrastructure.wal;

import com.loomq.domain.intent.Intent;
import com.loomq.recovery.WalReplayManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Scientific multi-window WAL crash-recovery test.
 *
 * Spawns a child JVM that writes intents via SimpleWalWriter, kills it at
 * varying delays after write (50ms, 100ms, 500ms, 1000ms), then replays
 * the WAL. Builds a risk profile: "at T ms after write, X% at risk."
 *
 * Platform note: Windows OS flushes memory-mapped pages on process exit,
 * so data MAY survive even without explicit fsync. This test documents
 * actual behavior rather than assuming worst-case.
 */
class WalCrashRiskProfileTest {

    private static final Logger log = LoggerFactory.getLogger(WalCrashRiskProfileTest.class);
    private static final int INTENT_COUNT = 100;
    private static final int[] KILL_DELAYS_MS = {50, 100, 500, 1000};
    private static final int FLUSH_INTERVAL_MS = 2000; // long interval so daemon doesn't interfere

    @TempDir
    Path tempDir;

    @Test
    void buildAsyncCrashRiskProfile() throws Exception {
        System.out.println("RESULT_WAL_CRASH_RISK|mode=ASYNC|flush_interval_ms=" + FLUSH_INTERVAL_MS);

        List<RiskPoint> profile = new ArrayList<>();
        for (int delayMs : KILL_DELAYS_MS) {
            CrashResult r = runCrashTest("ASYNC", INTENT_COUNT, delayMs);
            int lost = r.written - r.recovered;
            long unflushed = r.writePos - r.flushedPos;
            double riskPct = r.written > 0 ? (double) lost / r.written * 100.0 : 0;
            double riskBytesPct = r.writePos > 0 ? (double) unflushed / r.writePos * 100.0 : 0;

            profile.add(new RiskPoint(delayMs, lost, unflushed, riskPct, riskBytesPct));

            log.info("ASYNC kill@{}ms: written={} recovered={} lost={} ({:.1f}%) unflushed={} ({:.1f}%)",
                delayMs, r.written, r.recovered, lost, riskPct, unflushed, riskBytesPct);

            System.out.printf("RESULT_WAL_CRASH_POINT|mode=ASYNC|kill_delay_ms=%d|written=%d|recovered=%d|lost=%d|loss_pct=%.1f|write_pos=%d|flushed_pos=%d|unflushed_pct=%.1f%n",
                delayMs, r.written, r.recovered, lost, riskPct, r.writePos, r.flushedPos, riskBytesPct);
        }

        // Summary: what's the actual risk?
        System.out.println();
        System.out.println("=== WAL ASYNC Crash Risk Profile ===");
        System.out.println("Platform: " + System.getProperty("os.name"));
        System.out.println("Flush daemon interval: " + FLUSH_INTERVAL_MS + "ms");
        System.out.println("Intents per test: " + INTENT_COUNT);
        System.out.println();
        System.out.println("KillDelay  Lost  DataAtRisk  Verdict");
        System.out.println("---------  ----  ----------  -------");
        for (RiskPoint p : profile) {
            String verdict = p.lost > 0 ? "DATA LOST — risk confirmed" :
                (p.unflushedBytes > 0 ? "OS SAVED — " + p.unflushedBytes + " bytes were at risk" :
                    "SAFE — daemon flushed before kill");
            System.out.printf("%6dms  %4d  %8d   %s%n", p.delayMs, p.lost, p.unflushedBytes, verdict);
        }
        System.out.println();
        System.out.println("Note: On this OS, memory-mapped file pages " +
            (profile.stream().allMatch(p -> p.lost == 0) ? "WERE flushed by OS on process exit." :
             "showed data loss at some kill delays."));
        System.out.println("      True power-loss / kernel-panic behavior may differ.");
    }

    @Test
    void durableModeZeroLossAllWindows() throws Exception {
        // DURABLE across all kill delays — should always be zero loss
        for (int delayMs : KILL_DELAYS_MS) {
            CrashResult r = runCrashTest("DURABLE", INTENT_COUNT, delayMs);
            assertEquals(INTENT_COUNT, r.recovered,
                "DURABLE kill@" + delayMs + "ms: lost " + (r.written - r.recovered));
            System.out.printf("RESULT_WAL_CRASH_POINT|mode=DURABLE|kill_delay_ms=%d|written=%d|recovered=%d|lost=0|loss_pct=0.0%n",
                delayMs, r.written, r.recovered);
        }
    }

    // ========== helpers ==========

    private CrashResult runCrashTest(String mode, int count, int killDelayMs) throws Exception {
        Path testDir = tempDir.resolve("crash-" + mode.toLowerCase() + "-" + killDelayMs + "ms");
        Files.createDirectories(testDir);

        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
            javaBin, "-cp", classpath,
            "com.loomq.infrastructure.wal.WalCrashTestHarness",
            testDir.toString(), mode, String.valueOf(count), String.valueOf(FLUSH_INTERVAL_MS)
        );
        pb.redirectErrorStream(true);

        Process process = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

        long writePos = 0, flushedPos = 0;
        boolean ready = false;
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            String line = reader.readLine();
            if (line == null) { if (!process.isAlive()) break; Thread.sleep(20); continue; }
            if (line.startsWith("READY|")) {
                try {
                    for (String part : line.split("\\|")) {
                        if (part.startsWith("write_pos="))
                            writePos = Long.parseLong(part.substring(part.indexOf('=') + 1));
                        if (part.startsWith("flushed_pos="))
                            flushedPos = Long.parseLong(part.substring(part.indexOf('=') + 1));
                    }
                } catch (Exception e) { log.warn("Parse error: {}", line); }
                ready = true;
                break;
            }
        }
        if (!ready) { process.destroyForcibly(); throw new RuntimeException("No READY"); }

        // Wait the specified delay, then CRASH
        Thread.sleep(killDelayMs);
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);

        // Replay
        Path walPath = testDir.resolve("crash-test").resolve("wal.bin");
        int recovered = 0;
        if (Files.exists(walPath) && Files.size(walPath) > 0) {
            WalReplayManager replayer = new WalReplayManager();
            List<Intent> intents = replayer.replay(walPath, 0);
            recovered = intents.size();
        }

        return new CrashResult(count, recovered, writePos, flushedPos);
    }

    private record CrashResult(int written, int recovered, long writePos, long flushedPos) {}
    private record RiskPoint(int delayMs, int lost, long unflushedBytes, double riskPct, double riskBytesPct) {}
}
