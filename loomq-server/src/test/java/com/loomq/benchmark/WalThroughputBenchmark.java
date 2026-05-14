package com.loomq.benchmark;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wal.IntentBinaryCodec;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

@Tag("benchmark")
public class WalThroughputBenchmark {

    public static void main(String[] args) throws Exception {
        new WalThroughputBenchmark().run();
    }

    @Test
    void run() throws Exception {
        int count = Integer.getInteger("benchmark.count", 10000);
        Path dir = Files.createTempDirectory("wal-bench-");
        long[] durations = new long[2];
        double[] throughputs = new double[2];

        String[] modes = {"DURABLE", "ASYNC"};
        for (int m = 0; m < modes.length; m++) {
            WalConfig cfg = new WalConfig(dir.toString(), 1, "batch", 100, false, "memory_segment",
                1, 8, 64, 10, 4, 1, false);
            SimpleWalWriter writer = new SimpleWalWriter(cfg, "bench");
            writer.start();

            Intent intent = new Intent("bench");
            intent.setPrecisionTier(com.loomq.domain.intent.PrecisionTier.STANDARD);
            intent.transitionTo(IntentStatus.SCHEDULED);
            byte[] payload = IntentBinaryCodec.encode(intent);

            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                if ("DURABLE".equals(modes[m])) writer.writeDurable(payload).join();
                else writer.writeAsync(payload).join();
            }
            long dur = (System.nanoTime() - start) / 1_000_000;
            double tp = count / (dur / 1000.0);
            durations[m] = dur;
            throughputs[m] = tp;

            System.out.printf("RESULT|wal_throughput|mode=%s|count=%d|duration_ms=%d|throughput=%.0f%n",
                modes[m], count, dur, tp);
            writer.close();
        }

        System.out.println();
        System.out.println("  WAL Write Throughput (" + count + " intents, 128B payload)");
        System.out.println("  ┌──────────┬──────────────┬────────────────┐");
        System.out.println("  │ Mode     │ Duration(ms) │ Throughput(/s) │");
        System.out.println("  ├──────────┼──────────────┼────────────────┤");
        System.out.printf ("  │ %-8s │ %12d │ %14.0f │%n", "DURABLE", durations[0], throughputs[0]);
        System.out.printf ("  │ %-8s │ %12d │ %14.0f │%n", "ASYNC", durations[1], throughputs[1]);
        System.out.println("  └──────────┴──────────────┴────────────────┘");
    }
}
