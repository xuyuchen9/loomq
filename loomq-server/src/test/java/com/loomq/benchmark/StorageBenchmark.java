package com.loomq.benchmark;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.storage.RocksDBIntentStore;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Random;

@Tag("benchmark")
public class StorageBenchmark {

    public static void main(String[] args) throws Exception {
        new StorageBenchmark().run();
    }

    private static final int COUNT = 10000;
    private static final int READ_COUNT = 5000;

    @Test
    void run() throws Exception {
        long[] saveMs = new long[2];
        long[] readMs = new long[2];
        String[] names = {"concurrent", "rocksdb"};

        benchStore(names[0], new ConcurrentIntentStore(), saveMs, readMs, 0);
        Path dbPath = Files.createTempDirectory("storage-bench-");
        benchStore(names[1], new RocksDBIntentStore(dbPath), saveMs, readMs, 1);

        System.out.println();
        System.out.println("  Storage Engine Comparison (" + COUNT + " save + " + READ_COUNT + " read)");
        System.out.println("  ┌──────────────┬──────────┬─────────┐");
        System.out.println("  │ Engine       │ Save(ms) │ Read(ms)│");
        System.out.println("  ├──────────────┼──────────┼─────────┤");
        System.out.printf ("  │ %-12s │ %8d │ %7d │%n", "Concurrent", saveMs[0], readMs[0]);
        System.out.printf ("  │ %-12s │ %8d │ %7d │%n", "RocksDB", saveMs[1], readMs[1]);
        System.out.println("  └──────────────┴──────────┴─────────┘");
    }

    private void benchStore(String type, IntentStore store, long[] outSave, long[] outRead, int idx) {
        long saveStart = System.nanoTime();
        for (int i = 0; i < COUNT; i++) {
            Intent intent = new Intent(type + "-" + i);
            intent.setExecuteAt(Instant.now().plusSeconds(60));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.transitionTo(IntentStatus.SCHEDULED);
            store.save(intent);
        }
        outSave[idx] = (System.nanoTime() - saveStart) / 1_000_000;

        Random rng = new Random(42);
        long readStart = System.nanoTime();
        for (int i = 0; i < READ_COUNT; i++) {
            store.findById(type + "-" + rng.nextInt(COUNT));
        }
        outRead[idx] = (System.nanoTime() - readStart) / 1_000_000;

        System.out.printf("RESULT|storage|type=%s|save_ms=%d|read_ms=%d%n", type, outSave[idx], outRead[idx]);
        store.shutdown();
    }
}
