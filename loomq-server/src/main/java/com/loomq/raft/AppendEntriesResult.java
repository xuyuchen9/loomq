package com.loomq.raft;

/**
 * Result of an AppendEntries RPC.
 *
 * On success: {@code success=true, matchIndex=last index written}.
 * On failure: {@code success=false}. If {@code conflictIndex >= 0},
 * the leader can skip directly to that index for log reconciliation
 * instead of decrementing one-by-one (Raft §5.3 optimization).
 */
public class AppendEntriesResult {
    public final boolean success;
    public final long term;
    public final long matchIndex;
    /** Index of the first conflicting entry, or -1 if unknown. */
    public final long conflictIndex;

    private AppendEntriesResult(boolean s, long t, long m, long c) {
        success = s; term = t; matchIndex = m; conflictIndex = c;
    }

    public static AppendEntriesResult success(long term, long matchIndex) {
        return new AppendEntriesResult(true, term, matchIndex, -1);
    }

    public static AppendEntriesResult fail(long term) {
        return new AppendEntriesResult(false, term, -1, -1);
    }

    /** Fail with a known conflict index for efficient log backtracking. */
    public static AppendEntriesResult fail(long term, long conflictIndex) {
        return new AppendEntriesResult(false, term, -1, conflictIndex);
    }
}
