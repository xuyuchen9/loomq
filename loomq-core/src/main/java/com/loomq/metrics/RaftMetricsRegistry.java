package com.loomq.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Raft 运行指标注册表。
 *
 * 负责记录当前角色、term、commit index、lastApplied 和提交滞后。
 */
final class RaftMetricsRegistry {

    private final AtomicReference<String> raftRole = new AtomicReference<>("OFFLINE");
    private final AtomicLong raftTerm = new AtomicLong(0);
    private final AtomicLong raftCommitIndex = new AtomicLong(0);
    private final AtomicLong raftLastApplied = new AtomicLong(0);

    void updateRaftRole(String role) {
        raftRole.set(role == null || role.isBlank() ? "OFFLINE" : role);
    }

    void updateRaftTerm(long term) {
        raftTerm.set(Math.max(0, term));
    }

    void updateRaftCommitIndex(long commitIndex) {
        raftCommitIndex.set(Math.max(0, commitIndex));
    }

    void updateRaftLastApplied(long lastApplied) {
        raftLastApplied.set(Math.max(0, lastApplied));
    }

    String getRaftRole() {
        return raftRole.get();
    }

    long getRaftTerm() {
        return raftTerm.get();
    }

    long getRaftCommitIndex() {
        return raftCommitIndex.get();
    }

    long getRaftLastApplied() {
        return raftLastApplied.get();
    }

    long getRaftCommitLag() {
        return Math.max(0, raftCommitIndex.get() - raftLastApplied.get());
    }

    void reset() {
        raftRole.set("OFFLINE");
        raftTerm.set(0);
        raftCommitIndex.set(0);
        raftLastApplied.set(0);
    }
}
