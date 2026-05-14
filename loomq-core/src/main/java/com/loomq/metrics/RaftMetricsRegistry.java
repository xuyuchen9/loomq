package com.loomq.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Raft 运行指标注册表。
 *
 * 负责记录当前角色、term、commit index、lastApplied 和提交滞后。
 */
final class RaftMetricsRegistry {

    private final AtomicReference<String> raftRole = new AtomicReference<>("OFFLINE");
    private final AtomicReference<String> raftLeaderId = new AtomicReference<>(null);
    private final AtomicLong raftTerm = new AtomicLong(0);
    private final AtomicLong raftCommitIndex = new AtomicLong(0);
    private final AtomicLong raftLastApplied = new AtomicLong(0);
    private final AtomicLong raftReplicationLag = new AtomicLong(0);
    private final AtomicInteger raftConnectedPeers = new AtomicInteger(0);
    private final AtomicInteger raftTotalPeers = new AtomicInteger(0);

    void updateRaftRole(String role) {
        raftRole.set(role == null || role.isBlank() ? "OFFLINE" : role);
    }

    void updateRaftLeaderId(String leaderId) {
        raftLeaderId.set(leaderId == null || leaderId.isBlank() ? null : leaderId);
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

    void updateRaftReplicationLag(long replicationLag) {
        raftReplicationLag.set(Math.max(0, replicationLag));
    }

    void updateRaftConnectedPeers(int connectedPeers) {
        raftConnectedPeers.set(Math.max(0, connectedPeers));
    }

    void updateRaftTotalPeers(int totalPeers) {
        raftTotalPeers.set(Math.max(0, totalPeers));
    }

    String getRaftRole() {
        return raftRole.get();
    }

    String getRaftLeaderId() {
        return raftLeaderId.get();
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

    long getRaftReplicationLag() {
        return raftReplicationLag.get();
    }

    int getRaftConnectedPeers() {
        return raftConnectedPeers.get();
    }

    int getRaftTotalPeers() {
        return raftTotalPeers.get();
    }

    void reset() {
        raftRole.set("OFFLINE");
        raftLeaderId.set(null);
        raftTerm.set(0);
        raftCommitIndex.set(0);
        raftLastApplied.set(0);
        raftReplicationLag.set(0);
        raftConnectedPeers.set(0);
        raftTotalPeers.set(0);
    }
}
