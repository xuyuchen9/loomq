package com.loomq.raft;

/**
 * Exposes the current Raft control-plane status to the HTTP layer.
 */
public interface RaftStatusProvider {

    /**
     * Whether Raft mode is active for this node.
     */
    boolean isRaftEnabled();

    /**
     * Current Raft role.
     */
    RaftRole role();

    /**
     * Whether this node is the current leader.
     */
    default boolean isLeader() {
        return role() == RaftRole.LEADER;
    }

    /**
     * Current leader node id, if known.
     */
    String currentLeaderId();

    /**
     * Snapshot of the current Raft health / topology state.
     */
    RaftStatusSnapshot snapshotStatus();
}
