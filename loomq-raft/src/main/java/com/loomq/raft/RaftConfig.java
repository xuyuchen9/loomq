package com.loomq.raft;

import java.util.List;

public record RaftConfig(String nodeId, List<String> peers, String dataDir,
        long electionMinMs, long electionMaxMs, long heartbeatMs) {
    public static RaftConfig singleNode(String nodeId) {
        return new RaftConfig(nodeId, List.of(), "./raft-data", 150, 300, 50);
    }
    public static RaftConfig cluster(String nodeId, List<String> peers) {
        return new RaftConfig(nodeId, peers, "./raft-data", 150, 300, 50);
    }
}
