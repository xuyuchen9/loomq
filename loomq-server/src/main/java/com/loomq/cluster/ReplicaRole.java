package com.loomq.cluster;

/**
 * 副本角色枚举
 *
 * @author loomq
 * @since v0.3+
 */
public enum ReplicaRole {
    LEADER,     // 主节点：负责写入和调度
    FOLLOWER,   // 从节点：复制 WAL，被动等待
    CANDIDATE   // 候选者：选举过程中
}
