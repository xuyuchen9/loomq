package com.loomq.config;

import org.aeonbits.owner.Config;

/**
 * WAL 配置
 */
@Config.Sources({"classpath:application.yml", "file:./config/application.yml"})
public interface WalConfig extends Config {
    @Key("wal.data_dir")
    @DefaultValue("./data/wal")
    String dataDir();

    @Key("wal.segment_size_mb")
    @DefaultValue("64")
    int segmentSizeMb();

    @Key("wal.flush_strategy")
    @DefaultValue("batch")
    String flushStrategy(); // per_record, batch, async

    @Key("wal.batch_flush_interval_ms")
    @DefaultValue("100")
    long batchFlushIntervalMs();

    @Key("wal.sync_on_write")
    @DefaultValue("false")
    boolean syncOnWrite();

    // ==================== Replication v0.4.8 ====================

    @Key("wal.replication.enabled")
    @DefaultValue("false")
    boolean isReplicationEnabled();

    @Key("wal.replication.replica_host")
    @DefaultValue("localhost")
    String replicaHost();

    @Key("wal.replication.replica_port")
    @DefaultValue("9090")
    int replicaPort();

    @Key("wal.replication.ack_timeout_ms")
    @DefaultValue("30000")
    long replicationAckTimeoutMs();

    @Key("wal.replication.require_replicated_ack")
    @DefaultValue("false")
    boolean requireReplicatedAck();
}
