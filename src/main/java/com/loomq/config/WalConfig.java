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

    // ==================== Memory Segment WAL (v0.7.0) ====================

    @Key("wal.engine")
    @DefaultValue("memory_segment")
    String engine(); // memory_segment | file_channel | async

    @Key("wal.memory_segment.initial_size_mb")
    @DefaultValue("64")
    int memorySegmentInitialSizeMb();

    @Key("wal.memory_segment.max_size_mb")
    @DefaultValue("1024")
    int memorySegmentMaxSizeMb();

    @Key("wal.memory_segment.flush_threshold_kb")
    @DefaultValue("64")
    int memorySegmentFlushThresholdKb();

    @Key("wal.memory_segment.flush_interval_ms")
    @DefaultValue("10")
    long memorySegmentFlushIntervalMs();

    @Key("wal.memory_segment.stripe_count")
    @DefaultValue("16")
    int memorySegmentStripeCount();

    @Key("wal.memory_segment.min_batch_size")
    @DefaultValue("100")
    int memorySegmentMinBatchSize();

    @Key("wal.memory_segment.adaptive_flush_enabled")
    @DefaultValue("true")
    boolean memorySegmentAdaptiveFlushEnabled();

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
