package com.loomq.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ClusterConfig 单元测试
 */
class ClusterConfigTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("创建基本配置应正确")
    void createBasicConfig() {
        List<ClusterConfig.NodeConfig> nodes = List.of(
                new ClusterConfig.NodeConfig("shard-0", "localhost", 8080, 100),
                new ClusterConfig.NodeConfig("shard-1", "localhost", 8081, 100)
        );

        ClusterConfig config = new ClusterConfig("test-cluster", 2, 0, nodes, false, 60000);

        assertEquals("test-cluster", config.getClusterName());
        assertEquals(2, config.getTotalShards());
        assertEquals(0, config.getLocalShardIndex());
        assertEquals("shard-0", config.getLocalShardId());
        assertEquals(2, config.getNodes().size());
        assertTrue(config.isSharded());
        assertFalse(config.isHotReloadEnabled());
    }

    @Test
    @DisplayName("单节点配置不应视为分片模式")
    void singleNodeConfig() {
        List<ClusterConfig.NodeConfig> nodes = List.of(
                new ClusterConfig.NodeConfig("shard-0", "localhost", 8080, 100)
        );

        ClusterConfig config = new ClusterConfig("single", 1, 0, nodes, false, 60000);

        assertFalse(config.isSharded());
        assertEquals(1, config.getTotalShards());
    }

    @Test
    @DisplayName("应验证参数有效性")
    void validateParameters() {
        List<ClusterConfig.NodeConfig> nodes = List.of(
                new ClusterConfig.NodeConfig("shard-0", "localhost", 8080, 100)
        );

        // 无效 totalShards
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig("test", 0, 0, nodes, false, 60000));

        // 无效 localShardIndex（负数）
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig("test", 2, -1, nodes, false, 60000));

        // 无效 localShardIndex（超出范围）
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig("test", 2, 2, nodes, false, 60000));

        // 空节点列表
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig("test", 1, 0, List.of(), false, 60000));
    }

    @Test
    @DisplayName("应检测重复 shardId")
    void detectDuplicateShardId() {
        List<ClusterConfig.NodeConfig> nodes = List.of(
                new ClusterConfig.NodeConfig("shard-0", "localhost", 8080, 100),
                new ClusterConfig.NodeConfig("shard-0", "localhost", 8081, 100)  // 重复
        );

        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig("test", 2, 0, nodes, false, 60000));
    }

    @Test
    @DisplayName("应从配置文件加载")
    void loadFromFile() throws IOException {
        String yaml = """
            cluster:
              name: "test-cluster"
              total_shards: 4
              local_shard_index: 1
              hot_reload: true
              reload_interval_ms: 30000
              nodes:
                - shard_id: "shard-0"
                  host: "node1.example.com"
                  port: 8080
                  weight: 100
                - shard_id: "shard-1"
                  host: "node2.example.com"
                  port: 8081
                  weight: 150
                - shard_id: "shard-2"
                  host: "node3.example.com"
                  port: 8082
                  weight: 100
            """;

        Path configFile = tempDir.resolve("cluster.yml");
        Files.writeString(configFile, yaml);

        ClusterConfig config = ClusterConfig.load(configFile.toString());

        assertEquals("test-cluster", config.getClusterName());
        assertEquals(4, config.getTotalShards());
        assertEquals(1, config.getLocalShardIndex());
        assertTrue(config.isHotReloadEnabled());
        assertEquals(30000, config.getHotReloadIntervalMs());
        assertEquals(3, config.getNodes().size());

        ClusterConfig.NodeConfig node0 = config.getNodeConfig(0).orElseThrow();
        assertEquals("shard-0", node0.shardId());
        assertEquals("node1.example.com", node0.host());
        assertEquals(8080, node0.port());
        assertEquals(100, node0.weight());

        ClusterConfig.NodeConfig node1 = config.getNodeConfig(1).orElseThrow();
        assertEquals(150, node1.weight());
    }

    @Test
    @DisplayName("默认单节点配置应有效")
    void defaultSingleNodeConfig() {
        ClusterConfig config = ClusterConfig.defaultSingleNodeConfig();

        assertEquals("loomq-single", config.getClusterName());
        assertEquals(1, config.getTotalShards());
        assertEquals(0, config.getLocalShardIndex());
        assertEquals(1, config.getNodes().size());
        assertFalse(config.isHotReloadEnabled());
    }

    @Test
    @DisplayName("本地多 shard 配置应有效")
    void localMultiShardConfig() {
        ClusterConfig config = ClusterConfig.localMultiShard(4, 8080);

        assertEquals("loomq-local", config.getClusterName());
        assertEquals(4, config.getTotalShards());
        assertEquals(4, config.getNodes().size());

        for (int i = 0; i < 4; i++) {
            ClusterConfig.NodeConfig node = config.getNodeConfig(i).orElseThrow();
            assertEquals("shard-" + i, node.shardId());
            assertEquals("localhost", node.host());
            assertEquals(8080 + i, node.port());
        }
    }

    @Test
    @DisplayName("NodeConfig 应验证参数")
    void nodeConfigValidation() {
        // 无效 port
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig.NodeConfig("shard-0", "localhost", 0, 100));
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig.NodeConfig("shard-0", "localhost", 70000, 100));

        // 无效 weight
        assertThrows(IllegalArgumentException.class,
                () -> new ClusterConfig.NodeConfig("shard-0", "localhost", 8080, 0));

        // null shardId
        assertThrows(NullPointerException.class,
                () -> new ClusterConfig.NodeConfig(null, "localhost", 8080, 100));

        // null host
        assertThrows(NullPointerException.class,
                () -> new ClusterConfig.NodeConfig("shard-0", null, 8080, 100));
    }

    @Test
    @DisplayName("配置变更监听器应被触发")
    void configChangeListener() {
        ClusterConfig config = ClusterConfig.defaultSingleNodeConfig();

        AtomicBoolean triggered = new AtomicBoolean(false);
        AtomicReference<ClusterConfig> oldConfig = new AtomicReference<>();
        AtomicReference<ClusterConfig> newConfig = new AtomicReference<>();

        config.addChangeListener((old, current) -> {
            oldConfig.set(old);
            newConfig.set(current);
            triggered.set(true);
        });

        ClusterConfig newCfg = ClusterConfig.localMultiShard(2, 8080);
        config.notifyChange(newCfg);

        assertTrue(triggered.get());
        assertNotNull(oldConfig.get());
    }

    @Test
    @DisplayName("获取本地节点配置应正确")
    void getLocalNodeConfig() {
        List<ClusterConfig.NodeConfig> nodes = List.of(
                new ClusterConfig.NodeConfig("shard-0", "host0", 8080, 100),
                new ClusterConfig.NodeConfig("shard-1", "host1", 8081, 100)
        );

        ClusterConfig config = new ClusterConfig("test", 2, 1, nodes, false, 60000);
        ClusterConfig.NodeConfig local = config.getLocalNodeConfig();

        assertEquals("shard-1", local.shardId());
        assertEquals("host1", local.host());
        assertEquals(8081, local.port());
    }

    @Test
    @DisplayName("获取不存在的节点应返回空")
    void getNonExistentNode() {
        List<ClusterConfig.NodeConfig> nodes = List.of(
                new ClusterConfig.NodeConfig("shard-0", "localhost", 8080, 100)
        );

        ClusterConfig config = new ClusterConfig("test", 2, 0, nodes, false, 60000);

        assertTrue(config.getNodeConfig(1).isEmpty());
    }
}
