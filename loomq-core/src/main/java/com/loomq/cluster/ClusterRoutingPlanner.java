package com.loomq.cluster;

import com.loomq.cluster.ClusterCoordinator.NodeHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * 集群路由计划器。
 *
 * 负责把节点配置与健康状态转换成可提交给 RoutingTable 的路由器和节点状态快照。
 */
final class ClusterRoutingPlanner {

    private static final Logger logger = LoggerFactory.getLogger(ClusterRoutingPlanner.class);

    private ClusterRoutingPlanner() {
    }

    static RoutingPlan initialPlan(ClusterConfig config) {
        return buildPlan(config, shardId -> ShardNode.State.ACTIVE);
    }

    static RoutingPlan offlinePlan(ClusterConfig config,
                                   Map<String, NodeHealth> nodeHealthMap,
                                   String offlineShardId) {
        return buildPlan(config, shardId -> {
            if (shardId.equals(offlineShardId)) {
                return ShardNode.State.OFFLINE;
            }

            NodeHealth health = nodeHealthMap.get(shardId);
            return health != null && health.status() == NodeHealth.Status.HEALTHY
                    ? ShardNode.State.ACTIVE
                    : ShardNode.State.JOINING;
        });
    }

    static RoutingPlan recoveryPlan(ClusterConfig config,
                                    RoutingTable.TableSnapshot snapshot,
                                    String recoveredShardId) {
        Map<String, ShardNode.State> currentStates = new HashMap<>(snapshot.nodeStates());
        currentStates.put(recoveredShardId, ShardNode.State.ACTIVE);
        return buildPlan(config, shardId -> currentStates.getOrDefault(shardId, ShardNode.State.JOINING));
    }

    private static RoutingPlan buildPlan(ClusterConfig config,
                                         Function<String, ShardNode.State> stateResolver) {
        ShardRouter router = new ShardRouter();
        Map<String, ShardNode.State> nodeStates = new HashMap<>();

        for (ClusterConfig.NodeConfig nodeConfig : config.getNodes()) {
            String shardId = nodeConfig.shardId();
            ShardNode.State state = stateResolver.apply(shardId);
            nodeStates.put(shardId, state);

            if (state == ShardNode.State.ACTIVE) {
                router.addNode(createNode(config, nodeConfig));
            }
        }

        return new RoutingPlan(router, Map.copyOf(nodeStates));
    }

    private static LocalShardNode createNode(ClusterConfig config, ClusterConfig.NodeConfig nodeConfig) {
        return new LocalShardNode(
                parseShardIndex(nodeConfig.shardId()),
                config.getTotalShards(),
                nodeConfig.host(),
                nodeConfig.port(),
                nodeConfig.weight()
        );
    }

    static int parseShardIndex(String shardId) {
        if (shardId != null && shardId.startsWith("shard-")) {
            try {
                return Integer.parseInt(shardId.substring(6));
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        logger.warn("Invalid shardId format: {}", shardId);
        return -1;
    }

    record RoutingPlan(ShardRouter router, Map<String, ShardNode.State> nodeStates) {
    }
}
