package com.loomq.cluster;

import java.util.Objects;

/**
 * 复制端点配置。
 *
 * 由 FailoverController 显式持有，避免把复制地址和绑定地址藏在默认字段里。
 */
public record ReplicationEndpoints(
        String primaryReplicaHost,
        int primaryReplicaPort,
        String replicaBindHost,
        int replicaBindPort
) {

    public ReplicationEndpoints {
        primaryReplicaHost = requireHost(primaryReplicaHost, "primaryReplicaHost");
        replicaBindHost = requireHost(replicaBindHost, "replicaBindHost");
        requirePort(primaryReplicaPort, "primaryReplicaPort");
        requirePort(replicaBindPort, "replicaBindPort");
    }

    private static String requireHost(String value, String fieldName) {
        String checked = Objects.requireNonNull(value, fieldName + " cannot be null");
        if (checked.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return checked;
    }

    private static void requirePort(int value, String fieldName) {
        if (value <= 0 || value > 65535) {
            throw new IllegalArgumentException(fieldName + " must be in range [1, 65535]");
        }
    }
}
