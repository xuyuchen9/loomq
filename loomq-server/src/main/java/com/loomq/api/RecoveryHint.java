package com.loomq.api;

import java.util.List;

/**
 * 错误恢复提示，为每个 API 错误提供可操作的下一步建议。
 *
 * <p>在时态系统中，很多错误是暂态的 —— Intent 正在 DISPATCHING 是因为它此刻正在投递，
 * 而非永久不可修改。RecoveryHint 将错误从死胡同变为引导路径。</p>
 */
public record RecoveryHint(
    String suggestion,
    String relevantEndpoint,
    Long estimatedWaitMs,
    String currentState,
    List<String> transitionsTo,
    boolean temporal,
    String leaderId
) {

    public static RecoveryHint temporal(String suggestion, String relevantEndpoint,
                                        long estimatedWaitMs, String currentState,
                                        List<String> transitionsTo) {
        return new RecoveryHint(suggestion, relevantEndpoint, estimatedWaitMs,
            currentState, transitionsTo, true, null);
    }

    public static RecoveryHint nonTemporal(String suggestion, String relevantEndpoint) {
        return new RecoveryHint(suggestion, relevantEndpoint, null, null, null, false, null);
    }

    public static RecoveryHint raftRedirect(String suggestion, String leaderId) {
        return new RecoveryHint(suggestion, null, null, null, null, false, leaderId);
    }
}
