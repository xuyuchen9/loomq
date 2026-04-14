package com.loomq.common.exception;

/**
 * 调度异常
 *
 * 当调度器操作失败时抛出
 *
 * @author loomq
 * @since v0.6.2
 */
public class SchedulerException extends LoomQException {

    /** 调度器已满 */
    public static final String ERR_SCHEDULER_FULL = "50001";
    /** 调度失败 */
    public static final String ERR_SCHEDULE_FAILED = "50002";
    /** 无效的调度时间 */
    public static final String ERR_INVALID_SCHEDULE_TIME = "50003";

    public SchedulerException(String message) {
        super(message);
    }

    public SchedulerException(String errorCode, String message) {
        super(errorCode, message);
    }

    public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }

    public SchedulerException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    /**
     * 创建调度器已满异常
     */
    public static SchedulerException schedulerFull(String intentId) {
        return new SchedulerException(ERR_SCHEDULER_FULL,
            "Scheduler is full, cannot schedule intent: " + intentId);
    }

    /**
     * 创建调度失败异常
     */
    public static SchedulerException scheduleFailed(String intentId, Throwable cause) {
        return new SchedulerException(ERR_SCHEDULE_FAILED,
            "Failed to schedule intent: " + intentId, cause);
    }
}
