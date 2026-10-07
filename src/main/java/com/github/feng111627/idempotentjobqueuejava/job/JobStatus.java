package com.github.feng111627.idempotentjobqueuejava.job;

/**
 * 作业生命周期状态。
 */
public enum JobStatus {
    /** 已入队，等待执行 */
    PENDING,
    /** 正在执行 */
    RUNNING,
    /** 执行成功，终态 */
    SUCCEEDED,
    /** 执行失败，等待按退避策略重试 */
    RETRY_WAITING,
    /** 超过重试上限，进入死信，终态 */
    DEAD
}
