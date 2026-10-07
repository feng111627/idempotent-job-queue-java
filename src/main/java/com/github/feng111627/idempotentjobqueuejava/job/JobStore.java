package com.github.feng111627.idempotentjobqueuejava.job;

import java.util.Optional;
import java.util.Set;

/**
 * 作业存储抽象。所有状态迁移必须是原子的。
 * 实现抛出的底层异常必须包装为 {@link JobStorageException}。
 */
public interface JobStore {

    /**
     * 按幂等键原子地插入作业。
     * 幂等键已存在时不插入、不报错，返回已有作业。
     */
    SubmitResult insertIfAbsent(Job job);

    Optional<Job> findById(String id);

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

    /**
     * 原子地领取一个到期可执行的作业（PENDING，或 RETRY_WAITING 且已到重试时间），
     * 状态迁移为 RUNNING。businessKey 落在 excludedBusinessKeys 中的作业本轮跳过，
     * 用于保证同一业务键不会并发执行。没有可执行的作业时返回空。
     */
    Optional<Job> claimNext(Set<String> excludedBusinessKeys);

    /** 标记执行成功（仅 RUNNING -> SUCCEEDED 有效）。 */
    void markSucceeded(String jobId);

    /**
     * 标记执行失败：未达上限 -> RETRY_WAITING 并记录下次重试时间与失败原因；
     * 达到上限 -> DEAD 并保留失败原因。
     */
    void markFailed(String jobId, String error, RetryPolicy retryPolicy);
}
