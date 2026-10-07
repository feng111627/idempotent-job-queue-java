package com.github.feng111627.jobqueue.store;

import com.github.feng111627.jobqueue.core.Job;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface JobStore {

    /** 按幂等键写入；已存在时返回已有作业，否则返回 null。 */
    Job saveIfAbsent(Job job);

    Optional<Job> findById(String jobId);

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

    /** 领取一个到期可执行的作业（PENDING 且 nextAttemptAt <= now），置为 RUNNING。 */
    Optional<Job> claimNextDue(Instant now);

    void markSucceeded(String jobId, Instant now);

    /** 记录失败：还有重试额度则回到 PENDING 并设置下次执行时间，否则进入 DEAD。 */
    void recordFailure(String jobId, String error, int maxAttempts, java.time.Duration nextDelay, Instant now);

    List<Job> listDead();

    /** 对同一业务键串行化执行。 */
    <T> T withBusinessKeyLock(String businessKey, java.util.function.Supplier<T> action);
}
