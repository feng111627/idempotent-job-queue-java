package com.github.feng111627.jobqueue.core;

import com.github.feng111627.jobqueue.error.JobStorageException;
import com.github.feng111627.jobqueue.store.JobStore;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class JobService {

    private final JobStore store;
    private final RetryPolicy retryPolicy;
    private final Clock clock;

    @org.springframework.beans.factory.annotation.Autowired
    public JobService(JobStore store, RetryPolicy retryPolicy) {
        this(store, retryPolicy, Clock.systemUTC());
    }

    public JobService(JobStore store, RetryPolicy retryPolicy, Clock clock) {
        this.store = store;
        this.retryPolicy = retryPolicy;
        this.clock = clock;
    }

    /**
     * 幂等提交：同一幂等键只生成一个作业。
     * 重复提交返回 ALREADY_EXISTS 和已存在的作业 ID，不抛模糊异常。
     */
    public SubmitResult submit(String idempotencyKey, String businessKey, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (businessKey == null || businessKey.isBlank()) {
            throw new IllegalArgumentException("businessKey must not be blank");
        }
        Job job = new Job(UUID.randomUUID().toString(), idempotencyKey, businessKey, payload,
                retryPolicy.getMaxAttempts(), Instant.now(clock));
        Job existing;
        try {
            existing = store.saveIfAbsent(job);
        } catch (JobStorageException e) {
            throw e;
        } catch (RuntimeException e) {
            // 存储层底层异常统一包装为可区分的错误类型，不原样透出
            throw new JobStorageException("failed to submit job with idempotency key " + idempotencyKey, e);
        }
        if (existing != null) {
            return SubmitResult.alreadyExists(existing.getId());
        }
        return SubmitResult.created(job.getId());
    }

    public Optional<Job> findById(String jobId) {
        return store.findById(jobId);
    }

    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        return store.findByIdempotencyKey(idempotencyKey);
    }

    public List<Job> listDead() {
        return store.listDead();
    }
}
