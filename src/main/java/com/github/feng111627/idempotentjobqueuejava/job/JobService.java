package com.github.feng111627.idempotentjobqueuejava.job;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/**
 * 作业提交与查询门面。
 *
 * 幂等语义：同一 idempotencyKey 无论被并发提交多少次，
 * 只有一次返回 CREATED，其余返回 ALREADY_EXISTS 并携带已存在的作业，
 * 不会重复入队、不会抛出含糊的异常。
 */
@Service
public class JobService {

    private final JobStore jobStore;
    private final int defaultMaxAttempts;

    public JobService(JobStore jobStore,
                      @Value("${jobqueue.max-attempts:3}") int defaultMaxAttempts) {
        this.jobStore = jobStore;
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    public SubmitResult submit(String idempotencyKey, String businessKey, String payload) {
        return submit(idempotencyKey, businessKey, payload, defaultMaxAttempts);
    }

    public SubmitResult submit(String idempotencyKey, String businessKey, String payload, int maxAttempts) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new JobQueueException("idempotencyKey must not be blank");
        }
        if (businessKey == null || businessKey.isBlank()) {
            throw new JobQueueException("businessKey must not be blank");
        }
        if (maxAttempts < 1) {
            throw new JobQueueException("maxAttempts must be >= 1");
        }
        Job job = new Job(UUID.randomUUID().toString(), idempotencyKey, businessKey, payload, maxAttempts);
        return jobStore.insertIfAbsent(job);
    }

    public Optional<Job> findById(String id) {
        return jobStore.findById(id);
    }

    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        return jobStore.findByIdempotencyKey(idempotencyKey);
    }
}
