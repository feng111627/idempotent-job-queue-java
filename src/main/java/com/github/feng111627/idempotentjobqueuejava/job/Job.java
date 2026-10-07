package com.github.feng111627.idempotentjobqueuejava.job;

import java.time.Instant;

/**
 * 作业实体。状态迁移由 JobStore 以原子方式完成。
 */
public class Job {
    private final String id;
    private final String idempotencyKey;
    private final String businessKey;
    private final String payload;
    private final int maxAttempts;
    private volatile JobStatus status;
    private volatile int attempts;
    private volatile String lastError;
    private volatile Instant nextRetryAt;
    private final Instant createdAt;
    private volatile Instant updatedAt;

    public Job(String id, String idempotencyKey, String businessKey, String payload, int maxAttempts) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.businessKey = businessKey;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.status = JobStatus.PENDING;
        this.attempts = 0;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getBusinessKey() { return businessKey; }
    public String getPayload() { return payload; }
    public int getMaxAttempts() { return maxAttempts; }
    public JobStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public String getLastError() { return lastError; }
    public Instant getNextRetryAt() { return nextRetryAt; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    void setStatus(JobStatus status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    void setAttempts(int attempts) { this.attempts = attempts; }
    void setLastError(String lastError) { this.lastError = lastError; }
    void setNextRetryAt(Instant nextRetryAt) { this.nextRetryAt = nextRetryAt; }
}
