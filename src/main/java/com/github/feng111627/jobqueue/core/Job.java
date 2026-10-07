package com.github.feng111627.jobqueue.core;

import java.time.Instant;

public class Job {

    private final String id;
    private final String idempotencyKey;
    private final String businessKey;
    private final String payload;
    private final int maxAttempts;
    private volatile JobStatus status;
    private volatile int attempts;
    private volatile Instant nextAttemptAt;
    private volatile String lastError;
    private final Instant createdAt;
    private volatile Instant updatedAt;

    public Job(String id, String idempotencyKey, String businessKey, String payload, int maxAttempts, Instant now) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.businessKey = businessKey;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.status = JobStatus.PENDING;
        this.attempts = 0;
        this.nextAttemptAt = now;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public String getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getBusinessKey() { return businessKey; }
    public String getPayload() { return payload; }
    public int getMaxAttempts() { return maxAttempts; }
    public JobStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public String getLastError() { return lastError; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public void markRunning(Instant now) {
        this.status = JobStatus.RUNNING;
        this.attempts++;
        this.updatedAt = now;
    }

    public void markSucceeded(Instant now) {
        this.status = JobStatus.SUCCEEDED;
        this.lastError = null;
        this.updatedAt = now;
    }

    public void markRetry(String error, Instant nextAttemptAt, Instant now) {
        this.status = JobStatus.PENDING;
        this.lastError = error;
        this.nextAttemptAt = nextAttemptAt;
        this.updatedAt = now;
    }

    public void markDead(String error, Instant now) {
        this.status = JobStatus.DEAD;
        this.lastError = error;
        this.updatedAt = now;
    }
}
