package com.github.feng111627.idempotentjobqueuejava.job;

/**
 * 幂等键冲突：同一个幂等键的作业已存在。
 */
public class JobAlreadyExistsException extends JobQueueException {
    private final String idempotencyKey;

    public JobAlreadyExistsException(String idempotencyKey) {
        super("job with idempotency key already exists: " + idempotencyKey);
        this.idempotencyKey = idempotencyKey;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }
}
