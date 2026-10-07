package com.github.feng111627.idempotentjobqueuejava.job;

/**
 * 存储/队列层底层异常的统一包装，对外可区分。
 */
public class JobStorageException extends JobQueueException {
    public JobStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
