package com.github.feng111627.idempotentjobqueuejava.job;

/**
 * 作业队列对外暴露的受检异常基类。
 * 底层框架/存储异常一律包装成本类或其子类，不向外透传。
 */
public class JobQueueException extends RuntimeException {
    public JobQueueException(String message) {
        super(message);
    }

    public JobQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
