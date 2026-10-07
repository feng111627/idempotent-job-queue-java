package com.github.feng111627.idempotentjobqueuejava.job;

/**
 * 业务执行器：执行一个作业的业务逻辑。
 * 抛出任何异常都视为执行失败，进入重试链路。
 */
@FunctionalInterface
public interface JobHandler {
    void handle(Job job) throws Exception;
}
