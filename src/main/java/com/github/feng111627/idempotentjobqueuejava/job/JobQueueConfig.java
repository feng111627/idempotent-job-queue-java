package com.github.feng111627.idempotentjobqueuejava.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * 作业队列装配：退避策略、业务执行器、工作线程池的生命周期管理。
 */
@Configuration
public class JobQueueConfig {

    private static final Logger log = LoggerFactory.getLogger(JobQueueConfig.class);

    @Bean
    public RetryPolicy retryPolicy(
            @Value("${jobqueue.retry.initial-delay-ms:100}") long initialDelayMs,
            @Value("${jobqueue.retry.multiplier:2.0}") double multiplier,
            @Value("${jobqueue.retry.max-delay-ms:5000}") long maxDelayMs) {
        return new RetryPolicy(Duration.ofMillis(initialDelayMs), multiplier, Duration.ofMillis(maxDelayMs));
    }

    /**
     * 默认业务执行器：仅记录日志。真实业务可替换为自己的 JobHandler Bean。
     */
    @Bean
    public JobHandler jobHandler() {
        return job -> log.info("executing job id={} businessKey={} payload={}",
                job.getId(), job.getBusinessKey(), job.getPayload());
    }

    @Bean(destroyMethod = "stop")
    public JobWorkerPool jobWorkerPool(JobStore jobStore, JobHandler jobHandler, RetryPolicy retryPolicy,
                                       @Value("${jobqueue.workers:4}") int workers) {
        JobWorkerPool pool = new JobWorkerPool(jobStore, jobHandler, retryPolicy, workers);
        pool.start();
        return pool;
    }
}
