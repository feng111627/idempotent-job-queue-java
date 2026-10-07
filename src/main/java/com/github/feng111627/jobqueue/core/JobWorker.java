package com.github.feng111627.jobqueue.core;

import com.github.feng111627.jobqueue.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * 工作线程：循环领取到期作业并执行。
 * 同一业务键通过存储层锁串行执行；失败按 RetryPolicy 退避重试，超限进入死信。
 */
public class JobWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);

    private final JobStore store;
    private final RetryPolicy retryPolicy;
    private final JobHandler handler;
    private final Clock clock;
    private final long idleSleepMillis;
    private volatile boolean running = true;

    public JobWorker(JobStore store, RetryPolicy retryPolicy, JobHandler handler) {
        this(store, retryPolicy, handler, Clock.systemUTC(), 50);
    }

    public JobWorker(JobStore store, RetryPolicy retryPolicy, JobHandler handler, Clock clock, long idleSleepMillis) {
        this.store = store;
        this.retryPolicy = retryPolicy;
        this.handler = handler;
        this.clock = clock;
        this.idleSleepMillis = idleSleepMillis;
    }

    public void stop() {
        this.running = false;
    }

    @Override
    public void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            boolean processed = processOne();
            if (!processed) {
                try {
                    Thread.sleep(idleSleepMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** 尝试处理一个到期作业；返回 true 表示确实处理了一个。 */
    public boolean processOne() {
        Optional<Job> claimed;
        try {
            claimed = store.claimNextDue(Instant.now(clock));
        } catch (RuntimeException e) {
            // 存储层错误（含未包装的底层异常）只记录日志，工作线程继续存活
            log.error("storage error while claiming job: {}", e.toString());
            return false;
        }
        if (claimed.isEmpty()) {
            return false;
        }
        Job job = claimed.get();
        // 同一业务键串行执行，避免并发执行与完成顺序错乱
        store.withBusinessKeyLock(job.getBusinessKey(), () -> {
            execute(job);
            return null;
        });
        return true;
    }

    private void execute(Job job) {
        try {
            handler.handle(job);
            store.markSucceeded(job.getId(), Instant.now(clock));
            log.info("job {} (businessKey={}) succeeded after {} attempt(s)",
                    job.getId(), job.getBusinessKey(), job.getAttempts());
        } catch (Exception e) {
            Duration delay = retryPolicy.delayForAttempt(job.getAttempts());
            store.recordFailure(job.getId(), e.getMessage(), retryPolicy.getMaxAttempts(), delay, Instant.now(clock));
            Job updated = store.findById(job.getId()).orElse(job);
            if (updated.getStatus() == JobStatus.DEAD) {
                log.warn("job {} (businessKey={}) moved to dead letter after {} attempt(s): {}",
                        job.getId(), job.getBusinessKey(), job.getAttempts(), e.getMessage());
            } else {
                log.info("job {} (businessKey={}) failed attempt {}, retrying after {}ms: {}",
                        job.getId(), job.getBusinessKey(), job.getAttempts(), delay.toMillis(), e.getMessage());
            }
        }
    }
}
