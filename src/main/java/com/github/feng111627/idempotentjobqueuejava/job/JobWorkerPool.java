package com.github.feng111627.idempotentjobqueuejava.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 多线程作业消费器。
 *
 * 并发语义：
 * - 多个工作线程并发领取作业；
 * - “领取 + 登记在途业务键”在同一监视器下完成，保证同一 businessKey
 *   任意时刻最多一个作业在执行，且按入队顺序逐个完成；
 * - 执行失败按 RetryPolicy 退避重试，超过上限进入死信；
 * - 存储层异常以 JobStorageException 形式向上传递，工作线程捕获后记录日志继续运行。
 */
public class JobWorkerPool {

    private static final Logger log = LoggerFactory.getLogger(JobWorkerPool.class);

    private final JobStore jobStore;
    private final JobHandler handler;
    private final RetryPolicy retryPolicy;
    private final int workerCount;
    private final long idleSleepMillis;

    private final Object inFlightLock = new Object();
    private final Set<String> inFlightBusinessKeys = new HashSet<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    public JobWorkerPool(JobStore jobStore, JobHandler handler, RetryPolicy retryPolicy, int workerCount) {
        this(jobStore, handler, retryPolicy, workerCount, 10);
    }

    public JobWorkerPool(JobStore jobStore, JobHandler handler, RetryPolicy retryPolicy,
                         int workerCount, long idleSleepMillis) {
        this.jobStore = jobStore;
        this.handler = handler;
        this.retryPolicy = retryPolicy;
        this.workerCount = workerCount;
        this.idleSleepMillis = idleSleepMillis;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        executor = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r, "job-worker-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < workerCount; i++) {
            executor.submit(this::runLoop);
        }
        log.info("JobWorkerPool started with {} workers", workerCount);
    }

    public void stop() {
        running.set(false);
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("JobWorkerPool stopped");
    }

    private void runLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            Optional<Job> claimed = claimOne();
            if (claimed.isEmpty()) {
                sleepQuietly();
                continue;
            }
            Job job = claimed.get();
            try {
                execute(job);
            } finally {
                synchronized (inFlightLock) {
                    inFlightBusinessKeys.remove(job.getBusinessKey());
                }
            }
        }
    }

    private Optional<Job> claimOne() {
        synchronized (inFlightLock) {
            Set<String> excluded = Set.copyOf(inFlightBusinessKeys);
            Optional<Job> job;
            try {
                job = jobStore.claimNext(excluded);
            } catch (JobStorageException e) {
                log.error("storage error while claiming job: {}", e.getMessage(), e);
                return Optional.empty();
            }
            job.ifPresent(j -> inFlightBusinessKeys.add(j.getBusinessKey()));
            return job;
        }
    }

    private void execute(Job job) {
        try {
            handler.handle(job);
            jobStore.markSucceeded(job.getId());
            log.info("job {} (businessKey={}) succeeded after {} attempt(s)",
                    job.getId(), job.getBusinessKey(), job.getAttempts());
        } catch (JobQueueException e) {
            // 存储层异常：包装类型直接向上记录，不拆包不透出底层异常
            log.error("storage error while finishing job {}: {}", job.getId(), e.getMessage(), e);
        } catch (Exception e) {
            jobStore.markFailed(job.getId(), e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(),
                    retryPolicy);
            Job updated = jobStore.findById(job.getId()).orElse(job);
            if (updated.getStatus() == JobStatus.DEAD) {
                log.warn("job {} (businessKey={}) moved to DEAD after {} attempt(s), reason: {}",
                        job.getId(), job.getBusinessKey(), updated.getAttempts(), updated.getLastError());
            } else {
                log.info("job {} (businessKey={}) failed attempt {}/{}, will retry at {}",
                        job.getId(), job.getBusinessKey(), updated.getAttempts(),
                        job.getMaxAttempts(), updated.getNextRetryAt());
            }
        }
    }

    private void sleepQuietly() {
        try {
            Thread.sleep(idleSleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
