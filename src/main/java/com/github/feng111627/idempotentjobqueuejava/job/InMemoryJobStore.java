package com.github.feng111627.idempotentjobqueuejava.job;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 基于内存的 JobStore 实现。
 *
 * 并发语义：
 * - 全部状态迁移在内部锁上完成，保证原子性；
 * - 幂等键 -> 作业 的映射保证同一幂等键只会真正创建一个作业；
 * - 就绪队列只存放 PENDING / 到期 RETRY_WAITING 的作业 id，领取即转为 RUNNING；
 * - SUCCEEDED / DEAD 为终态，任何后续迁移请求都会被拒绝。
 */
@Component
public class InMemoryJobStore implements JobStore {

    private final Object lock = new Object();
    private final Map<String, Job> byId = new HashMap<>();
    private final Map<String, String> idByIdempotencyKey = new HashMap<>();
    private final Deque<String> readyQueue = new ArrayDeque<>();

    @Override
    public SubmitResult insertIfAbsent(Job job) {
        try {
            synchronized (lock) {
                String existingId = idByIdempotencyKey.get(job.getIdempotencyKey());
                if (existingId != null) {
                    return SubmitResult.alreadyExists(byId.get(existingId));
                }
                byId.put(job.getId(), job);
                idByIdempotencyKey.put(job.getIdempotencyKey(), job.getId());
                readyQueue.addLast(job.getId());
                return SubmitResult.created(job);
            }
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to insert job: " + job.getId(), e);
        }
    }

    @Override
    public Optional<Job> findById(String id) {
        synchronized (lock) {
            return Optional.ofNullable(byId.get(id));
        }
    }

    @Override
    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        synchronized (lock) {
            String id = idByIdempotencyKey.get(idempotencyKey);
            return id == null ? Optional.empty() : Optional.ofNullable(byId.get(id));
        }
    }

    @Override
    public Optional<Job> claimNext(Set<String> excludedBusinessKeys) {
        try {
            synchronized (lock) {
                Instant now = Instant.now();
                int scanned = 0;
                int size = readyQueue.size();
                // 最多扫描一圈：被排除的业务键与未到重试时间的作业重新排到队尾，避免饥饿。
                while (scanned < size) {
                    String id = readyQueue.pollFirst();
                    scanned++;
                    Job job = byId.get(id);
                    if (job == null) {
                        continue;
                    }
                    if (excludedBusinessKeys.contains(job.getBusinessKey())) {
                        readyQueue.addLast(id);
                        continue;
                    }
                    JobStatus status = job.getStatus();
                    if (status == JobStatus.PENDING) {
                        job.setStatus(JobStatus.RUNNING);
                        return Optional.of(job);
                    }
                    if (status == JobStatus.RETRY_WAITING
                            && job.getNextRetryAt() != null
                            && !job.getNextRetryAt().isAfter(now)) {
                        job.setStatus(JobStatus.RUNNING);
                        return Optional.of(job);
                    }
                    // 未到期的重试作业放回队尾
                    readyQueue.addLast(id);
                }
                return Optional.empty();
            }
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to claim next job", e);
        }
    }

    @Override
    public void markSucceeded(String jobId) {
        try {
            synchronized (lock) {
                Job job = mustFind(jobId);
                if (job.getStatus() != JobStatus.RUNNING) {
                    // 终态或非运行态不允许再被标记成功，保证“已成功不可再被重试链路触碰”
                    return;
                }
                job.setAttempts(job.getAttempts() + 1);
                job.setStatus(JobStatus.SUCCEEDED);
            }
        } catch (JobQueueException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to mark succeeded: " + jobId, e);
        }
    }

    @Override
    public void markFailed(String jobId, String error, RetryPolicy retryPolicy) {
        try {
            synchronized (lock) {
                Job job = mustFind(jobId);
                if (job.getStatus() != JobStatus.RUNNING) {
                    // 已成功/已死信的作业不再进入重试链路
                    return;
                }
                int attempts = job.getAttempts() + 1;
                job.setAttempts(attempts);
                job.setLastError(error);
                if (attempts >= job.getMaxAttempts()) {
                    job.setStatus(JobStatus.DEAD);
                    job.setNextRetryAt(null);
                } else {
                    job.setStatus(JobStatus.RETRY_WAITING);
                    job.setNextRetryAt(Instant.now().plus(retryPolicy.backoffFor(attempts)));
                    readyQueue.addLast(jobId);
                }
            }
        } catch (JobQueueException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to mark failed: " + jobId, e);
        }
    }

    private Job mustFind(String jobId) {
        Job job = byId.get(jobId);
        if (job == null) {
            throw new JobQueueException("job not found: " + jobId);
        }
        return job;
    }
}
