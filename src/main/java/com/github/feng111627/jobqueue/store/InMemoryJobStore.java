package com.github.feng111627.jobqueue.store;

import com.github.feng111627.jobqueue.core.Job;
import com.github.feng111627.jobqueue.core.JobStatus;
import com.github.feng111627.jobqueue.error.JobStorageException;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * 内存作业存储：
 * - 幂等键唯一索引保证同一幂等键只生成一个作业；
 * - claimNextDue 原子地把到期作业置为 RUNNING，避免多线程重复领取；
 * - 每个业务键一把锁，保证同一业务键的作业不并发执行、按提交顺序完成。
 * 底层并发结构异常统一包装为 {@link JobStorageException}，不外泄框架异常。
 */
@Repository
public class InMemoryJobStore implements JobStore {

    private final ConcurrentMap<String, Job> jobsById = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> idByIdempotencyKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ReentrantLock> businessKeyLocks = new ConcurrentHashMap<>();

    @Override
    public Job saveIfAbsent(Job job) {
        try {
            String existing = idByIdempotencyKey.putIfAbsent(job.getIdempotencyKey(), job.getId());
            if (existing != null) {
                return jobsById.get(existing);
            }
            jobsById.put(job.getId(), job);
            return null;
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to save job with idempotency key " + job.getIdempotencyKey(), e);
        }
    }

    @Override
    public Optional<Job> findById(String jobId) {
        try {
            return Optional.ofNullable(jobsById.get(jobId));
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to find job " + jobId, e);
        }
    }

    @Override
    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        try {
            String id = idByIdempotencyKey.get(idempotencyKey);
            return id == null ? Optional.empty() : Optional.ofNullable(jobsById.get(id));
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to find job by idempotency key " + idempotencyKey, e);
        }
    }

    @Override
    public Optional<Job> claimNextDue(Instant now) {
        try {
            Optional<Job> candidate = jobsById.values().stream()
                    .filter(j -> j.getStatus() == JobStatus.PENDING && !j.getNextAttemptAt().isAfter(now))
                    .min(Comparator.comparing(Job::getNextAttemptAt).thenComparing(Job::getCreatedAt));
            if (candidate.isEmpty()) {
                return Optional.empty();
            }
            Job job = candidate.get();
            synchronized (job) {
                if (job.getStatus() != JobStatus.PENDING || job.getNextAttemptAt().isAfter(now)) {
                    return Optional.empty(); // 被其他线程抢先领取
                }
                job.markRunning(now);
                return Optional.of(job);
            }
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to claim next due job", e);
        }
    }

    @Override
    public void markSucceeded(String jobId, Instant now) {
        try {
            Job job = requireJob(jobId);
            synchronized (job) {
                if (job.getStatus() != JobStatus.RUNNING) {
                    return; // 只允许从 RUNNING 完成，防止重复完成
                }
                job.markSucceeded(now);
            }
        } catch (JobStorageException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to mark job " + jobId + " succeeded", e);
        }
    }

    @Override
    public void recordFailure(String jobId, String error, int maxAttempts, Duration nextDelay, Instant now) {
        try {
            Job job = requireJob(jobId);
            synchronized (job) {
                if (job.getStatus() != JobStatus.RUNNING) {
                    return; // 已成功或已死信的作业不再进入重试链路
                }
                if (job.getAttempts() >= maxAttempts) {
                    job.markDead(error, now);
                } else {
                    job.markRetry(error, now.plus(nextDelay), now);
                }
            }
        } catch (JobStorageException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to record failure for job " + jobId, e);
        }
    }

    @Override
    public List<Job> listDead() {
        try {
            return jobsById.values().stream()
                    .filter(j -> j.getStatus() == JobStatus.DEAD)
                    .sorted(Comparator.comparing(Job::getUpdatedAt))
                    .toList();
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to list dead jobs", e);
        }
    }

    @Override
    public <T> T withBusinessKeyLock(String businessKey, Supplier<T> action) {
        ReentrantLock lock;
        try {
            lock = businessKeyLocks.computeIfAbsent(businessKey, k -> new ReentrantLock(true));
        } catch (RuntimeException e) {
            throw new JobStorageException("failed to acquire lock for business key " + businessKey, e);
        }
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    private Job requireJob(String jobId) {
        Job job = jobsById.get(jobId);
        if (job == null) {
            throw new JobStorageException("job not found: " + jobId, null);
        }
        return job;
    }
}
