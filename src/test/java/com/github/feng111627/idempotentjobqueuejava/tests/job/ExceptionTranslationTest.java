package com.github.feng111627.idempotentjobqueuejava.tests.job;

import com.github.feng111627.idempotentjobqueuejava.job.InMemoryJobStore;
import com.github.feng111627.idempotentjobqueuejava.job.Job;
import com.github.feng111627.idempotentjobqueuejava.job.JobQueueException;
import com.github.feng111627.idempotentjobqueuejava.job.JobService;
import com.github.feng111627.idempotentjobqueuejava.job.JobStatus;
import com.github.feng111627.idempotentjobqueuejava.job.JobStorageException;
import com.github.feng111627.idempotentjobqueuejava.job.JobStore;
import com.github.feng111627.idempotentjobqueuejava.job.JobWorkerPool;
import com.github.feng111627.idempotentjobqueuejava.job.RetryPolicy;
import com.github.feng111627.idempotentjobqueuejava.job.SubmitResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖业务：存储/队列层抛出的底层异常统一转换为 JobStorageException，
 * 不向外透出框架或底层异常；工作线程遇到存储异常不崩溃、恢复后继续消费。
 */
class ExceptionTranslationTest {

    @Test
    @DisplayName("异常转换：存储层底层 RuntimeException 被包装为 JobStorageException")
    void underlyingExceptionIsWrapped() {
        TestSupport.scenario("异常转换-底层异常包装为 JobStorageException");
        InMemoryJobStore store = new InMemoryJobStore();
        // 构造一个访问 idempotencyKey 时会抛底层异常的作业，模拟存储层故障
        Job broken = new Job("id-x", "idem-x", "biz-x", "p", 1) {
            @Override
            public String getIdempotencyKey() {
                throw new IllegalStateException("simulated low-level failure");
            }
        };

        JobStorageException ex = assertThrows(JobStorageException.class,
                () -> store.insertIfAbsent(broken));
        assertInstanceOf(JobQueueException.class, ex, "JobStorageException 必须是对外可区分的错误类型");
        assertInstanceOf(IllegalStateException.class, ex.getCause(), "原始异常应作为 cause 保留而非直接透出");
        TestSupport.pass("异常转换-底层异常包装为 JobStorageException");
    }

    @Test
    @DisplayName("异常转换：参数非法时抛出 JobQueueException 而非底层 NPE/IllegalArgument")
    void invalidInputRaisesDomainException() {
        TestSupport.scenario("异常转换-非法参数抛领域异常");
        JobService service = new JobService(new InMemoryJobStore(), 3);
        assertThrows(JobQueueException.class, () -> service.submit(" ", "biz", "p"));
        assertThrows(JobQueueException.class, () -> service.submit("idem", null, "p"));
        assertThrows(JobQueueException.class, () -> service.submit("idem", "biz", "p", 0));
        TestSupport.pass("异常转换-非法参数抛领域异常");
    }

    @Test
    @DisplayName("异常转换：工作线程遇到存储异常不崩溃，存储恢复后继续正常消费")
    void workerSurvivesStorageFailures() {
        TestSupport.scenario("异常转换-工作线程容错并恢复");
        InMemoryJobStore delegate = new InMemoryJobStore();
        AtomicBoolean failClaims = new AtomicBoolean(true);
        // 模拟一个不稳定的存储层：claim 阶段先抛 JobStorageException，之后恢复
        JobStore flakyStore = new JobStore() {
            @Override
            public SubmitResult insertIfAbsent(Job job) {
                return delegate.insertIfAbsent(job);
            }

            @Override
            public Optional<Job> findById(String id) {
                return delegate.findById(id);
            }

            @Override
            public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
                return delegate.findByIdempotencyKey(idempotencyKey);
            }

            @Override
            public Optional<Job> claimNext(Set<String> excludedBusinessKeys) {
                if (failClaims.get()) {
                    throw new JobStorageException("simulated storage outage",
                            new RuntimeException("low-level cause"));
                }
                return delegate.claimNext(excludedBusinessKeys);
            }

            @Override
            public void markSucceeded(String jobId) {
                delegate.markSucceeded(jobId);
            }

            @Override
            public void markFailed(String jobId, String error, RetryPolicy retryPolicy) {
                delegate.markFailed(jobId, error, retryPolicy);
            }
        };

        JobService service = new JobService(flakyStore, 1);
        String jobId = service.submit("idem-flaky", "biz-flaky", "p").getJob().getId();

        AtomicInteger executions = new AtomicInteger();
        JobWorkerPool worker = new JobWorkerPool(flakyStore, job -> executions.incrementAndGet(),
                RetryPolicy.defaultPolicy(), 2, 5);
        worker.start();
        try {
            // 存储故障期间作业不应被执行，也不应有异常逃出工作线程
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            assertEquals(0, executions.get());
            assertEquals(JobStatus.PENDING, flakyStore.findById(jobId).orElseThrow().getStatus());

            // 存储恢复后作业被正常消费
            failClaims.set(false);
            TestSupport.await("存储恢复后作业执行成功",
                    () -> flakyStore.findById(jobId).orElseThrow().getStatus() == JobStatus.SUCCEEDED,
                    Duration.ofSeconds(10));
            assertEquals(1, executions.get());
            assertTrue(true);
        } finally {
            worker.stop();
        }
        TestSupport.pass("异常转换-工作线程容错并恢复");
    }
}
