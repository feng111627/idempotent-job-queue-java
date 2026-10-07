package com.github.feng111627.idempotentjobqueuejava.tests.job;

import com.github.feng111627.idempotentjobqueuejava.job.InMemoryJobStore;
import com.github.feng111627.idempotentjobqueuejava.job.Job;
import com.github.feng111627.idempotentjobqueuejava.job.JobService;
import com.github.feng111627.idempotentjobqueuejava.job.JobStatus;
import com.github.feng111627.idempotentjobqueuejava.job.JobWorkerPool;
import com.github.feng111627.idempotentjobqueuejava.job.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖业务：超过重试上限进入死信并保留失败原因；
 * 已死信/已成功的作业不会再被重试链路执行。
 */
class DeadLetterTest {

    @Test
    @DisplayName("死信：持续失败达到上限后进入 DEAD，保留失败原因，且不再被执行")
    void exceedsMaxAttemptsGoesToDeadLetter() throws Exception {
        TestSupport.scenario("死信-超过重试上限进入死信");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 3);
        RetryPolicy retryPolicy = new RetryPolicy(Duration.ofMillis(10), 1.0, Duration.ofMillis(10));

        AtomicInteger executions = new AtomicInteger();
        JobWorkerPool worker = new JobWorkerPool(store, job -> {
            executions.incrementAndGet();
            throw new IllegalStateException("always fails");
        }, retryPolicy, 2, 5);
        worker.start();
        try {
            String jobId = service.submit("idem-dead", "biz-dead", "p").getJob().getId();
            TestSupport.await("作业进入死信",
                    () -> store.findById(jobId).orElseThrow().getStatus() == JobStatus.DEAD,
                    Duration.ofSeconds(10));

            Job dead = store.findById(jobId).orElseThrow();
            assertEquals(3, dead.getAttempts(), "应执行满 maxAttempts 次");
            assertEquals("always fails", dead.getLastError(), "死信必须保留失败原因");

            // 再等一段时间，确认死信作业不会被重试链路重新捞起来执行
            Thread.sleep(300);
            assertEquals(3, executions.get(), "进入死信后不能再被执行");
            assertEquals(JobStatus.DEAD, store.findById(jobId).orElseThrow().getStatus());
        } finally {
            worker.stop();
        }
        TestSupport.pass("死信-超过重试上限进入死信");
    }

    @Test
    @DisplayName("终态保护：已成功的作业不会被重复执行，对终态作业的迁移请求被忽略")
    void succeededJobIsNeverReexecuted() throws Exception {
        TestSupport.scenario("终态保护-成功作业不再执行");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 3);

        AtomicInteger executions = new AtomicInteger();
        JobWorkerPool worker = new JobWorkerPool(store, job -> executions.incrementAndGet(),
                new RetryPolicy(Duration.ofMillis(10), 1.0, Duration.ofMillis(10)), 2, 5);
        worker.start();
        try {
            String jobId = service.submit("idem-done", "biz-done", "p").getJob().getId();
            TestSupport.await("作业执行成功",
                    () -> store.findById(jobId).orElseThrow().getStatus() == JobStatus.SUCCEEDED,
                    Duration.ofSeconds(10));

            // 直接对终态作业发起状态迁移，必须被忽略而不是抛错或改状态
            store.markFailed(jobId, "late failure", RetryPolicy.defaultPolicy());
            store.markSucceeded(jobId);
            assertEquals(JobStatus.SUCCEEDED, store.findById(jobId).orElseThrow().getStatus());

            Thread.sleep(200);
            assertEquals(1, executions.get(), "成功作业不能被执行第二次");
        } finally {
            worker.stop();
        }
        TestSupport.pass("终态保护-成功作业不再执行");
    }

    @Test
    @DisplayName("终态保护：死信作业的状态迁移请求被忽略，保持 DEAD 与失败原因")
    void deadJobStateIsImmutable() {
        TestSupport.scenario("终态保护-死信作业不可变更");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 1);
        String jobId = service.submit("idem-immutable", "biz-immutable", "p").getJob().getId();

        // 手动驱动一次领取 + 失败，maxAttempts=1 直接进死信
        Job claimed = store.claimNext(java.util.Set.of()).orElseThrow();
        store.markFailed(claimed.getId(), "fatal", RetryPolicy.defaultPolicy());
        assertEquals(JobStatus.DEAD, store.findById(jobId).orElseThrow().getStatus());

        store.markFailed(jobId, "another", RetryPolicy.defaultPolicy());
        store.markSucceeded(jobId);
        Job job = store.findById(jobId).orElseThrow();
        assertEquals(JobStatus.DEAD, job.getStatus());
        assertEquals("fatal", job.getLastError(), "死信保留最初失败原因，不被覆盖");
        assertTrue(store.claimNext(java.util.Set.of()).isEmpty(), "死信作业不应再被领取");
        TestSupport.pass("终态保护-死信作业不可变更");
    }
}
