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
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖业务：失败按可配置退避策略自动重试，最终成功；
 * 退避间隔符合 initialDelay * multiplier^(n-1) 且封顶 maxDelay。
 */
class RetryBackoffTest {

    @Test
    @DisplayName("重试：前两次失败后第三次成功，attempts=3 且状态为 SUCCEEDED")
    void failsTwiceThenSucceeds() {
        TestSupport.scenario("重试-失败后自动重试直至成功");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 3);
        RetryPolicy retryPolicy = new RetryPolicy(Duration.ofMillis(20), 2.0, Duration.ofMillis(200));

        AtomicInteger attempts = new AtomicInteger();
        JobWorkerPool worker = new JobWorkerPool(store, job -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("boom-" + attempts.get());
            }
        }, retryPolicy, 2, 5);
        worker.start();
        try {
            String jobId = service.submit("idem-retry", "biz-retry", "p").getJob().getId();
            TestSupport.await("作业重试后成功",
                    () -> store.findById(jobId).orElseThrow().getStatus() == JobStatus.SUCCEEDED,
                    Duration.ofSeconds(10));

            Job job = store.findById(jobId).orElseThrow();
            assertEquals(3, job.getAttempts());
            assertEquals(3, attempts.get(), "业务处理器应被调用 3 次");
            assertEquals("boom-2", job.getLastError(), "失败原因应保留最后一次错误");
        } finally {
            worker.stop();
        }
        TestSupport.pass("重试-失败后自动重试直至成功");
    }

    @Test
    @DisplayName("退避：重试间隔按指数增长并封顶 maxDelay")
    void backoffIntervalsFollowPolicy() {
        TestSupport.scenario("退避-间隔符合指数退避并封顶");
        RetryPolicy policy = new RetryPolicy(Duration.ofMillis(100), 2.0, Duration.ofMillis(250));
        assertEquals(Duration.ofMillis(100), policy.backoffFor(1));
        assertEquals(Duration.ofMillis(200), policy.backoffFor(2));
        assertEquals(Duration.ofMillis(250), policy.backoffFor(3), "超过封顶值应被截断到 maxDelay");
        assertEquals(Duration.ofMillis(250), policy.backoffFor(5));
        TestSupport.pass("退避-间隔符合指数退避并封顶");
    }

    @Test
    @DisplayName("退避：失败后的作业在 nextRetryAt 之前不会被重新执行")
    void retryRespectsBackoffTiming() {
        TestSupport.scenario("退避-未到重试时间不执行");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 2);
        RetryPolicy retryPolicy = new RetryPolicy(Duration.ofMillis(150), 1.0, Duration.ofMillis(150));

        ConcurrentLinkedQueue<Instant> executionTimes = new ConcurrentLinkedQueue<>();
        JobWorkerPool worker = new JobWorkerPool(store, job -> {
            executionTimes.add(Instant.now());
            if (executionTimes.size() == 1) {
                throw new IllegalStateException("first attempt fails");
            }
        }, retryPolicy, 1, 5);
        worker.start();
        try {
            String jobId = service.submit("idem-timing", "biz-timing", "p").getJob().getId();
            TestSupport.await("作业重试后成功",
                    () -> store.findById(jobId).orElseThrow().getStatus() == JobStatus.SUCCEEDED,
                    Duration.ofSeconds(10));

            assertEquals(2, executionTimes.size());
            long gapMillis = Duration.between(
                    executionTimes.poll(), executionTimes.poll()).toMillis();
            assertTrue(gapMillis >= 140,
                    "第二次执行与第一次的间隔应不小于退避时长，实际 " + gapMillis + "ms");
        } finally {
            worker.stop();
        }
        TestSupport.pass("退避-未到重试时间不执行");
    }
}
