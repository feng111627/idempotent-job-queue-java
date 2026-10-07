package com.github.feng111627.idempotentjobqueuejava.tests.job;

import com.github.feng111627.idempotentjobqueuejava.job.InMemoryJobStore;
import com.github.feng111627.idempotentjobqueuejava.job.JobService;
import com.github.feng111627.idempotentjobqueuejava.job.JobStatus;
import com.github.feng111627.idempotentjobqueuejava.job.JobWorkerPool;
import com.github.feng111627.idempotentjobqueuejava.job.RetryPolicy;
import com.github.feng111627.idempotentjobqueuejava.job.SubmitResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖业务：幂等提交 —— 同一幂等键并发重复提交，只生成并执行一个作业，
 * 重复提交方拿到可识别的 ALREADY_EXISTS 结果。
 */
class IdempotencyTest {

    @Test
    @DisplayName("幂等：同一幂等键顺序重复提交，第二次返回 ALREADY_EXISTS 且不重复入队")
    void duplicateSubmitReturnsAlreadyExists() {
        TestSupport.scenario("幂等-顺序重复提交");
        JobService service = new JobService(new InMemoryJobStore(), 3);

        SubmitResult first = service.submit("idem-1", "biz-1", "payload");
        SubmitResult second = service.submit("idem-1", "biz-1", "payload");

        assertTrue(first.isCreated());
        assertEquals(SubmitResult.Outcome.ALREADY_EXISTS, second.getOutcome());
        assertEquals(first.getJob().getId(), second.getJob().getId());
        TestSupport.pass("幂等-顺序重复提交");
    }

    @Test
    @DisplayName("幂等：并发重复提交同一幂等键，仅一个 CREATED，作业只被执行一次")
    void concurrentDuplicateSubmitExecutesOnce() throws Exception {
        TestSupport.scenario("幂等-并发重复提交只执行一次");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 3);

        int submitters = 16;
        ExecutorService pool = Executors.newFixedThreadPool(submitters);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicInteger createdCount = new AtomicInteger();
        AtomicInteger alreadyExistsCount = new AtomicInteger();
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < submitters; i++) {
            tasks.add(() -> {
                try {
                    startGate.await();
                    SubmitResult r = service.submit("idem-race", "biz-race", "payload");
                    if (r.isCreated()) {
                        createdCount.incrementAndGet();
                    } else {
                        alreadyExistsCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        tasks.forEach(pool::submit);
        startGate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, createdCount.get(), "只能有一个提交真正创建作业");
        assertEquals(submitters - 1, alreadyExistsCount.get(), "其余提交必须拿到 ALREADY_EXISTS");

        // 启动工作线程消费，作业应只被执行一次
        ConcurrentLinkedQueue<String> executedJobIds = new ConcurrentLinkedQueue<>();
        JobWorkerPool worker = new JobWorkerPool(store, job -> executedJobIds.add(job.getId()),
                RetryPolicy.defaultPolicy(), 4, 5);
        worker.start();
        String jobId = store.findByIdempotencyKey("idem-race").orElseThrow().getId();
        TestSupport.await("作业执行完成",
                () -> store.findById(jobId).orElseThrow().getStatus() == JobStatus.SUCCEEDED,
                Duration.ofSeconds(10));
        worker.stop();

        assertEquals(1, executedJobIds.size(), "同一幂等键的作业不能被执行多次");
        assertEquals(jobId, executedJobIds.peek());
        TestSupport.pass("幂等-并发重复提交只执行一次");
    }
}
