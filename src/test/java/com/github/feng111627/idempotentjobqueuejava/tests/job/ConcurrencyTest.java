package com.github.feng111627.idempotentjobqueuejava.tests.job;

import com.github.feng111627.idempotentjobqueuejava.job.InMemoryJobStore;
import com.github.feng111627.idempotentjobqueuejava.job.JobService;
import com.github.feng111627.idempotentjobqueuejava.job.JobStatus;
import com.github.feng111627.idempotentjobqueuejava.job.JobWorkerPool;
import com.github.feng111627.idempotentjobqueuejava.job.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖业务：多工作线程并发消费时，同一业务键的作业不并发执行、
 * 完成顺序与提交顺序一致；不同业务键可以并行。
 */
class ConcurrencyTest {

    @Test
    @DisplayName("并发：同一业务键的作业不并发执行且按提交顺序完成")
    void sameBusinessKeyIsSerializedAndOrdered() {
        TestSupport.scenario("并发-同业务键串行且保序");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 1);

        int jobCount = 20;
        List<String> submittedOrder = new CopyOnWriteArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            String id = service.submit("idem-seq-" + i, "biz-serial", "p-" + i).getJob().getId();
            submittedOrder.add(id);
        }

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        ConcurrentLinkedQueue<String> completionOrder = new ConcurrentLinkedQueue<>();
        JobWorkerPool worker = new JobWorkerPool(store, job -> {
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(5); // 放大并发窗口
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            completionOrder.add(job.getId());
            inFlight.decrementAndGet();
        }, RetryPolicy.defaultPolicy(), 8, 2);
        worker.start();
        try {
            TestSupport.await("全部作业完成",
                    () -> completionOrder.size() == jobCount, Duration.ofSeconds(30));
        } finally {
            worker.stop();
        }

        assertEquals(1, maxInFlight.get(), "同一业务键不允许并发执行");
        assertEquals(submittedOrder, List.copyOf(completionOrder), "完成顺序必须与提交顺序一致");
        TestSupport.pass("并发-同业务键串行且保序");
    }

    @Test
    @DisplayName("并发：不同业务键的作业可以并行执行，且全部成功")
    void differentBusinessKeysRunInParallel() {
        TestSupport.scenario("并发-不同业务键可并行");
        InMemoryJobStore store = new InMemoryJobStore();
        JobService service = new JobService(store, 1);

        int keyCount = 4;
        CountDownLatch allStarted = new CountDownLatch(keyCount);
        for (int i = 0; i < keyCount; i++) {
            service.submit("idem-par-" + i, "biz-par-" + i, "p");
        }

        JobWorkerPool worker = new JobWorkerPool(store, job -> {
            allStarted.countDown();
            try {
                // 只有 4 个不同业务键的作业同时进入执行体，await 才会返回 true
                if (!allStarted.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("jobs with different business keys did not run in parallel");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, RetryPolicy.defaultPolicy(), keyCount, 2);
        worker.start();
        try {
            TestSupport.await("全部作业成功",
                    () -> {
                        for (int i = 0; i < keyCount; i++) {
                            var job = store.findByIdempotencyKey("idem-par-" + i).orElseThrow();
                            if (job.getStatus() != JobStatus.SUCCEEDED) {
                                return false;
                            }
                        }
                        return true;
                    }, Duration.ofSeconds(15));
            assertTrue(true, "不同业务键的作业并行完成");
        } finally {
            worker.stop();
        }
        TestSupport.pass("并发-不同业务键可并行");
    }
}
