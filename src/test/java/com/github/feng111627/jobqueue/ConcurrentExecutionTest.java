package com.github.feng111627.jobqueue;

import com.github.feng111627.jobqueue.core.JobService;
import com.github.feng111627.jobqueue.core.JobStatus;
import com.github.feng111627.jobqueue.core.JobWorker;
import com.github.feng111627.jobqueue.core.RetryPolicy;
import com.github.feng111627.jobqueue.core.SubmitResult;
import com.github.feng111627.jobqueue.store.InMemoryJobStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ConcurrentExecutionTest {

    @Test
    @DisplayName("并发: 多工作线程消费同一业务键的作业，不并发执行且按提交顺序完成")
    void sameBusinessKeyExecutesSequentiallyAndInOrder() throws Exception {
        InMemoryJobStore store = new InMemoryJobStore();
        RetryPolicy policy = new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
        JobService service = new JobService(store, policy);

        int jobCount = 20;
        List<String> submittedOrder = new CopyOnWriteArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            SubmitResult r = service.submit("k-" + i, "same-biz", "payload-" + i);
            submittedOrder.add(r.jobId());
        }

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        ConcurrentLinkedQueue<String> completionOrder = new ConcurrentLinkedQueue<>();

        JobWorker worker = new JobWorker(store, policy, job -> {
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                Thread.sleep(2); // 放大竞争窗口
            } finally {
                inFlight.decrementAndGet();
                completionOrder.add(job.getId());
            }
        }, java.time.Clock.systemUTC(), 1);

        int workerThreads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(workerThreads);
        CountDownLatch done = new CountDownLatch(jobCount);
        for (int i = 0; i < workerThreads; i++) {
            pool.submit(() -> {
                while (done.getCount() > 0) {
                    if (worker.processOne()) {
                        done.countDown();
                    }
                }
            });
        }
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        assertThat(maxInFlight.get()).isEqualTo(1);
        assertThat(completionOrder).containsExactlyElementsOf(submittedOrder);
        assertThat(submittedOrder.stream()
                .map(id -> service.findById(id).orElseThrow().getStatus()))
                .containsOnly(JobStatus.SUCCEEDED);
    }

    @Test
    @DisplayName("并发: 不同业务键的作业可以被多个工作线程并行消费")
    void differentBusinessKeysCanRunInParallel() throws Exception {
        InMemoryJobStore store = new InMemoryJobStore();
        RetryPolicy policy = new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
        JobService service = new JobService(store, policy);

        int jobCount = 4;
        CountDownLatch allStarted = new CountDownLatch(jobCount);
        CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < jobCount; i++) {
            service.submit("pk-" + i, "biz-" + i, "p");
        }

        JobWorker worker = new JobWorker(store, policy, job -> {
            allStarted.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, java.time.Clock.systemUTC(), 1);

        ExecutorService pool = Executors.newFixedThreadPool(jobCount);
        for (int i = 0; i < jobCount; i++) {
            pool.submit(() -> { while (!worker.processOne()) { } });
        }
        // 若业务键间互相阻塞，4 个作业无法同时进入执行体，这里会超时
        assertThat(allStarted.await(10, TimeUnit.SECONDS)).isTrue();
        release.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
}
