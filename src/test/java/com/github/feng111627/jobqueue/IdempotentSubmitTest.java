package com.github.feng111627.jobqueue;

import com.github.feng111627.jobqueue.core.JobService;
import com.github.feng111627.jobqueue.core.RetryPolicy;
import com.github.feng111627.jobqueue.core.SubmitResult;
import com.github.feng111627.jobqueue.core.SubmitStatus;
import com.github.feng111627.jobqueue.store.InMemoryJobStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotentSubmitTest {

    private JobService newService() {
        return new JobService(new InMemoryJobStore(),
                new RetryPolicy(3, Duration.ofMillis(10), 2.0, Duration.ofMillis(100)));
    }

    @Test
    @DisplayName("幂等: 同一幂等键顺序重复提交，第二次返回 ALREADY_EXISTS 且作业 ID 相同")
    void duplicateSubmitReturnsAlreadyExists() {
        JobService service = newService();
        SubmitResult first = service.submit("key-1", "biz-1", "payload");
        SubmitResult second = service.submit("key-1", "biz-1", "payload");

        assertThat(first.status()).isEqualTo(SubmitStatus.CREATED);
        assertThat(second.status()).isEqualTo(SubmitStatus.ALREADY_EXISTS);
        assertThat(second.jobId()).isEqualTo(first.jobId());
        assertThat(service.listDead()).isEmpty();
    }

    @Test
    @DisplayName("幂等: 同一幂等键并发重复提交，只生成一个作业，其余全部返回 ALREADY_EXISTS")
    void concurrentDuplicateSubmitCreatesOnlyOneJob() throws Exception {
        JobService service = newService();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Callable<SubmitResult>> tasks = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                tasks.add(() -> {
                    ready.countDown();
                    go.await();
                    return service.submit("key-race", "biz-race", "payload");
                });
            }
            List<Future<SubmitResult>> futures = tasks.stream().map(pool::submit).toList();
            ready.await();
            go.countDown();

            List<SubmitResult> results = new ArrayList<>();
            for (Future<SubmitResult> f : futures) {
                results.add(f.get());
            }

            long created = results.stream().filter(r -> r.status() == SubmitStatus.CREATED).count();
            long alreadyExists = results.stream().filter(r -> r.status() == SubmitStatus.ALREADY_EXISTS).count();
            assertThat(created).isEqualTo(1);
            assertThat(alreadyExists).isEqualTo(threads - 1);
            assertThat(results.stream().map(SubmitResult::jobId).distinct()).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("幂等: 不同幂等键各自生成独立作业")
    void distinctKeysCreateDistinctJobs() {
        JobService service = newService();
        SubmitResult a = service.submit("key-a", "biz", "p");
        SubmitResult b = service.submit("key-b", "biz", "p");
        assertThat(a.status()).isEqualTo(SubmitStatus.CREATED);
        assertThat(b.status()).isEqualTo(SubmitStatus.CREATED);
        assertThat(a.jobId()).isNotEqualTo(b.jobId());
    }
}
