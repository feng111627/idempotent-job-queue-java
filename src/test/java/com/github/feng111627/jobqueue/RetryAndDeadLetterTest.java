package com.github.feng111627.jobqueue;

import com.github.feng111627.jobqueue.core.Job;
import com.github.feng111627.jobqueue.core.JobService;
import com.github.feng111627.jobqueue.core.JobStatus;
import com.github.feng111627.jobqueue.core.JobWorker;
import com.github.feng111627.jobqueue.core.RetryPolicy;
import com.github.feng111627.jobqueue.core.SubmitResult;
import com.github.feng111627.jobqueue.store.InMemoryJobStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RetryAndDeadLetterTest {

    /** 可手动拨动的时钟，用于验证退避时间窗口。 */
    static class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-10-07T00:00:00Z");
        @Override public Instant instant() { return now; }
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        void advance(Duration d) { now = now.plus(d); }
    }

    private record Fixture(JobService service, InMemoryJobStore store, MutableClock clock) {}

    private Fixture newFixture(int maxAttempts, Duration initialDelay) {
        InMemoryJobStore store = new InMemoryJobStore();
        MutableClock clock = new MutableClock();
        RetryPolicy policy = new RetryPolicy(maxAttempts, initialDelay, 2.0, Duration.ofSeconds(60));
        return new Fixture(new JobService(store, policy, clock), store, clock);
    }

    @Test
    @DisplayName("重试: 持续失败的作业按次数重试，超限后进入死信并保留失败原因")
    void failsUntilDeadLetter() {
        Fixture f = newFixture(3, Duration.ZERO);
        AtomicInteger calls = new AtomicInteger();
        JobWorker worker = new JobWorker(f.store(),
                new RetryPolicy(3, Duration.ZERO, 2.0, Duration.ofSeconds(60)),
                job -> { calls.incrementAndGet(); throw new RuntimeException("boom-" + calls.get()); },
                f.clock(), 1);

        SubmitResult result = f.service().submit("k-dead", "biz-dead", "p");

        assertThat(worker.processOne()).isTrue();
        assertThat(f.service().findById(result.jobId()).orElseThrow().getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(worker.processOne()).isTrue();
        assertThat(worker.processOne()).isTrue();

        Job job = f.service().findById(result.jobId()).orElseThrow();
        assertThat(job.getStatus()).isEqualTo(JobStatus.DEAD);
        assertThat(job.getAttempts()).isEqualTo(3);
        assertThat(job.getLastError()).isEqualTo("boom-3");
        assertThat(f.service().listDead()).extracting(Job::getId).containsExactly(result.jobId());
    }

    @Test
    @DisplayName("死信: 已进入死信的作业不会被重试链路再次执行")
    void deadJobIsNotReexecuted() {
        Fixture f = newFixture(2, Duration.ZERO);
        AtomicInteger calls = new AtomicInteger();
        JobWorker worker = new JobWorker(f.store(),
                new RetryPolicy(2, Duration.ZERO, 2.0, Duration.ofSeconds(60)),
                job -> { calls.incrementAndGet(); throw new RuntimeException("always fails"); },
                f.clock(), 1);

        f.service().submit("k-dead2", "biz-dead2", "p");
        worker.processOne();
        worker.processOne();
        assertThat(calls.get()).isEqualTo(2);

        for (int i = 0; i < 5; i++) {
            assertThat(worker.processOne()).isFalse();
        }
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("重试: 前两次失败第三次成功，最终状态为 SUCCEEDED")
    void succeedsAfterRetries() {
        Fixture f = newFixture(3, Duration.ZERO);
        AtomicInteger calls = new AtomicInteger();
        JobWorker worker = new JobWorker(f.store(),
                new RetryPolicy(3, Duration.ZERO, 2.0, Duration.ofSeconds(60)),
                job -> { if (calls.incrementAndGet() < 3) throw new RuntimeException("flaky"); },
                f.clock(), 1);

        SubmitResult result = f.service().submit("k-flaky", "biz-flaky", "p");
        worker.processOne();
        worker.processOne();
        worker.processOne();

        Job job = f.service().findById(result.jobId()).orElseThrow();
        assertThat(job.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(job.getAttempts()).isEqualTo(3);
        assertThat(job.getLastError()).isNull();
    }

    @Test
    @DisplayName("重试: 已成功的作业不会被再次执行")
    void succeededJobIsNotReexecuted() {
        Fixture f = newFixture(3, Duration.ZERO);
        AtomicInteger calls = new AtomicInteger();
        JobWorker worker = new JobWorker(f.store(),
                new RetryPolicy(3, Duration.ZERO, 2.0, Duration.ofSeconds(60)),
                job -> calls.incrementAndGet(),
                f.clock(), 1);

        f.service().submit("k-ok", "biz-ok", "p");
        assertThat(worker.processOne()).isTrue();
        assertThat(calls.get()).isEqualTo(1);
        for (int i = 0; i < 5; i++) {
            assertThat(worker.processOne()).isFalse();
        }
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("退避: 失败后未到退避时间不会被领取，时钟前进后才可重试")
    void backoffWindowIsRespected() {
        Fixture f = newFixture(3, Duration.ofSeconds(30));
        AtomicReference<Job> seen = new AtomicReference<>();
        JobWorker worker = new JobWorker(f.store(),
                new RetryPolicy(3, Duration.ofSeconds(30), 2.0, Duration.ofSeconds(60)),
                job -> { seen.set(job); throw new RuntimeException("fail"); },
                f.clock(), 1);

        SubmitResult result = f.service().submit("k-backoff", "biz-backoff", "p");
        assertThat(worker.processOne()).isTrue();
        // 刚失败，退避 30s 内不可领取
        assertThat(worker.processOne()).isFalse();

        f.clock().advance(Duration.ofSeconds(29));
        assertThat(worker.processOne()).isFalse();

        f.clock().advance(Duration.ofSeconds(1));
        assertThat(worker.processOne()).isTrue();
        assertThat(f.service().findById(result.jobId()).orElseThrow().getAttempts()).isEqualTo(2);
    }
}
