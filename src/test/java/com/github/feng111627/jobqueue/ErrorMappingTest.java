package com.github.feng111627.jobqueue;

import com.github.feng111627.jobqueue.core.Job;
import com.github.feng111627.jobqueue.core.JobService;
import com.github.feng111627.jobqueue.core.JobWorker;
import com.github.feng111627.jobqueue.core.RetryPolicy;
import com.github.feng111627.jobqueue.error.JobQueueException;
import com.github.feng111627.jobqueue.error.JobStorageException;
import com.github.feng111627.jobqueue.store.JobStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ErrorMappingTest {

    /** 模拟底层存储抛框架异常的 JobStore。 */
    static class ExplodingStore implements JobStore {
        @Override
        public Job saveIfAbsent(Job job) {
            throw new IllegalStateException("simulated low-level failure");
        }
        @Override public Optional<Job> findById(String jobId) { return Optional.empty(); }
        @Override public Optional<Job> findByIdempotencyKey(String key) { return Optional.empty(); }
        @Override public Optional<Job> claimNextDue(Instant now) {
            throw new IllegalStateException("simulated low-level failure");
        }
        @Override public void markSucceeded(String jobId, Instant now) { }
        @Override public void recordFailure(String jobId, String error, int maxAttempts, Duration d, Instant now) { }
        @Override public List<Job> listDead() { return List.of(); }
        @Override public <T> T withBusinessKeyLock(String businessKey, Supplier<T> action) { return action.get(); }
    }

    @Test
    @DisplayName("错误映射: 存储层底层异常被包装为 JobStorageException，不透出原始异常类型")
    void storageErrorsAreWrapped() {
        JobService service = new JobService(new ExplodingStore(),
                new RetryPolicy(3, Duration.ZERO, 1.0, Duration.ZERO));

        assertThatThrownBy(() -> service.submit("k", "biz", "p"))
                .isInstanceOf(JobStorageException.class)
                .isInstanceOf(JobQueueException.class)
                .hasMessageContaining("k")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("错误映射: 工作线程遇到存储错误时记录日志并继续，不抛出线程")
    void workerSurvivesStorageErrors() {
        JobWorker worker = new JobWorker(new ExplodingStore(),
                new RetryPolicy(3, Duration.ZERO, 1.0, Duration.ZERO),
                job -> { });
        // 不抛异常，返回 false 表示未处理
        assertThat(worker.processOne()).isFalse();
    }

    @Test
    @DisplayName("错误映射: 提交参数非法时给出可识别的参数错误")
    void invalidSubmitArgs() {
        JobService service = new JobService(new com.github.feng111627.jobqueue.store.InMemoryJobStore(),
                new RetryPolicy(3, Duration.ZERO, 1.0, Duration.ZERO));
        assertThatThrownBy(() -> service.submit(" ", "biz", "p"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idempotencyKey");
        assertThatThrownBy(() -> service.submit("k", null, "p"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("businessKey");
    }
}
