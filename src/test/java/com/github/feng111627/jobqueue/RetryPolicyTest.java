package com.github.feng111627.jobqueue;

import com.github.feng111627.jobqueue.core.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    @Test
    @DisplayName("退避策略: 指数退避按 multiplier 增长")
    void exponentialBackoff() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), 2.0, Duration.ofSeconds(10));
        assertThat(policy.delayForAttempt(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.delayForAttempt(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(policy.delayForAttempt(3)).isEqualTo(Duration.ofMillis(400));
    }

    @Test
    @DisplayName("退避策略: 延迟不超过 maxDelay 上限")
    void backoffCappedAtMaxDelay() {
        RetryPolicy policy = new RetryPolicy(10, Duration.ofMillis(100), 10.0, Duration.ofMillis(500));
        assertThat(policy.delayForAttempt(5)).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    @DisplayName("退避策略: 非法参数被拒绝")
    void invalidArgsRejected() {
        assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, 1.0, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(1, Duration.ZERO, 0.5, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
