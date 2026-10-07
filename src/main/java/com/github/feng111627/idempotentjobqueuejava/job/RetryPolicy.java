package com.github.feng111627.idempotentjobqueuejava.job;

import java.time.Duration;

/**
 * 可配置的退避重试策略：initialDelay * multiplier^(attempt-1)，封顶 maxDelay。
 */
public class RetryPolicy {
    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;

    public RetryPolicy(Duration initialDelay, double multiplier, Duration maxDelay) {
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
        this.maxDelay = maxDelay;
    }

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(Duration.ofMillis(100), 2.0, Duration.ofSeconds(5));
    }

    /** 第 attempt 次失败（从 1 开始）后应等待的时长。 */
    public Duration backoffFor(int attempt) {
        double millis = initialDelay.toMillis() * Math.pow(multiplier, attempt - 1);
        long capped = (long) Math.min(millis, maxDelay.toMillis());
        return Duration.ofMillis(capped);
    }

    public Duration getInitialDelay() { return initialDelay; }
    public double getMultiplier() { return multiplier; }
    public Duration getMaxDelay() { return maxDelay; }
}
