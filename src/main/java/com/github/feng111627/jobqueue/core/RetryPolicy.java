package com.github.feng111627.jobqueue.core;

import java.time.Duration;

/**
 * 可配置的指数退避重试策略：
 * 第 n 次失败（从 1 开始）后的延迟 = min(initialDelay * multiplier^(n-1), maxDelay)。
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;

    public RetryPolicy(int maxAttempts, Duration initialDelay, double multiplier, Duration maxDelay) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must be >= 0");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        if (maxDelay.isNegative()) {
            throw new IllegalArgumentException("maxDelay must be >= 0");
        }
        this.maxAttempts = maxAttempts;
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
        this.maxDelay = maxDelay;
    }

    public int getMaxAttempts() { return maxAttempts; }
    public Duration getInitialDelay() { return initialDelay; }
    public double getMultiplier() { return multiplier; }
    public Duration getMaxDelay() { return maxDelay; }

    /** 第 failedAttempt 次失败（从 1 开始）之后应等待的退避时长。 */
    public Duration delayForAttempt(int failedAttempt) {
        if (failedAttempt < 1) {
            throw new IllegalArgumentException("failedAttempt must be >= 1");
        }
        double millis = initialDelay.toMillis() * Math.pow(multiplier, failedAttempt - 1);
        long capped = (long) Math.min(millis, maxDelay.toMillis());
        return Duration.ofMillis(capped);
    }
}
