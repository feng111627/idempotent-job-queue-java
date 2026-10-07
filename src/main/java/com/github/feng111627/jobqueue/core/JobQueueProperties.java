package com.github.feng111627.jobqueue.core;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "jobqueue")
public class JobQueueProperties {

    /** 最大尝试次数（含首次执行），超过后进入死信。 */
    private int maxAttempts = 3;
    /** 首次失败后的退避延迟。 */
    private Duration initialDelay = Duration.ofMillis(100);
    /** 退避倍数。 */
    private double multiplier = 2.0;
    /** 退避上限。 */
    private Duration maxDelay = Duration.ofSeconds(5);
    /** 工作线程数。 */
    private int workerThreads = 4;
    /** 无作业可执行时的空转休眠毫秒数。 */
    private long idleSleepMillis = 50;

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getInitialDelay() { return initialDelay; }
    public void setInitialDelay(Duration initialDelay) { this.initialDelay = initialDelay; }
    public double getMultiplier() { return multiplier; }
    public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
    public Duration getMaxDelay() { return maxDelay; }
    public void setMaxDelay(Duration maxDelay) { this.maxDelay = maxDelay; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public long getIdleSleepMillis() { return idleSleepMillis; }
    public void setIdleSleepMillis(long idleSleepMillis) { this.idleSleepMillis = idleSleepMillis; }
}
