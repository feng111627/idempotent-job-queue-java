package com.github.feng111627.idempotentjobqueuejava.job;

/**
 * 提交结果：调用方能明确区分“新建”与“已存在（幂等命中）”。
 */
public final class SubmitResult {
    public enum Outcome { CREATED, ALREADY_EXISTS }

    private final Outcome outcome;
    private final Job job;

    private SubmitResult(Outcome outcome, Job job) {
        this.outcome = outcome;
        this.job = job;
    }

    public static SubmitResult created(Job job) {
        return new SubmitResult(Outcome.CREATED, job);
    }

    public static SubmitResult alreadyExists(Job job) {
        return new SubmitResult(Outcome.ALREADY_EXISTS, job);
    }

    public Outcome getOutcome() { return outcome; }
    public Job getJob() { return job; }

    public boolean isCreated() { return outcome == Outcome.CREATED; }
}
