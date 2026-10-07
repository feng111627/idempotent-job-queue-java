package com.github.feng111627.jobqueue.core;

public record SubmitResult(SubmitStatus status, String jobId) {

    public static SubmitResult created(String jobId) {
        return new SubmitResult(SubmitStatus.CREATED, jobId);
    }

    public static SubmitResult alreadyExists(String jobId) {
        return new SubmitResult(SubmitStatus.ALREADY_EXISTS, jobId);
    }
}
