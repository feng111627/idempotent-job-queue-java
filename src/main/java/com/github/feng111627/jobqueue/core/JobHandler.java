package com.github.feng111627.jobqueue.core;

@FunctionalInterface
public interface JobHandler {

    void handle(Job job) throws Exception;
}
