package com.github.feng111627.idempotentjobqueuejava.tests.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * 测试辅助：场景日志 + 条件等待。
 */
final class TestSupport {

    private static final Logger log = LoggerFactory.getLogger(TestSupport.class);

    private TestSupport() {
    }

    static void scenario(String name) {
        log.info("[场景] {}", name);
    }

    static void pass(String name) {
        log.info("[结果] {} -> PASS", name);
    }

    static void await(String description, BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while awaiting: " + description);
            }
        }
        throw new AssertionError("timeout awaiting: " + description);
    }
}
