package com.github.feng111627.jobqueue;

import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * 在测试日志中逐条打印每个用例的业务场景（DisplayName）与执行结果。
 */
public class TestResultLogger implements TestExecutionListener {

    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        if (testIdentifier.isTest()) {
            System.out.println("[TEST-CASE] START  " + testIdentifier.getDisplayName());
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier,
                                  org.junit.platform.engine.TestExecutionResult result) {
        if (testIdentifier.isTest()) {
            String outcome = switch (result.getStatus()) {
                case SUCCESSFUL -> "PASS";
                case FAILED -> "FAIL " + result.getThrowable().map(Throwable::toString).orElse("");
                case ABORTED -> "SKIP";
            };
            System.out.println("[TEST-CASE] " + outcome + "  " + testIdentifier.getDisplayName());
        }
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        System.out.println("[TEST-CASE] all scenarios finished");
    }
}
