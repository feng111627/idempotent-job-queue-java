package com.github.feng111627.jobqueue;

import com.github.feng111627.jobqueue.core.JobService;
import com.github.feng111627.jobqueue.core.SubmitStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "jobqueue.worker-threads=2",
        "jobqueue.max-attempts=2",
        "jobqueue.initial-delay=10ms"
})
class ApplicationContextTest {

    @Autowired
    private JobService jobService;

    @Test
    @DisplayName("集成: Spring 上下文装配成功，提交链路可用且幂等")
    void contextLoadsAndSubmitWorks() {
        var first = jobService.submit("ctx-key", "ctx-biz", "p");
        var second = jobService.submit("ctx-key", "ctx-biz", "p");
        assertThat(first.status()).isEqualTo(SubmitStatus.CREATED);
        assertThat(second.status()).isEqualTo(SubmitStatus.ALREADY_EXISTS);
        assertThat(second.jobId()).isEqualTo(first.jobId());
    }
}
