package com.github.feng111627.jobqueue.core;

import com.github.feng111627.jobqueue.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@EnableConfigurationProperties(JobQueueProperties.class)
public class JobQueueConfig {

    private static final Logger log = LoggerFactory.getLogger(JobQueueConfig.class);

    @Bean
    public RetryPolicy retryPolicy(JobQueueProperties props) {
        return new RetryPolicy(props.getMaxAttempts(), props.getInitialDelay(),
                props.getMultiplier(), props.getMaxDelay());
    }

    /** 默认处理器：仅记录日志。生产使用时可覆盖该 Bean 接入真实业务。 */
    @Bean
    public JobHandler jobHandler() {
        return job -> log.info("executing job {} businessKey={} payload={}",
                job.getId(), job.getBusinessKey(), job.getPayload());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService jobWorkerExecutor(JobStore store, RetryPolicy retryPolicy,
                                             JobHandler handler, JobQueueProperties props) {
        ExecutorService executor = Executors.newFixedThreadPool(props.getWorkerThreads());
        List<JobWorker> workers = new ArrayList<>();
        for (int i = 0; i < props.getWorkerThreads(); i++) {
            workers.add(new JobWorker(store, retryPolicy, handler,
                    java.time.Clock.systemUTC(), props.getIdleSleepMillis()));
        }
        workers.forEach(executor::submit);
        log.info("started {} job worker threads", workers.size());
        return executor;
    }
}
