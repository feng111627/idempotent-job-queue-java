package com.github.feng111627.idempotentjobqueuejava.web;

import com.github.feng111627.idempotentjobqueuejava.job.Job;
import com.github.feng111627.idempotentjobqueuejava.job.JobQueueException;
import com.github.feng111627.idempotentjobqueuejava.job.JobService;
import com.github.feng111627.idempotentjobqueuejava.job.JobStorageException;
import com.github.feng111627.idempotentjobqueuejava.job.SubmitResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 作业提交/查询 REST 接口。
 *
 * - 提交：新建返回 201 + outcome=CREATED；幂等命中返回 200 + outcome=ALREADY_EXISTS，
 *   调用方能明确识别“已存在”，不会拿到含糊的 5xx；
 * - 存储层异常统一映射为 503 + 可区分的错误类型，不向外透出底层异常细节。
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    public record SubmitRequest(String idempotencyKey, String businessKey, String payload,
                                Integer maxAttempts) {
    }

    @PostMapping
    public ResponseEntity<?> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = request.maxAttempts() == null
                ? jobService.submit(request.idempotencyKey(), request.businessKey(), request.payload())
                : jobService.submit(request.idempotencyKey(), request.businessKey(), request.payload(),
                        request.maxAttempts());
        HttpStatus status = result.isCreated() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(Map.of(
                "outcome", result.getOutcome().name(),
                "job", toView(result.getJob())));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable String id) {
        return jobService.findById(id)
                .<ResponseEntity<?>>map(job -> ResponseEntity.ok(toView(job)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "JOB_NOT_FOUND", "id", id)));
    }

    @GetMapping("/by-key/{idempotencyKey}")
    public ResponseEntity<?> getByIdempotencyKey(@PathVariable String idempotencyKey) {
        return jobService.findByIdempotencyKey(idempotencyKey)
                .<ResponseEntity<?>>map(job -> ResponseEntity.ok(toView(job)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "JOB_NOT_FOUND", "idempotencyKey", idempotencyKey)));
    }

    private Map<String, Object> toView(Job job) {
        return Map.of(
                "id", job.getId(),
                "idempotencyKey", job.getIdempotencyKey(),
                "businessKey", job.getBusinessKey(),
                "status", job.getStatus().name(),
                "attempts", job.getAttempts(),
                "maxAttempts", job.getMaxAttempts(),
                "lastError", job.getLastError() == null ? "" : job.getLastError());
    }

    @ExceptionHandler(JobStorageException.class)
    public ResponseEntity<?> handleStorage(JobStorageException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "JOB_STORAGE_ERROR", "message", e.getMessage()));
    }

    @ExceptionHandler(JobQueueException.class)
    public ResponseEntity<?> handleBadRequest(JobQueueException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "JOB_QUEUE_ERROR", "message", e.getMessage()));
    }
}
