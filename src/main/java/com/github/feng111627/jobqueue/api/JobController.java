package com.github.feng111627.jobqueue.api;

import com.github.feng111627.jobqueue.core.Job;
import com.github.feng111627.jobqueue.core.JobService;
import com.github.feng111627.jobqueue.core.SubmitResult;
import com.github.feng111627.jobqueue.core.SubmitStatus;
import com.github.feng111627.jobqueue.error.JobStorageException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    public record SubmitRequest(String idempotencyKey, String businessKey, String payload) {}

    public record SubmitResponse(String status, String jobId) {}

    @PostMapping
    public ResponseEntity<SubmitResponse> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = jobService.submit(request.idempotencyKey(), request.businessKey(), request.payload());
        HttpStatus httpStatus = result.status() == SubmitStatus.CREATED ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(httpStatus)
                .body(new SubmitResponse(result.status().name(), result.jobId()));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<Job> getById(@PathVariable String jobId) {
        return jobService.findById(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/dead")
    public List<Job> listDead() {
        return jobService.listDead();
    }

    /** 存储层错误统一映射为可区分的 500 响应，不透出底层异常。 */
    @ExceptionHandler(JobStorageException.class)
    public ResponseEntity<Map<String, String>> handleStorage(JobStorageException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "JOB_STORAGE_ERROR", "message", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "INVALID_JOB_REQUEST", "message", e.getMessage()));
    }
}
