# Idempotent Job Queue

一个具备幂等提交、自动重试、死信与并发安全语义的后台作业队列服务（Spring Boot + 内存存储实现）。

## 作业生命周期

```
submit ──> PENDING ──claim──> RUNNING ──成功──> SUCCEEDED
                            │
                            └─失败─┬─ attempts < maxAttempts ─> PENDING(延迟 nextAttemptAt)
                                   └─ attempts >= maxAttempts ─> DEAD(保留失败原因)
```

- `SUCCEEDED` 与 `DEAD` 是终态：状态迁移只允许从 `RUNNING` 出发，已成功或已死信的作业不会被重试链路再次执行。
- 每次失败都会把异常消息写入 `lastError`，进入死信后可通过 `GET /jobs/dead` 查询。

## 幂等语义

- 提交作业必须携带 `idempotencyKey`。存储层以幂等键为唯一索引，用 `putIfAbsent` 原子写入。
- 同一幂等键无论被顺序还是并发重复提交，只会有一个作业真正生成并执行：
  - 首次提交返回 `{"status": "CREATED", "jobId": "..."}`（HTTP 201）；
  - 重复提交返回 `{"status": "ALREADY_EXISTS", "jobId": "<已存在的作业ID>"}`（HTTP 200），调用方可以明确识别，而不是收到一个说不清的错误。
- 不同幂等键各自生成独立作业。

## 重试与退避规则

重试策略由 `RetryPolicy` 实现，可通过配置项调整（`JobQueueProperties`，前缀 `jobqueue`）：

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `jobqueue.max-attempts` | 3 | 最大尝试次数（含首次执行），超过即进入死信 |
| `jobqueue.initial-delay` | 100ms | 首次失败后的退避延迟 |
| `jobqueue.multiplier` | 2.0 | 退避倍数（>= 1.0） |
| `jobqueue.max-delay` | 5s | 退避延迟上限 |
| `jobqueue.worker-threads` | 4 | 工作线程数 |

第 n 次失败后的等待时长为 `min(initialDelay * multiplier^(n-1), maxDelay)`。
未到 `nextAttemptAt` 的作业不会被工作线程领取。

## 并发语义

- **领取原子性**：`claimNextDue` 在作业对象上同步，把到期作业从 `PENDING` 原子置为 `RUNNING`，多个工作线程不会重复领取同一作业。
- **业务键串行**：同一 `businessKey` 的作业在执行体外套一把公平锁（`withBusinessKeyLock`），保证同一业务键的作业不并发执行，且按提交顺序完成；不同业务键之间可以并行。
- **错误类型隔离**：存储层抛出的底层异常统一包装为 `JobStorageException`（继承自 `JobQueueException`），REST 层映射为 `{"error": "JOB_STORAGE_ERROR"}` 的 500 响应；参数非法映射为 `INVALID_JOB_REQUEST` 的 400 响应。框架/底层异常不会原样透出。工作线程遇到存储错误只记录日志并继续存活。

## API

- `POST /jobs` — 提交作业，请求体 `{"idempotencyKey": "...", "businessKey": "...", "payload": "..."}`
- `GET /jobs/{jobId}` — 查询作业状态、尝试次数、失败原因
- `GET /jobs/dead` — 列出死信作业

## 如何验证

运行全部测试：

```bash
mvn test
```

测试日志中每个用例都会打印 `[TEST-CASE] PASS/FAIL <场景描述>`，覆盖：

- **幂等**（`IdempotentSubmitTest`）：顺序重复提交返回 `ALREADY_EXISTS`；16 线程并发重复提交只生成一个作业；不同幂等键各自独立。
- **重试与退避**（`RetryPolicyTest`、`RetryAndDeadLetterTest`）：指数退避与上限；退避窗口内不被领取；前两次失败第三次成功；已成功作业不再执行。
- **死信**（`RetryAndDeadLetterTest`）：超过重试上限进入 `DEAD` 并保留失败原因；死信作业不会被再次执行。
- **并发竞争**（`ConcurrentExecutionTest`）：多工作线程下同一业务键不并发执行且按提交顺序完成；不同业务键可并行。
- **错误映射**（`ErrorMappingTest`）：底层存储异常包装为 `JobStorageException`；工作线程在存储错误下存活；非法参数返回可识别错误。
- **集成**（`ApplicationContextTest`）：Spring 上下文装配与提交链路端到端可用。
