# Idempotent Job Queue

一套后台作业处理能力：作业从提交、执行、重试到死信的全过程，在重复提交与并发执行下都能给出可预期、可解释的结果。

## 核心概念

### 作业生命周期

```
PENDING --claim--> RUNNING --success--> SUCCEEDED (终态)
                       |
                       +--failure, attempts < maxAttempts--> RETRY_WAITING --到期--> RUNNING
                       |
                       +--failure, attempts >= maxAttempts--> DEAD (终态)
```

- `SUCCEEDED` 与 `DEAD` 是终态：终态作业不会再被领取、不会再被重试链路触碰，针对终态作业的状态迁移请求会被静默忽略。
- 死信作业永久保留 `lastError`（最后一次失败原因）与 `attempts`，可通过查询接口审计。

### 幂等语义

- 提交作业必须携带 `idempotencyKey`。
- 同一幂等键无论被顺序还是并发提交多少次，**只有一次**真正创建并入队作业，返回 `CREATED`（HTTP 201）；其余提交返回 `ALREADY_EXISTS`（HTTP 200）并携带已存在的作业，不会重复执行，也不会抛出含糊的错误。
- 幂等判断在存储层原子完成（`JobStore.insertIfAbsent`），并发提交不会产生两个作业。

### 重试与退避规则

- 执行器（`JobHandler`）抛出任何异常都视为失败，自动进入重试链路。
- 退避间隔：`initialDelay * multiplier^(attempt-1)`，封顶 `maxDelay`。第 n 次失败后等待该间隔再被重新领取。
- 达到 `maxAttempts`（默认 3）后进入死信（`DEAD`）。
- 配置项（`application.properties`）：

| 配置 | 默认值 | 说明 |
|---|---|---|
| `jobqueue.max-attempts` | 3 | 默认最大尝试次数（含首次执行） |
| `jobqueue.retry.initial-delay-ms` | 100 | 首次退避间隔 |
| `jobqueue.retry.multiplier` | 2.0 | 退避倍率 |
| `jobqueue.retry.max-delay-ms` | 5000 | 退避间隔上限 |
| `jobqueue.workers` | 4 | 工作线程数 |

### 并发语义

- 多个工作线程并发领取作业；**“领取 + 登记在途业务键”原子完成**，保证同一 `businessKey` 任意时刻最多一个作业在执行。
- 同一业务键的作业严格按提交顺序逐个完成；不同业务键的作业可以并行。
- 存储/队列层的底层异常一律包装为 `JobStorageException`（`JobQueueException` 子类）对外暴露，原始异常仅作为 `cause` 保留，不会直接透出框架或底层异常类型；REST 层映射为 `503 JOB_STORAGE_ERROR`。

## REST 接口

- `POST /api/jobs` — 提交作业
  ```json
  { "idempotencyKey": "order-123-pay", "businessKey": "order-123", "payload": "...", "maxAttempts": 3 }
  ```
  响应：`201 {"outcome":"CREATED", "job":{...}}` 或 `200 {"outcome":"ALREADY_EXISTS", "job":{...}}`
- `GET /api/jobs/{id}` — 按作业 id 查询状态/失败原因
- `GET /api/jobs/by-key/{idempotencyKey}` — 按幂等键查询

## 代码结构

```
job/
  Job.java               作业实体与状态
  JobStatus.java         生命周期状态枚举
  JobStore.java          存储抽象（所有状态迁移原子完成）
  InMemoryJobStore.java  内存实现：幂等插入、原子领取、终态保护
  JobService.java        提交/查询门面，幂等语义入口
  JobWorkerPool.java     多线程消费：业务键串行保序、失败重试、死信
  RetryPolicy.java       可配置指数退避策略
  SubmitResult.java      CREATED / ALREADY_EXISTS 提交结果
  JobQueueException / JobAlreadyExistsException / JobStorageException
web/
  JobController.java     REST 入口与异常映射
```

## 如何验证

运行全部测试：

```bash
mvn test
```

测试日志会逐条打印覆盖的业务场景与结果（`[场景] ...` / `[结果] ... -> PASS`）：

| 测试类 | 覆盖场景 |
|---|---|
| `IdempotencyTest` | 顺序/并发重复提交同一幂等键，仅一个 `CREATED`，作业只执行一次 |
| `RetryBackoffTest` | 失败后自动重试直至成功；退避间隔指数增长并封顶；未到重试时间不执行 |
| `DeadLetterTest` | 超限进入死信并保留失败原因；死信/成功作业不再被执行、状态不可变更 |
| `ConcurrencyTest` | 同业务键串行且按提交顺序完成；不同业务键可并行 |
| `ExceptionTranslationTest` | 底层异常包装为 `JobStorageException`；非法参数抛领域异常；工作线程容错并恢复 |

启动服务手工验证：

```bash
mvn spring-boot:run
# 重复提交同一幂等键，第二次返回 ALREADY_EXISTS
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","businessKey":"b1","payload":"hello"}'
curl localhost:8080/api/jobs/by-key/k1
```
