# storm-warning-ledger

多省应急平台统一的暴雨 / 雷暴大风 / 冰雹预警生命周期后端。针对上游消息**重复、乱序、解除后仍收到旧修订**的场景设计：

- **append-only 事件流**：修订与解除只能新增事件，数据库触发器禁止对事件流 `UPDATE` / `DELETE`；
- **幂等**：同一 `source + external_id + revision` 由唯一约束保证全库只落一行，重复提交返回 `replayed`，同号不同内容返回 `409`；
- **修订号驱动状态**：更高修订（含 `cancelled` 解除）成为当前有效状态；更低修订的迟到消息只留痕（`late`），不回退当前状态；
- **事务性 outbox**：事件、当前状态物化、通知在同一事务提交——要么全部可见，要么整体回滚；
- **可投递、可重试的通知流水线**：独立 dispatcher worker 以短事务租约（`FOR UPDATE SKIP LOCKED` + `claimed_until`）认领通知；持有者崩溃/卡住后租约过期即被其他 worker 接管，`claim_token` 围栏拒绝旧持有者误标；重投始终沿用同一通知身份 `source/external_id/revision`，下游凭 `X-Notification-ID` 幂等，一条修订最终只产生一个业务通知；连续失败达到上限（默认 3 次）进入死信终止状态，`GET /v1/outbox/dead` 可查询；
- **as_of 历史**：可按任意时刻重建当时的有效状态；
- **稳定排序**：审计轨迹按自增 `id`（接收顺序）返回，检索按 `(source, external_id)` keyset 分页。

技术栈：Go 1.24（仅标准库 + pgx/v5）、PostgreSQL 16。无 Docker 依赖，原生运行。

## 目录结构

```
cmd/server/            API 服务入口（启动时自动执行迁移）
cmd/dispatcher/        outbox 投递 worker（可启动任意多个实例）
internal/domain/       领域模型与纯决策逻辑（修订决策、指纹、as_of 折叠、校验）
internal/store/        PostgreSQL 持久化（事务、唯一约束、outbox 认领/标记）+ 内置 migrator
internal/dispatch/     投递循环：租约认领 → 事务外投递 → token 围栏标记，退避与死信
internal/httpapi/      HTTP JSON API（协议转换与错误映射）
internal/testutil/     集成测试隔离（每测试独立 schema，包间并发安全）
migrations/            SQL 迁移（embed 进二进制）
openapi.yaml           OpenAPI 3.0 规范
scripts/seed.sh        通过真实接口回放典型消息序列的演示脚本
```

## 快速开始（原生环境）

前置：Go 1.24+、可连接的 PostgreSQL 16（14+ 亦可，SQL 无版本特性依赖）。

```bash
# 1. 建库
createdb -h localhost -p 5433 storm_warning
createdb -h localhost -p 5433 storm_warning_test

# 2. 执行迁移（幂等，可重复）
DATABASE_URL="postgres://localhost:5433/storm_warning?sslmode=disable" \
  go run ./cmd/server -migrate-only

# 3. 运行测试（领域单测不需要数据库；集成测试需 TEST_DATABASE_URL）
TEST_DATABASE_URL="postgres://localhost:5433/storm_warning_test?sslmode=disable" go test ./...

# 4. 启动 API（默认 :8080）
DATABASE_URL="postgres://localhost:5433/storm_warning?sslmode=disable" go run ./cmd/server

# 5. 启动投递 worker（可同时启动多个；WEBHOOK_URL 为空则只打日志）
WEBHOOK_URL="http://localhost:9090/" DATABASE_URL="postgres://localhost:5433/storm_warning?sslmode=disable" \
  go run ./cmd/dispatcher

# 6. 另开终端，回放种子数据（修订 1 / 修订 2 / 重复修订 1 / 冲突修订 1 / 修订 3 解除 + 乱序序列）
./scripts/seed.sh
```

环境变量：`DATABASE_URL`（默认 `postgres://localhost:5433/storm_warning?sslmode=disable`）、`LISTEN_ADDR`（默认 `:8080`）、`TEST_DATABASE_URL`（仅测试）；dispatcher 另有 `WEBHOOK_URL`、`POLL_INTERVAL`（默认 `1s`）、`BATCH_SIZE`（默认 `32`）、`LEASE_DURATION`（默认 `30s`，必须大于下游 p99 响应时间）、`MAX_ATTEMPTS`（默认 `3`）。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/warnings/events` | 写入修订/解除消息；`outcome ∈ {applied, late, replayed}` |
| GET | `/v1/warnings/{source}/{external_id}` | 当前有效状态；`?as_of=<RFC3339>` 查历史状态 |
| GET | `/v1/warnings/{source}/{external_id}/events` | 完整审计轨迹（含迟到留痕），按接收顺序 |
| GET | `/v1/warnings` | 检索当前预警：`region_code` / `status` / `severity` / `limit` / `cursor` |
| GET | `/v1/outbox/dead` | 查询死信（连续失败达到上限的终止通知） |
| GET | `/healthz` | 存活探针 |

写入请求体（字段含义见 [openapi.yaml](openapi.yaml)）：

```json
{
  "source": "cn-met",
  "external_id": "rainstorm-2026-0801-hb-001",
  "revision": 2,
  "severity": "red",
  "status": "active",
  "region_code": "420000",
  "headline": "暴雨红色预警（修订 2）",
  "description": "……",
  "issued_at": "2026-08-01T08:00:00+08:00",
  "effective_at": "2026-08-01T09:00:00+08:00",
  "expires_at": "2026-08-02T09:00:00+08:00"
}
```

状态码：`201` applied/late，`200` replayed，`409` 同号不同内容冲突，`422` 输入校验失败（`fields` 给字段级错误），`404` 序列不存在。

## 并发与一致性设计（压测关注点对应）

| 关注点 | 机制 | 证明它的测试 |
| --- | --- | --- |
| 同一 revision 并发提交 | `UNIQUE(source, external_id, revision)` + 序列级 `pg_advisory_xact_lock`；败者回滚后按指纹判定重放/冲突 | `TestConcurrentSameRevision`（16 并发 → 恰好 1 事件 / 1 outbox） |
| 事件落库后、outbox 前失败 | 事件 + current + outbox 在同一事务；故障即整体回滚，重试安全 | `TestRollbackWhenOutboxFails`（注入故障 → 三表皆空 → 重试成功） |
| 乱序 / 解除后收到旧修订 | 领域规则 `Decide`：只有更高修订才应用；解除同样只是“更高修订的一种” | `TestAppendLifecycle`（解除后 rev2 迟到，`late` 不复活）、`TestCurrentAsOf` |
| 解除后恢复发布（rev4） | rev4 是更高修订，正常应用；rev3 解除记录是 append-only 事件，不受影响；重复提交 rev4 返回首次的事件与 outbox | `TestRev4AfterCancellation` |
| 两个 worker 并发投递 | 认领为短事务 `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED)`，一行同一时刻只属一个租约持有者 | `TestTwoWorkersNoDoubleClaim`（8 行 × 2 worker，每行恰好投递一次） |
| 持有者崩溃后接管 | 租约（`claimed_until`）过期才可被接管；接管轮换 `claim_token`，旧持有者标记被 `ErrLeaseLost` 围栏拒绝；重投身份不变，业务通知只有一个；事件流 / 当前状态 / as_of 均不被污染 | `TestLeaseExpiryTakeover` |
| 毒丸连续失败 | `attempts` 达到 `MAX_ATTEMPTS`（默认 3）写入 `dead_lettered_at`，不再被认领，`GET /v1/outbox/dead` 可查 | `TestDeadLetterAfterThreeFailures`、`TestDeadLettersEndpoint` |
| 下游故障重试 | `attempts` + `next_attempt_at` 指数退避，身份不变 | `TestRetryWithBackoff` |
| 稳定排序 | 事件按 `BIGSERIAL id`；检索按主键 `(source, external_id)` keyset 分页 | `TestSearchStableOrderAndPagination`、审计轨迹断言 |
| append-only | 触发器拒绝 `UPDATE`/`DELETE` | `TestAppendOnlyEnforced` |
| as_of 与实时语义一致 | 实时与历史共用同一规则（“截止 t 已收到的最大修订”），`domain.AsOf` 单测 | `TestAsOf`、`TestCurrentAsOf` |

设计取舍：

- 迟到事件与幂等重放**不产生 outbox 通知**（状态未变），审计轨迹里始终可见；
- 咨询锁以 `hashtextextended(source || \x1f || external_id)` 为键，只串行化同一事件序列的写入，不同序列完全并行；
- 重放判定读取的是**已提交**的行（唯一冲突只会在对方提交后抛出），无脏读。

## 通知投递（事务性 outbox + 租约）

- 通知身份 `notification_id = source/external_id/revision`，由 `warning_outbox` 上的唯一约束保证一行通知对应一个身份；webhook 请求头携带 `X-Notification-ID`，下游按它幂等去重——无论崩溃重放还是租约接管，一条修订最终只产生一个业务通知；
- 认领是**短事务**：`UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED)` 写入 `claimed_by` / `claimed_until` / `claim_token` 即提交，投递在事务外进行，不长时间持有行锁；
- 持有者崩溃或卡住：租约过期（`claimed_until < now()`）后其他 worker 接管并轮换 `claim_token`；旧持有者事后标记会被 `ErrLeaseLost` 围栏拒绝，不会把别人接管的行误标为已投递；
- 失败簿记：`attempts`、`next_attempt_at`（指数退避，1s/2s/4s…封顶 64s）、`last_error`；连续失败达到 `MAX_ATTEMPTS`（默认 3）写入 `dead_lettered_at` 进入终止状态，不再被认领，`GET /v1/outbox/dead` 可查询；
- 配置纪律：`LEASE_DURATION` 必须大于下游 p99 响应时间，否则租约会反复易主（接管重投无害但浪费）；
- 投递循环只读写 `warning_outbox`，不触碰事件流与当前状态——重试、接管、死信都不会污染 `current`、`as_of` 历史或审计轨迹。

## 数据模型

- `warning_events`：append-only 事件流，唯一约束 `(source, external_id, revision)`，`fingerprint` 为规范化内容摘要；
- `warning_current`：每个序列的当前有效状态（物化读模型，检索走这里）；
- `warning_outbox`：状态变更通知，唯一约束 `(source, external_id, revision)` 即通知身份；`dispatched_at IS NULL` 为待投递，`attempts` / `next_attempt_at` / `last_error` 支持退避重试，`claimed_by` / `claimed_until` / `claim_token` 为投递租约与围栏，`dead_lettered_at` 为终止状态；与事件同事务写入。
