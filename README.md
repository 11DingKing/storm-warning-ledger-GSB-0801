# Storm Warning Ledger

多省应急平台暴雨、雷暴大风、冰雹预警的统一接入后端。使用 **append-only 事件流** 处理上游消息的重复、乱序和迟到修订，保证：

- 修订与解除**只新增事件**，永不覆盖或删除历史；
- 同一 `source + external_id + revision` **幂等**；
- 迟到的低修订**留痕但不回退**当前有效状态；
- 事件落库与 outbox 通知在**同一事务**中原子写入；
- 支持当前状态、`as_of` 历史状态和条件检索。

## 技术栈

- Go 1.23+（兼容 Go 1.24，仅用标准库 `net/http` 路由）
- PostgreSQL 16+（开发环境 PostgreSQL 17 兼容，使用 `DISTINCT ON`、`TIMESTAMPTZ`、JSONB）
- 连接池：`pgx/v5`

## 目录结构

```
.
├── cmd/
│   ├── server/            # API 服务入口（自动迁移）
│   ├── seed/              # 5 条消息场景走通 HTTP 的演示程序
│   ├── seed-cnmet/        # cn-met rev1→4（解除后重发 red）场景
│   ├── worker/            # outbox 投递 worker（可多实例并发）
│   └── mock-downstream/   # 测试用下游（按 Idempotency-Key 去重）
├── internal/
│   ├── config/            # 环境变量配置
│   ├── domain/            # 领域模型、状态推导纯函数、服务接口
│   ├── postgres/          # 连接池、迁移器、事务存储、认领投递
│   ├── outbox/            # 分发器、HTTP 投递器、SKIP LOCKED 认领
│   └── httpapi/           # JSON HTTP handler、路由、DTO
├── migrations_sql/        # 由 go:embed 打包进二进制的 SQL 迁移
│   ├── 0001_init.sql
│   └── 0002_outbox_delivery.sql
├── tests/                 # 针对真实 PostgreSQL 的并发集成测试
├── openapi.yaml           # OpenAPI 3.0 规范
└── go.mod
```

## 数据模型

### `warning_events`（append-only 事件流）

| 列 | 说明 |
| --- | --- |
| `id` | `BIGSERIAL`，单调递增，用于稳定排序 |
| `source` / `external_id` | 上游来源与外部事件 ID |
| `revision` | 修订号，≥1 |
| `event_type` | `revision` 或 `cancellation` |
| `warning_type` | `rainstorm` / `thunderstorm_wind` / `hail` |
| `severity` | `blue` / `yellow` / `orange` / `red` |
| `area_code` / `area_name` | 地区编码与名称 |
| `issued_at` / `effective_at` / `expires_at` | 预警时间 |
| `status` | `active` / `cancelled`（上游变体如 `cleared`/`expired`/`解除` 会归一化） |
| `payload` | 原始上游报文 JSONB，留痕 |
| `received_at` | 本服务接收时间（驱动 `as_of`） |
| `recorded_at` | 事务提交时间 |

唯一约束：`UNIQUE(source, external_id, revision)` —— 幂等的根基。
当前状态排序：`revision DESC, id ASC` —— 高修订获胜，同修订取最早记录，**稳定且确定**。

### `warning_outbox`（事务性发件箱 + 可投递队列）

每个新事件在**同一事务**中写入一条 `warning_outbox` 行（`UNIQUE(event_id)`、`UNIQUE(notification_id)`），事件与通知要么同时提交，要么同时回滚。投递相关列由 `0002_outbox_delivery.sql` 增加：

| 列 | 说明 |
| --- | --- |
| `notification_id` | 稳定通知身份 `source/external_id/revision`，重放时不变，作为下游 `Idempotency-Key` |
| `status` | `pending` / `processing` / `dispatched` / `dead` |
| `attempts` / `max_attempts` | 投递次数与上限（默认 10） |
| `next_retry_at` | 下次可认领时间（失败退避） |
| `dispatched_at` | 成功投递时间 |
| `locked_by` / `locked_at` | 当前认领 worker 与认领时间 |
| `last_error` | 最近一次失败原因 |

worker 采用**租约（lease）模式**：`ClaimPending` 用 `SELECT ... FOR UPDATE SKIP LOCKED` 认领一行，立即提交（`status='processing'`、`locked_at=now()`、`attempts+1`），投递在事务外进行；投递成功后置为 `dispatched`，失败则按退避重置为 `pending` 并更新 `next_retry_at`，连续失败达到 `max_attempts` 后置为 `dead`（可查询的终态，不再阻塞队列）。

- **互斥认领**：认领查询只选 `status='pending' AND next_retry_at<=now()` 的行，配合 `FOR UPDATE SKIP LOCKED`，多个 worker 不会同时处理同一行。
- **租约接管**：若 worker 崩溃或投递过慢，行停留在 `processing`；当 `locked_at < now() - lease_ttl` 时，另一个 worker 可以认领该过期租约并重投，保证不丢通知。租约 TTL 由 `--lease-duration` 配置。
- **at-least-once + 幂等**：租约接管/崩溃重放沿用**相同的 `notification_id`**，并通过 `Idempotency-Key` 头发送给下游；下游去重后每个业务通知只生效一次（可能被投递多次）。
- **毒消息终态**：连续失败 `max_attempts` 次（默认 10）后进入 `dead`，可通过 `GET /api/v1/outbox?include_dispatched=true` 查询 `status=dead` 与 `last_error`。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/warnings` | 写入一条修订/解除（幂等） |
| GET | `/api/v1/warnings` | 按当前状态检索（area_code/warning_type/severity/status/active_only/limit/offset） |
| GET | `/api/v1/warnings/{source}/{external_id}` | 当前有效状态 |
| GET | `/api/v1/warnings/{source}/{external_id}/history` | 完整事件流（含 superseded 标记） |
| GET | `/api/v1/warnings/{source}/{external_id}/as-of?at=<RFC3339>` | 指定时刻的历史状态 |
| GET | `/api/v1/outbox?include_dispatched=false` | 查询 outbox 通知（含 `notification_id`/`status`/`attempts`） |
| GET | `/healthz` | 健康检查 |

写入请求体示例：

```json
{
  "source": "CMA",
  "external_id": "BJ-2026-RAIN-001",
  "revision": 1,
  "warning_type": "rainstorm",
  "severity": "orange",
  "area_code": "110000",
  "area_name": "北京市",
  "issued_at": "2026-08-02T10:00:00Z",
  "effective_at": "2026-08-02T10:00:00Z",
  "expires_at": "2026-08-02T18:00:00Z",
  "status": "active",
  "payload": { "headline": "暴雨橙色预警" }
}
```

响应：新事件 `201 Created`，重复修订 `200 OK` 且 `deduplicated: true`。

完整字段见 [openapi.yaml](openapi.yaml)。

## 快速开始

### 1. 创建数据库

```bash
createdb warning_ledger
createdb warning_ledger_test   # 可选，用于集成测试
```

### 2. 执行迁移

服务启动时自动执行；也可单独执行：

```bash
DATABASE_URL="postgres:///warning_ledger?sslmode=disable" \
  go run ./cmd/server -migrate
```

或直接用 psql（按顺序执行两个迁移）：

```bash
psql -d warning_ledger -f internal/postgres/migrations_sql/0001_init.sql
psql -d warning_ledger -f internal/postgres/migrations_sql/0002_outbox_delivery.sql
```

### 3. 启动 API

```bash
DATABASE_URL="postgres:///warning_ledger?sslmode=disable" \
  PORT=8080 go run ./cmd/server
```

### 4. 运行场景演示

`cmd/seed` 会在进程内启动真实 HTTP 服务，让 5 条消息走过完整链路：

```bash
DATABASE_URL="postgres:///warning_ledger?sslmode=disable" go run ./cmd/seed
```

消息顺序：修订1 → 修订2 → 迟到修订1 → 修订1重复 → 修订3解除。
预期：5 条消息产生 **3 个事件行 + 3 条 outbox**，当前状态为修订3解除，as-of 可回放任意时刻。

### 5. cn-met 场景：解除后重发 revision 4

`cmd/seed-cnmet` 复现 `cn-met/rainstorm-2026-0801-hb-001` 的生命周期：rev1(yellow) → rev2(orange) → rev3(cancelled) → rev4(active, red, effective 10:15)，并重复提交 rev4 验证幂等。rev3 的解除记录**不被改写**，rev4 作为新事件追加，通知身份固定为 `cn-met/rainstorm-2026-0801-hb-001/4`。

```bash
DATABASE_URL="postgres:///warning_ledger?sslmode=disable" go run ./cmd/seed-cnmet
```

### 6. 启动投递 worker（可多实例并发）

先启动一个下游接收端（测试用，按 `Idempotency-Key` 去重）：

```bash
go run ./cmd/mock-downstream -port 9900
```

再启动两个 worker（不同 `--worker-id`），它们会用 `FOR UPDATE SKIP LOCKED` 分摊队列：

```bash
DATABASE_URL="postgres:///warning_ledger?sslmode=disable" \
  go run ./cmd/worker --worker-id=A --downstream-url=http://127.0.0.1:9900/notify &
DATABASE_URL="postgres:///warning_ledger?sslmode=disable" \
  go run ./cmd/worker --worker-id=B --downstream-url=http://127.0.0.1:9900/notify &
```

`--once` 处理完当前 pending 后退出。worker 参数：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--lease-duration` | 30s | 认领租约 TTL；超过该时间仍为 `processing` 的行可被其他 worker 接管 |
| `--backoff` | -1（指数 1s,2s…） | 失败后固定退避；`0` 表示立即重试（测试用） |
| `--crash-after-deliver` | false | 首次投递成功后、写 `dispatched_at` 前退出，模拟崩溃 |
| `--once` | false | 处理完当前 pending 后退出 |

#### 租约接管（revision 4 崩溃后第二个 worker 接管）

```bash
# 1) worker A 投递 rev4 后崩溃，行停留在 processing（认领已提交）
go run ./cmd/worker --worker-id=crash-A --crash-after-deliver \
  --lease-duration=2s --downstream-url=http://127.0.0.1:9900/notify --once
# 2) 等待 --lease-duration 后，worker B 接管并重投，沿用相同 notification_id；
#    下游凭 Idempotency-Key 去重，rev4 最终只有一个业务通知
go run ./cmd/worker --worker-id=takeover-B \
  --lease-duration=2s --downstream-url=http://127.0.0.1:9900/notify --once
```

#### 毒消息进入 dead 终态

```bash
# 让下游对指定 notification_id 始终返回 500
go run ./cmd/mock-downstream -port 9901 -fail cn-met/delivery-poison-01/1 &
# 将该通知的 max_attempts 设为 3，用立即退避连续投递 3 次
for i in 1 2 3; do
  go run ./cmd/worker --downstream-url=http://127.0.0.1:9901/notify \
    --lease-duration=10s --backoff=0 --once
done
# 第 3 次失败后 status=dead，可通过 API 查询：
curl 'http://localhost:8080/api/v1/outbox?include_dispatched=true'
```

投递重试与 outbox 状态变化**只影响 `warning_outbox`**，不会改写 `warning_events`，因此当前状态、`as_of` 历史状态和 history 视图在任意次重试后保持不变。

### 7. 运行测试

```bash
# 领域纯逻辑单元测试（无需 DB）
go test ./internal/domain/ -v

# 全部测试（含真实 PostgreSQL 的并发集成测试）
TEST_DATABASE_URL="postgres:///warning_ledger_test?sslmode=disable" \
  go test -race -count=1 ./...
```

未设置 `TEST_DATABASE_URL` 时默认连接 `postgres:///warning_ledger_test?sslmode=disable`；
若数据库不可达，集成测试会打印 skip 信息并以 0 退出（方便在无 DB 环境编译验证）。

## 关键正确性保证

### 幂等

`INSERT ... ON CONFLICT (source, external_id, revision) DO NOTHING RETURNING id`。
并发写入同一修订时，PostgreSQL 唯一约束串行化冲突：一个事务插入，其余阻塞后读到已有行并返回，无 500、无重复行。

### 乱序与迟到

当前状态由 `ORDER BY revision DESC, id ASC` 推导，与消息到达顺序无关。
低修订即使后到也排在高修订之后，**无法回退**状态；解除事件（最高修订）后到的低修订更不会复活预警。
所有事件保留在 history 中并标记 `superseded`。

### 事件与通知原子性

事件插入和 outbox 插入在同一个 `pgx.Tx` 中。若 outbox 写入前进程崩溃/出错，事务回滚，事件行一并消失，不会出现"有事件无通知"的中间态。重试时整事务重新执行，幂等保证安全。

### 事务回滚验证

测试通过 context 注入"事件插入后、outbox 插入前失败"，断言事件和 outbox 行数均为 0，随后重试成功，得到 1 事件 + 1 outbox。

### as-of 历史状态

`GET .../as-of?at=T` 只统计 `received_at <= T` 的事件，再取最高修订。这回答了"在 T 时刻我们认为预警处于什么状态"，即使之后有迟到/重复消息到达。

### 稳定排序

状态推导排序 `revision DESC, id ASC` 完全确定：id 严格单调，无时间戳同值歧义。测试重复读取 20 次结果一致，并在并发混合修订下验证。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgres:///warning_ledger?sslmode=disable` | PostgreSQL 连接串 |
| `PORT` | `8080` | HTTP 监听端口 |
| `TEST_DATABASE_URL` | `postgres:///warning_ledger_test?sslmode=disable` | 集成测试库（仅测试时） |

## 分层说明

- **domain**：无外部依赖的纯类型与纯函数（`NormalizeAndValidate`、`ProjectCurrent`、`ProjectAsOf`、`BuildHistory`），`Repository` 接口在此定义。
- **postgres**：实现 `domain.Repository`，管理连接池、迁移、事务、outbox。
- **httpapi**：标准库 `net/http`（Go 1.22+ 路由），负责 JSON 编解码与参数校验，不含业务逻辑。
