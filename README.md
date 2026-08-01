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
│   └── seed/              # 5 条消息场景走通 HTTP 的演示程序
├── internal/
│   ├── config/            # 环境变量配置
│   ├── domain/            # 领域模型、状态推导纯函数、服务接口
│   ├── postgres/         # 连接池、迁移器、事务存储、outbox
│   └── httpapi/           # JSON HTTP handler、路由、DTO
├── migrations_sql/        # 由 go:embed 打包进二进制的 SQL 迁移
│   └── 0001_init.sql
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

### `outbox`（事务性发件箱）

每个新事件在**同一事务**中写入一条 outbox 行（`UNIQUE(event_id)`），由独立的 relay 进程轮询 `published_at IS NULL` 发布。事件与通知要么同时提交，要么同时回滚。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/warnings` | 写入一条修订/解除（幂等） |
| GET | `/api/v1/warnings` | 按当前状态检索（area_code/warning_type/severity/status/active_only/limit/offset） |
| GET | `/api/v1/warnings/{source}/{external_id}` | 当前有效状态 |
| GET | `/api/v1/warnings/{source}/{external_id}/history` | 完整事件流（含 superseded 标记） |
| GET | `/api/v1/warnings/{source}/{external_id}/as-of?at=<RFC3339>` | 指定时刻的历史状态 |
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

或直接用 psql：

```bash
psql -d warning_ledger -f internal/postgres/migrations_sql/0001_init.sql
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

### 5. 运行测试

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
