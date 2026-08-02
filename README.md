# Storm Warning Ledger

暴雨、雷暴大风、冰雹预警的统一接入后端。采用 **append-only 事件流**，上游消息的重复、乱序和迟到修订都不会破坏当前有效状态，且全部历史事件完整保留可审计、可按任意时间点重建状态。

* Go 1.24（工具链在需要时会自动切换到兼容版本）
* PostgreSQL 16
* 无 ORM，直接使用 `pgx/v5`
* 原生 `net/http`（Go 1.22+ 方法+路径模式路由）

## 架构分层

```
cmd/server            程序入口、配置、迁移命令、HTTP server
internal/domain       领域模型、状态重放、修订比较、稳定排序（纯逻辑，无 IO）
internal/repository   PostgreSQL 连接、迁移、事件表/outbox 原子写入、查询
internal/service      应用服务：幂等判断、迟到消息标记、历史/当前/as-of 读取
internal/http         HTTP JSON handler
scripts/seed          演示用的 5 条消息序列写入脚本
test                  集成测试（真实 PostgreSQL）
migrations            SQL 迁移文件（与 internal/repository/migrations_sql 保持一致）
openapi.yaml          OpenAPI 3.0 规范
```

核心设计：

* `warning_events` 表只追加，不更新、不删除。
* 唯一约束 `(source, external_id, revision)` 保证同修订号幂等，并发下只有一个请求能写入。
* 迟到的低修订号事件会被写入并标记 `is_late=true`，但 `Replay` 重放时只认"已见过的最高修订号"，因此不会回退当前状态。
* 事件和 outbox 通知在**同一个数据库事务**里提交。`fail_before_outbox=true` 查询参数可在事件插入之后、outbox 插入之前强制失败，用于验证原子性——事务回滚后事件和 outbox 都不会落库，重试后两者同时出现。
* 历史稳定排序：`ORDER BY revision ASC, id ASC`，重放时再叠加 `received_at` 作为 tie-breaker。
* `as_of` 查询按 `received_at` 过滤事件后重放，返回当时的有效状态。

## 数据库

### 环境变量

```
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=storm_warning
DB_SSLMODE=disable
```

### 创建数据库

```sql
CREATE DATABASE storm_warning;
```

### 运行迁移

服务启动时会自动执行迁移。也可单独运行：

```bash
# 升级
go run ./cmd/server -migrate

# 回滚最后一个迁移
go run ./cmd/server -migrate-down
```

## 启动 API

```bash
go run ./cmd/server
# 默认监听 :8080，可用 -addr 覆盖
```

健康检查：

```bash
curl http://localhost:8080/health
```

## HTTP API

完整规范见 [openapi.yaml](openapi.yaml)。

### 写入修订

```bash
curl -X POST http://localhost:8080/api/v1/warnings \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "cma",
    "external_id": "WARN-2026-001",
    "revision": 1,
    "warning_type": "rain",
    "severity": "yellow",
    "status": "active",
    "issued_at": "2026-08-03T08:00:00Z",
    "effective_at": "2026-08-03T08:00:00Z",
    "expires_at": "2026-08-03T14:00:00Z",
    "region_codes": ["110000"],
    "payload": {"headline": "暴雨黄色预警"}
  }'
```

返回：

* `201` 新修订已应用（`result=applied`）
* `200` 重复修订（`result=duplicate`，幂等返回原事件）
* 迟到的低修订号返回 `result=late` 且 `is_late=true`

### 查询当前状态

```bash
curl http://localhost:8080/api/v1/warnings/cma/WARN-2026-001
```

### 查询历史状态（as-of）

```bash
curl "http://localhost:8080/api/v1/warnings/cma/WARN-2026-001?as_of=2026-08-03T09:00:00Z"
```

### 完整事件历史

```bash
curl http://localhost:8080/api/v1/warnings/cma/WARN-2026-001/history
```

### 检索

```bash
curl "http://localhost:8080/api/v1/warnings?status=active&warning_type=rain&region_code=110000&limit=20&offset=0"
```

## 题目要求的 5 条消息演示

1. 修订 1（黄色 active）
2. 修订 2（升级橙色）
3. 迟到的修订 1（与第 1 条同 revision → 幂等重复，不落第二条）
4. 修订 1 重复消息（同上，再返回重复）
5. 修订 3（解除 cancelled）

先启动 API，然后运行种子脚本：

```bash
go run ./scripts/seed
# 可通过 -api 指定地址、-source/-id 指定数据源和事件 ID
```

脚本最后会打印当前状态（应为 rev3 / cancelled）和完整历史（rev1、rev2、rev3 三条事件，重复修订不新增行）。

## 测试

```bash
# 纯领域逻辑单元测试（无需数据库）
go test ./internal/domain/...

# 全部测试（集成测试需要一个可连接的 PostgreSQL）
go test ./...
```

集成测试通过环境变量连接数据库：

```
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/storm_warning_test?sslmode=disable
# 或
DB_HOST=localhost DB_USER=postgres DB_PASSWORD=postgres TEST_DB_NAME=storm_warning_test
```

测试库需要提前创建：

```sql
CREATE DATABASE storm_warning_test;
```

### 测试覆盖的关键场景

* **生命周期序列**：rev1 → rev2 → 迟到 rev1（重复）→ rev1 重复 → rev3 解除，验证状态与历史条数。
* **真正迟到的低修订号**：先写 rev2，再写一个从未见过的 rev1，验证事件被 append 并标记 `is_late`，但当前状态不回退。
* **同 revision 并发幂等**：10 个 goroutine 同时写同一 revision，断言只有 1 个 applied、其余 duplicate，数据库只有 1 行。
* **事件与 outbox 原子性**：正常写入后事件和 outbox 各 1 行；`fail_before_outbox=true` 触发回滚后两者均为 0；重试成功后两者各 1 行。
* **as-of 历史重建**：未来时间点看到最新状态，远古时间点返回 not found。
* **乱序稳定排序**：rev3、rev2、rev1、rev2 重复写入后，历史按 `(revision, id)` 稳定排序，当前状态正确为 rev3。
* **outbox 轮询与发布标记**：拉取待发布、标记已发布后再次拉取为空。

## 事务与并发保证

| 关注点 | 实现方式 |
| --- | --- |
| 同 revision 幂等 | DB 唯一约束 `(source, external_id, revision)` + `ON CONFLICT DO NOTHING`，冲突时回读原事件 |
| 事件与通知原子性 | 事件 INSERT 与 outbox INSERT 在同一事务；失败 hook 验证回滚 |
| 低修订号不回退状态 | `is_late` 标记 + `Replay` 中 `if ev.Revision < currentRevision { continue }` |
| 稳定排序 | `revision ASC, id ASC`（DB）+ `revision, received_at, id`（领域重放 tie-breaker） |
| 并发提交 | Read Committed 下依赖唯一约束串行化冲突；并发结果为 1 applied + N duplicate |

## 目录索引

* 领域重放逻辑：[internal/domain/models.go](internal/domain/models.go)
* 事务写入/outbox：[internal/repository/postgres.go](internal/repository/postgres.go)
* 迁移：[migrations/000001_init_schema.up.sql](migrations/000001_init_schema.up.sql)
* HTTP 路由：[internal/http/handler.go](internal/http/handler.go)
* 集成测试：[test/integration_test.go](test/integration_test.go)
