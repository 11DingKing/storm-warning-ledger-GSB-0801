# Storm Warning Lifecycle Ledger

Backend for unified ingestion of multi-province emergency storm warnings
(暴雨 rainstorm, 雷暴大风 thunderstorm-gale, 冰雹 hail). Upstream feeds are noisy:
messages arrive **duplicated, out of order, and even carry stale revisions for
events that have already been cancelled**. This service keeps the lifecycle
correct anyway.

Built with **Go 1.24** and **PostgreSQL 16**.

## Design in one screen

The write model is an **append-only event stream**. Revisions and cancellations
only ever *append* a new row — nothing is overwritten or deleted.

Three tables, all touched inside a single transaction on every accepted write:

| Table             | Role                                                                 |
|-------------------|----------------------------------------------------------------------|
| `warning_events`  | Append-only log. `UNIQUE (source, external_id, revision)`.           |
| `warning_current` | Derived projection of the *current effective state* per warning.     |
| `warning_outbox`  | Deliverable/retryable notification queue. `UNIQUE (event_id)` and `UNIQUE (notification_id)`. |

Guarantees and how they are enforced:

- **Idempotency** — `(source, external_id, revision)` is unique. A repeated
  message hits `ON CONFLICT DO NOTHING`, appends nothing, and returns
  `outcome: "duplicate"` **together with the first event and first outbox
  record** (same ids, same notification identity).
- **No rollback on late/lower revisions** — the projection only advances when
  the incoming revision is strictly greater than the projected one
  (`domain.ShouldAdvance`). A genuinely-new lower revision is still appended for
  the audit trail (留痕) with `outcome: "superseded"`, but `warning_current` is
  untouched. Re-activating after a cancellation (e.g. revision 4 `active` after a
  revision 3 `cancelled`) is just another appended event — the cancellation row
  is never rewritten.
- **Event + notification atomicity** — the event insert, the projection upsert,
  and the outbox insert share one transaction. Either all commit or none do.
- **Concurrency safety** — each warning is serialized with
  `pg_advisory_xact_lock(hashtext(source), hashtext(external_id))`, so the
  read-modify-write of the projection is race-free and two concurrent identical
  revisions collapse to exactly one stored event.
- **Stable ordering** — history is ordered by `id ASC`; search is ordered by
  `(source, external_id) ASC`. Both are deterministic regardless of ingest
  timing.

### Outbox delivery (at-least-once, crash-safe)

The outbox is drained by one or more **workers** (`cmd/worker`). Each notification
carries a **stable identity** `notification_id = "<source>/<external_id>/<revision>"`
(e.g. `cn-met/rainstorm-2026-0801-hb-001/4`) that never changes across redelivery.

- **No double-claim** — a worker claims a due row with
  `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`, holding the row lock for the whole
  processing transaction. A row in flight in one worker is invisible to another,
  so two workers can never claim the same row simultaneously.
- **Deliver-before-mark** — the worker delivers to the downstream *first*, then
  writes `dispatched_at` in the same transaction. If the process crashes after
  the downstream received the message but before that local commit, the
  transaction rolls back: `dispatched_at` stays `NULL` and the row is redelivered
  later **with the same `notification_id`**. Downstreams treat that id as an
  idempotency key (sent as the `Idempotency-Key` header).
- **Retry with backoff** — a failed delivery records `last_error`, increments
  `attempts`, and pushes `next_attempt_at` out by an exponential, capped backoff.

Layers are kept separate:

```
cmd/api, cmd/migrate, cmd/seed, cmd/worker   entrypoints
internal/httpapi                   HTTP/JSON transport (no business rules)
internal/store                     SQL + transactional ingest (the write path)
internal/dispatch                  outbox claim/deliver/retry worker
internal/domain                    pure types, validation, projection rules
internal/migrate                   embedded-SQL migration runner
migrations/                        ordered *.up.sql / *.down.sql
```

## Prerequisites

- Go 1.24+ (the repo pins `go 1.24.0`; with `GOTOOLCHAIN=auto` the toolchain is
  fetched automatically).
- A running PostgreSQL 16 server and two databases (app + tests).

```bash
# Example: a local PostgreSQL 16 on port 55432 with a `postgres` superuser.
createdb -h localhost -p 55432 -U postgres storm
createdb -h localhost -p 55432 -U postgres storm_test

export DATABASE_URL="postgres://postgres@localhost:55432/storm?sslmode=disable"
export TEST_DATABASE_URL="postgres://postgres@localhost:55432/storm_test?sslmode=disable"
```

## Run migrations

```bash
go run ./cmd/migrate
```

Migrations are embedded and tracked in `schema_migrations`; re-running is a
no-op.

## Run the tests

```bash
# Unit tests always run. Integration/concurrency tests run only when
# TEST_DATABASE_URL points at a real PostgreSQL 16 (otherwise they t.Skip).
go test ./...

# The concurrency and atomicity proofs, under the race detector:
go test -race -run 'Concurrent|Fault|Crash' ./internal/store/ ./internal/dispatch/
```

What the integration tests prove against real PostgreSQL:

| Test                                        | Proves                                             |
|---------------------------------------------|----------------------------------------------------|
| `TestScenarioThroughStore`                  | The 5-message scenario end to end.                 |
| `TestLateLowerRevisionSupersededNoRollback` | New lower revision is appended but never rolls back current state. |
| `TestConcurrentIdenticalRevision`           | 16 concurrent identical revisions → exactly 1 event + 1 outbox row. |
| `TestFaultBetweenEventAndOutboxThenRetry`   | Fault after event insert / before outbox rolls everything back; retry succeeds once. |
| `TestAsOfHistoryVsCurrent`                  | `as_of` sees the historical revision while `current` sees the latest. |
| `TestSearchStableOrdering`                  | Search returns a deterministic order across repeated calls. |
| `TestFullScenarioOverHTTP`                  | The whole scenario driven through the JSON API.    |
| `TestRevision4ReactivatesWithoutRewritingCancellation` | Appending revision 4 (red/active) advances current but leaves the revision-3 cancellation row intact. |
| `TestDuplicateRevision4ReturnsFirst`        | Re-submitting revision 4 returns the first event + first outbox record; nothing new appended. |
| `TestConcurrentWorkersNoDoubleClaim`        | Two workers drain the queue with no row claimed twice; each delivered exactly once. |
| `TestCrashAfterReceiptRedeliversSameIdentity` | Crash after downstream receipt / before `dispatched_at` commit → redelivery with the same notification identity. |

## Run the dispatch workers

```bash
export DATABASE_URL="postgres://postgres@localhost:55432/storm?sslmode=disable"

# Start two workers concurrently; each claims disjoint rows (FOR UPDATE SKIP LOCKED).
# DOWNSTREAM_URL is optional — if unset, deliveries are logged.
WORKER_ID=w1 DOWNSTREAM_URL=http://localhost:9000/notify go run ./cmd/worker &
WORKER_ID=w2 DOWNSTREAM_URL=http://localhost:9000/notify go run ./cmd/worker &
```

Each delivery sends the stable `notification_id` as an `Idempotency-Key` header,
so the downstream can safely de-duplicate the redelivery that follows a crash.

## Start the API and push the demo data through it

```bash
export DATABASE_URL="postgres://postgres@localhost:55432/storm?sslmode=disable"
export HTTP_ADDR=":8080"
go run ./cmd/api            # in one terminal

# In another terminal — drives the 5 canonical messages through the HTTP API:
API_BASE=http://localhost:8080 go run ./cmd/seed
```

The seed scenario (external event `GD-RAIN-2026-0007`) is exactly:

1. revision 1 — initial active → `applied`
2. revision 2 — upgraded to severe → `applied`
3. revision 1 arriving **late / out of order** → `duplicate` (current stays at rev 2)
4. revision 1 **duplicate** retransmit → `duplicate`
5. revision 3 — **cancellation / 解除** → `applied`, current becomes rev 3 cancelled

## HTTP API

Full contract in [`openapi.yaml`](./openapi.yaml).

| Method & path                                   | Purpose                                  |
|-------------------------------------------------|------------------------------------------|
| `POST /v1/warnings`                             | Ingest one message (append-only, idempotent). |
| `GET  /v1/warnings/{source}/{external_id}`      | Current effective state.                 |
| `GET  /v1/warnings/{source}/{external_id}?as_of=<RFC3339>` | Point-in-time state.           |
| `GET  /v1/warnings/{source}/{external_id}/events` | Full append-only history.              |
| `GET  /v1/warnings?status=&severity=&region_code=&limit=&offset=` | Search current states. |

Example ingest:

```bash
curl -sS -X POST http://localhost:8080/v1/warnings \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "cma-guangdong",
    "external_id": "GD-RAIN-2026-0007",
    "revision": 1,
    "severity": "moderate",
    "status": "active",
    "issued_at": "2026-08-01T08:00:00Z",
    "effective_at": "2026-08-01T08:00:00Z",
    "expires_at": "2026-08-01T14:00:00Z",
    "region_codes": ["440100", "440300"],
    "payload": {"headline": "暴雨黄色预警"}
  }'
```

Ingest response `outcome` is one of:

- `applied` — appended and advanced the current state.
- `superseded` — appended for the audit trail, current state unchanged.
- `duplicate` — nothing appended; idempotent no-op.

## Notes

- `received_at` is the server clock at append time and is what `as_of` filters
  on, so the point-in-time view reflects *when we learned* a fact, not when it
  was issued upstream.
- Rejected duplicate inserts still consume identity values, so `warning_events.id`
  is monotonic but may contain gaps — this is expected and does not affect
  ordering guarantees.
- Docker is intentionally out of scope; everything runs natively.
