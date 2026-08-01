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
| `warning_outbox`  | One notification row per appended event (`UNIQUE (event_id)`).       |

Guarantees and how they are enforced:

- **Idempotency** — `(source, external_id, revision)` is unique. A repeated
  message hits `ON CONFLICT DO NOTHING`, appends nothing, and returns
  `outcome: "duplicate"`.
- **No rollback on late/lower revisions** — the projection only advances when
  the incoming revision is strictly greater than the projected one
  (`domain.ShouldAdvance`). A genuinely-new lower revision is still appended for
  the audit trail (留痕) with `outcome: "superseded"`, but `warning_current` is
  untouched.
- **Event + notification atomicity** — the event insert, the projection upsert,
  and the outbox insert share one transaction. Either all commit or none do.
- **Concurrency safety** — each warning is serialized with
  `pg_advisory_xact_lock(hashtext(source), hashtext(external_id))`, so the
  read-modify-write of the projection is race-free and two concurrent identical
  revisions collapse to exactly one stored event.
- **Stable ordering** — history is ordered by `id ASC`; search is ordered by
  `(source, external_id) ASC`. Both are deterministic regardless of ingest
  timing.

Layers are kept separate:

```
cmd/api, cmd/migrate, cmd/seed     entrypoints
internal/httpapi                   HTTP/JSON transport (no business rules)
internal/store                     SQL + transactional ingest (the write path)
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
go test -race -run 'Concurrent|Fault' ./internal/store/
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
