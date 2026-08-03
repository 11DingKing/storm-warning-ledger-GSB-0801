-- 0001_init.sql — append-only 预警事件流 + 当前状态 + outbox
--
-- 三张表职责分离：
--   warning_events  : append-only 事件流，每次修订/解除都是一行新事件，禁止 UPDATE/DELETE
--   warning_current : 每个 (source, external_id) 的当前有效状态（物化读模型）
--   warning_outbox  : 状态变更通知，与事件在同一事务写入（事务性 outbox）

CREATE TABLE warning_events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source       TEXT        NOT NULL,
    external_id  TEXT        NOT NULL,
    revision     INTEGER     NOT NULL CHECK (revision >= 1),
    severity     TEXT        NOT NULL CHECK (severity IN ('blue', 'yellow', 'orange', 'red')),
    status       TEXT        NOT NULL CHECK (status IN ('active', 'cancelled')),
    region_code  TEXT        NOT NULL,
    headline     TEXT        NOT NULL DEFAULT '',
    description  TEXT        NOT NULL DEFAULT '',
    issued_at    TIMESTAMPTZ NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    -- 规范化内容的摘要，用于区分“同一修订的重复消息”与“同号不同内容的冲突修订”
    fingerprint  TEXT        NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL,
    -- 幂等键：同一 (source, external_id, revision) 全库只能有一行
    CONSTRAINT warning_events_revision_uq UNIQUE (source, external_id, revision)
);

-- 事件流稳定排序依据：按自增 id 即接收顺序
CREATE INDEX warning_events_series_idx ON warning_events (source, external_id, id);
CREATE INDEX warning_events_received_idx ON warning_events (received_at);

CREATE TABLE warning_current (
    source       TEXT        NOT NULL,
    external_id  TEXT        NOT NULL,
    revision     INTEGER     NOT NULL,
    severity     TEXT        NOT NULL,
    status       TEXT        NOT NULL,
    region_code  TEXT        NOT NULL,
    headline     TEXT        NOT NULL DEFAULT '',
    description  TEXT        NOT NULL DEFAULT '',
    issued_at    TIMESTAMPTZ NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    last_event_id BIGINT     NOT NULL REFERENCES warning_events (id),
    updated_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source, external_id)
);

CREATE INDEX warning_current_status_idx ON warning_current (status);
CREATE INDEX warning_current_region_idx ON warning_current (region_code);
CREATE INDEX warning_current_severity_idx ON warning_current (severity);

CREATE TABLE warning_outbox (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id    BIGINT      NOT NULL REFERENCES warning_events (id),
    source      TEXT        NOT NULL,
    external_id TEXT        NOT NULL,
    revision    INTEGER     NOT NULL,
    -- applied: 事件成为了当前有效状态（含解除）；late 事件与幂等重放不产生通知
    kind        TEXT        NOT NULL CHECK (kind IN ('applied')),
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    dispatched_at TIMESTAMPTZ
);

CREATE INDEX warning_outbox_pending_idx ON warning_outbox (id) WHERE dispatched_at IS NULL;

-- append-only 强制：事件流禁止 UPDATE / DELETE
CREATE OR REPLACE FUNCTION warning_events_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'warning_events is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'raise_exception';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER warning_events_append_only
    BEFORE UPDATE OR DELETE ON warning_events
    FOR EACH ROW EXECUTE FUNCTION warning_events_reject_mutation();

-- 迁移版本表由内置 migrator 维护（migrator 也会先建一次，IF NOT EXISTS 保证幂等）
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
