-- 0002_outbox_dispatch.sql — 让 warning_outbox 成为可投递、可重试的事务性 outbox
--
-- 通知身份（notification_id）= source || '/' || external_id || '/' || revision，
-- 由三元组唯一约束保证稳定：同一逻辑通知无论重投多少次，下游看到的身份不变，
-- 因此下游可以按 notification_id 幂等去重。

-- 三元组唯一：一条 applied 事件至多对应一行通知；
-- 崩溃重放不会、也不允许产生第二行同身份通知
ALTER TABLE warning_outbox
    ADD CONSTRAINT warning_outbox_identity_uq UNIQUE (source, external_id, revision);

-- 重试簿记
ALTER TABLE warning_outbox
    ADD COLUMN attempts        INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN last_error      TEXT;

-- 认领扫描：待投递且到达重试时间的行，按 id 稳定顺序
CREATE INDEX warning_outbox_claim_idx ON warning_outbox (id)
    WHERE dispatched_at IS NULL;
