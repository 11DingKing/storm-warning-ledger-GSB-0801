-- 0003_outbox_lease.sql — 投递租约与死信（终止状态）
--
-- 租约语义：
--   认领 = 在短事务里写入 claimed_by / claimed_until / claim_token 后提交，
--   投递在事务外进行，不再长时间持有行锁；
--   worker 崩溃或卡住时租约到期（claimed_until < now()）即可被其他 worker 接管；
--   claim_token 是围栏令牌：租约被接管后旧 token 失效，
--   旧 worker 的 Complete/Fail 不会生效（ErrLeaseLost），防止过期 worker 误标。
--
-- 死信语义：
--   连续失败达到上限（dispatcher 侧默认 3 次）后写入 dead_lettered_at，
--   进入可查询的终止状态，不再被认领。

ALTER TABLE warning_outbox
    ADD COLUMN claimed_by      TEXT,
    ADD COLUMN claimed_until   TIMESTAMPTZ,
    ADD COLUMN claim_token     TEXT,
    ADD COLUMN dead_lettered_at TIMESTAMPTZ;

-- 可认领集合：未投递、未死信；租约是否过期在认领语句里判断
DROP INDEX IF EXISTS warning_outbox_claim_idx;
DROP INDEX IF EXISTS warning_outbox_pending_idx;
CREATE INDEX warning_outbox_claimable_idx ON warning_outbox (id)
    WHERE dispatched_at IS NULL AND dead_lettered_at IS NULL;

-- 死信查询
CREATE INDEX warning_outbox_dead_idx ON warning_outbox (id)
    WHERE dead_lettered_at IS NOT NULL;
