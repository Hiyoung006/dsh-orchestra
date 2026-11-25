-- 成本控制：预算预留、在途费用、阈值告警与超额中断策略。
-- 只新增列与表，不改变既有费用事件和调度语义。

ALTER TABLE scheduler_settings ADD COLUMN budget_warn_percent INTEGER NOT NULL DEFAULT 80
  CHECK (budget_warn_percent BETWEEN 1 AND 100);
ALTER TABLE scheduler_settings ADD COLUMN budget_interrupt_policy TEXT NOT NULL DEFAULT 'block_new'
  CHECK (budget_interrupt_policy IN ('block_new','cancel_running'));

ALTER TABLE scheduler_notifications ADD COLUMN severity TEXT NOT NULL DEFAULT 'warning'
  CHECK (severity IN ('info','warning','critical'));
ALTER TABLE scheduler_notifications ADD COLUMN metadata_json TEXT;

CREATE TABLE IF NOT EXISTS scheduler_budget_reservations (
  reservation_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL UNIQUE REFERENCES scheduler_jobs(job_id) ON DELETE CASCADE,
  task_id TEXT NOT NULL,
  amount_micros INTEGER NOT NULL CHECK (amount_micros >= 0),
  currency TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('held','settled','released')),
  settled_micros INTEGER NOT NULL DEFAULT 0 CHECK (settled_micros >= 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  settled_at TEXT
);

CREATE INDEX IF NOT EXISTS scheduler_budget_reservations_task_idx
  ON scheduler_budget_reservations(task_id, status);
CREATE INDEX IF NOT EXISTS scheduler_budget_reservations_status_idx
  ON scheduler_budget_reservations(status);
