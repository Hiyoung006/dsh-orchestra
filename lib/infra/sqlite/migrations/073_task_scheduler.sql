-- 任务调度器：队列、优先级、并发、超时、重试、暂停/恢复、预算与模型档位。
-- 只新增列/表，不改既有任务与运行语义。

ALTER TABLE tasks ADD COLUMN scheduler_priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN scheduler_model_class TEXT
  CHECK (scheduler_model_class IS NULL OR scheduler_model_class IN ('simple','code','audit'));
ALTER TABLE tasks ADD COLUMN scheduler_model_failure_policy TEXT NOT NULL DEFAULT 'retry'
  CHECK (scheduler_model_failure_policy IN ('retry','downgrade'));
ALTER TABLE tasks ADD COLUMN scheduler_fallback_model_class TEXT
  CHECK (scheduler_fallback_model_class IS NULL OR scheduler_fallback_model_class IN ('simple','code','audit'));
ALTER TABLE tasks ADD COLUMN scheduler_budget_micros INTEGER
  CHECK (scheduler_budget_micros IS NULL OR scheduler_budget_micros >= 0);

CREATE TABLE IF NOT EXISTS scheduler_settings (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  max_concurrency INTEGER NOT NULL DEFAULT 1 CHECK (max_concurrency BETWEEN 1 AND 128),
  paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0,1)),
  global_budget_micros INTEGER CHECK (global_budget_micros IS NULL OR global_budget_micros >= 0),
  currency TEXT NOT NULL DEFAULT 'USD',
  default_timeout_ms INTEGER NOT NULL DEFAULT 3600000 CHECK (default_timeout_ms > 0),
  default_retry_limit INTEGER NOT NULL DEFAULT 0 CHECK (default_retry_limit BETWEEN 0 AND 100),
  updated_at TEXT NOT NULL,
  updated_by TEXT
);

INSERT OR IGNORE INTO scheduler_settings (id, max_concurrency, paused, global_budget_micros, currency, default_timeout_ms, default_retry_limit, updated_at)
VALUES (1, 1, 0, NULL, 'USD', 3600000, 0, '1970-01-01T00:00:00.000Z');

CREATE TABLE IF NOT EXISTS scheduler_jobs (
  job_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  workflow_version TEXT NOT NULL,
  node_ids_json TEXT NOT NULL,
  autonomy TEXT,
  escalations_json TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL CHECK (status IN ('queued','dispatching','running','retrying','cancelling','succeeded','failed','cancelled','budget_blocked')),
  priority INTEGER NOT NULL DEFAULT 0,
  order_seq INTEGER NOT NULL,
  enqueued_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  run_id TEXT,
  workspace_key TEXT,
  timeout_ms INTEGER NOT NULL,
  retry_limit INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  allow_auto_retry INTEGER NOT NULL DEFAULT 1 CHECK (allow_auto_retry IN (0,1)),
  model_class TEXT CHECK (model_class IS NULL OR model_class IN ('simple','code','audit')),
  model_failure_policy TEXT NOT NULL DEFAULT 'retry' CHECK (model_failure_policy IN ('retry','downgrade')),
  fallback_model_class TEXT CHECK (fallback_model_class IS NULL OR fallback_model_class IN ('simple','code','audit')),
  budget_micros INTEGER CHECK (budget_micros IS NULL OR budget_micros >= 0),
  cancel_reason TEXT,
  last_error TEXT,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS scheduler_jobs_status_idx ON scheduler_jobs(status, priority DESC, order_seq ASC);
CREATE INDEX IF NOT EXISTS scheduler_jobs_task_idx ON scheduler_jobs(task_id);
CREATE INDEX IF NOT EXISTS scheduler_jobs_run_idx ON scheduler_jobs(run_id);

CREATE TABLE IF NOT EXISTS scheduler_job_dependencies (
  job_id TEXT NOT NULL REFERENCES scheduler_jobs(job_id) ON DELETE CASCADE,
  depends_on_task_id TEXT NOT NULL,
  PRIMARY KEY (job_id, depends_on_task_id)
);

CREATE INDEX IF NOT EXISTS scheduler_job_dependencies_task_idx ON scheduler_job_dependencies(depends_on_task_id);

CREATE TABLE IF NOT EXISTS scheduler_cost_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT,
  task_id TEXT NOT NULL,
  run_id TEXT,
  amount_micros INTEGER NOT NULL CHECK (amount_micros >= 0),
  currency TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source = 'host'),
  basis TEXT NOT NULL DEFAULT 'actual' CHECK (basis = 'actual'),
  external_id TEXT,
  reported_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (source, external_id)
);

CREATE INDEX IF NOT EXISTS scheduler_cost_events_task_idx ON scheduler_cost_events(task_id, created_at);
CREATE INDEX IF NOT EXISTS scheduler_cost_events_job_idx ON scheduler_cost_events(job_id, created_at);

CREATE TABLE IF NOT EXISTS scheduler_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  task_id TEXT,
  job_id TEXT,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL,
  acknowledged_at TEXT
);

CREATE INDEX IF NOT EXISTS scheduler_notifications_created_idx ON scheduler_notifications(created_at DESC);
