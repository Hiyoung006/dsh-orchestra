-- 任务验收：独立验收状态、冻结标准、AI 预审意见与人工最终审批分离。
-- 只新增列/表/触发器，不改 001-070 的既有结构与数据。

ALTER TABLE tasks ADD COLUMN acceptance_mode TEXT NOT NULL DEFAULT 'assisted'
  CHECK (acceptance_mode IN ('manual','assisted','automatic'));
ALTER TABLE tasks ADD COLUMN acceptance_status TEXT NOT NULL DEFAULT 'pending'
  CHECK (acceptance_status IN ('pending','reviewing','ai_reviewed','approved','rejected','not_applicable'));
ALTER TABLE tasks ADD COLUMN acceptance_criteria_version INTEGER;
ALTER TABLE tasks ADD COLUMN acceptance_criteria_hash TEXT;
ALTER TABLE tasks ADD COLUMN acceptance_reviewed_at TEXT;
ALTER TABLE tasks ADD COLUMN acceptance_approved_by TEXT;
ALTER TABLE tasks ADD COLUMN acceptance_approved_at TEXT;
ALTER TABLE tasks ADD COLUMN acceptance_reason TEXT;
ALTER TABLE tasks ADD COLUMN acceptance_remediation_json TEXT;

CREATE INDEX IF NOT EXISTS tasks_acceptance_idx ON tasks(acceptance_status, acceptance_mode);

-- 验收标准：每次冻结一个不可变版本；内容变化只能追加新版本。
CREATE TABLE IF NOT EXISTS acceptance_criteria (
  criteria_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  plan_id TEXT,
  content_hash TEXT,
  version INTEGER NOT NULL CHECK (version >= 1),
  criteria_hash TEXT NOT NULL,
  criteria_json TEXT NOT NULL,
  machine_checkable INTEGER NOT NULL DEFAULT 0 CHECK (machine_checkable IN (0,1)),
  source TEXT NOT NULL DEFAULT 'plan',
  frozen_at TEXT NOT NULL,
  frozen_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(task_id, version)
);

CREATE INDEX IF NOT EXISTS acceptance_criteria_task_idx ON acceptance_criteria(task_id, version DESC);

-- 已冻结标准禁止原地修改；新增版本必须 INSERT。
CREATE TRIGGER IF NOT EXISTS acceptance_criteria_frozen_update
BEFORE UPDATE ON acceptance_criteria
BEGIN
  SELECT RAISE(ABORT, '验收标准已冻结，不能修改');
END;

-- AI / 系统预审意见，仅作建议，不产生审批效力。
CREATE TABLE IF NOT EXISTS acceptance_reviews (
  review_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  run_id TEXT,
  criteria_version INTEGER,
  mode TEXT NOT NULL CHECK (mode IN ('manual','assisted','automatic')),
  reviewer_kind TEXT NOT NULL CHECK (reviewer_kind IN ('ai','system')),
  reviewer TEXT NOT NULL,
  verdict TEXT NOT NULL CHECK (verdict IN ('pass','fail','needs_human')),
  summary TEXT NOT NULL,
  items_json TEXT NOT NULL,
  remediation_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS acceptance_reviews_task_idx ON acceptance_reviews(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS acceptance_reviews_run_idx ON acceptance_reviews(run_id);

-- 最终审批：人工或 automatic 条件下的系统自动通过；与预审意见分开。
CREATE TABLE IF NOT EXISTS acceptance_approvals (
  approval_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  run_id TEXT,
  criteria_version INTEGER,
  review_id TEXT,
  decision TEXT NOT NULL CHECK (decision IN ('approved','rejected')),
  approver TEXT NOT NULL,
  approver_kind TEXT NOT NULL CHECK (approver_kind IN ('human','system_auto')),
  reason TEXT,
  items_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS acceptance_approvals_task_idx ON acceptance_approvals(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS acceptance_approvals_run_idx ON acceptance_approvals(run_id);

-- 验收历史：追加写，不覆盖。
CREATE TABLE IF NOT EXISTS acceptance_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  run_id TEXT,
  kind TEXT NOT NULL,
  actor TEXT,
  actor_kind TEXT,
  detail_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS acceptance_events_task_idx ON acceptance_events(task_id, id);

-- force 派发/重试的用户授权记录。
CREATE TABLE IF NOT EXISTS dispatch_authorizations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  run_id TEXT,
  operation_id TEXT,
  kind TEXT NOT NULL CHECK (kind IN ('start','retry')),
  authorized_by TEXT NOT NULL,
  reason TEXT NOT NULL,
  detail_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS dispatch_authorizations_task_idx ON dispatch_authorizations(task_id, id);

-- 历史已通过任务保持通过，不被默认 assisted 回退。
UPDATE tasks
SET acceptance_status = 'approved',
    acceptance_approved_by = 'system:migration',
    acceptance_approved_at = COALESCE(updated_at, lifecycle_updated_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    acceptance_reason = 'migration: historical passed task'
WHERE lifecycle_state = 'passed';
