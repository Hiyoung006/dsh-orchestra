-- 任务级生命周期（13 态）与执行尝试历史。
-- 只新增列/表/触发器，不改 001-068 的任何既有结构与数据。

ALTER TABLE tasks ADD COLUMN lifecycle_state TEXT NOT NULL DEFAULT 'draft'
  CHECK (lifecycle_state IN ('draft','blocked','queued','dispatching','running','review','retrying','passed','rejected','failed','timeout','cancelled','interrupted'));
ALTER TABLE tasks ADD COLUMN lifecycle_reason TEXT;
ALTER TABLE tasks ADD COLUMN lifecycle_updated_at TEXT;
ALTER TABLE tasks ADD COLUMN retry_limit INTEGER NOT NULL DEFAULT 0 CHECK (retry_limit >= 0);
ALTER TABLE tasks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0);
ALTER TABLE tasks ADD COLUMN next_retry_at TEXT;
ALTER TABLE tasks ADD COLUMN last_attempt_id TEXT;
ALTER TABLE tasks ADD COLUMN cancel_requested_at TEXT;

CREATE INDEX IF NOT EXISTS tasks_lifecycle_idx ON tasks(lifecycle_state, lifecycle_updated_at);
CREATE INDEX IF NOT EXISTS tasks_next_retry_idx ON tasks(next_retry_at) WHERE next_retry_at IS NOT NULL;

-- 执行尝试：attempt_id 与 AgentLifecycle 使用的 nodeRunId:attempt 完全一致。
CREATE TABLE IF NOT EXISTS task_attempts (
  attempt_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  run_id TEXT,
  node_run_id TEXT,
  session_id TEXT,
  attempt_no INTEGER NOT NULL,
  status TEXT NOT NULL,
  dispatched_at TEXT,
  completed_at TEXT,
  outcome TEXT,
  error_message TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  model_ref TEXT,
  model_provider TEXT,
  model_name TEXT,
  reasoning_effort TEXT,
  parent_attempt_id TEXT,
  dispatch_spec_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS task_attempts_task_idx ON task_attempts(task_id, created_at);
CREATE INDEX IF NOT EXISTS task_attempts_run_idx ON task_attempts(run_id);
CREATE INDEX IF NOT EXISTS task_attempts_status_idx ON task_attempts(status, updated_at);

-- 生命周期审计：每次状态迁移一行，供 UI 与排障使用。
CREATE TABLE IF NOT EXISTS task_lifecycle_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT,
  attempt_id TEXT,
  actor TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS task_lifecycle_events_task_idx ON task_lifecycle_events(task_id, id);

-- ---------------------------------------------------------------------------
-- 旧数据回填：只做推导，不删除任何原始字段。
-- ---------------------------------------------------------------------------
UPDATE tasks
SET lifecycle_state = CASE
  WHEN status = 'reserved' THEN 'draft'
  WHEN status IN ('succeeded', 'succeeded_with_unverified') THEN 'passed'
  WHEN status IN ('failed', 'failed_with_unverified') THEN 'failed'
  WHEN status = 'cancelled' THEN 'cancelled'
  WHEN status = 'orphaned' THEN 'interrupted'
  WHEN status = 'timeout' THEN 'timeout'
  WHEN status IN ('queued', 'settling', 'waiting') THEN 'queued'
  WHEN status = 'running' THEN COALESCE((
    SELECT CASE r.status
      WHEN 'succeeded' THEN 'passed'
      WHEN 'succeeded_with_unverified' THEN 'passed'
      WHEN 'failed' THEN 'failed'
      WHEN 'failed_with_unverified' THEN 'failed'
      WHEN 'cancelled' THEN 'cancelled'
      WHEN 'orphaned' THEN 'interrupted'
      WHEN 'timeout' THEN 'timeout'
      ELSE NULL
    END
    FROM runs r WHERE r.run_id = tasks.current_run_id
  ), 'running')
  ELSE 'draft'
END,
lifecycle_updated_at = updated_at;

INSERT OR IGNORE INTO task_attempts
  (attempt_id, task_id, run_id, node_run_id, session_id, attempt_no, status,
   dispatched_at, completed_at, outcome, error_message, retry_count,
   model_ref, model_provider, model_name, reasoning_effort,
   parent_attempt_id, created_at, updated_at)
SELECT
  n.node_run_id || ':' || n.attempt,
  r.task_id,
  n.run_id,
  n.node_run_id,
  n.executor_session_id,
  n.attempt,
  CASE
    WHEN n.status IN ('queued', 'leased') THEN 'queued'
    WHEN n.status = 'dispatched' THEN 'dispatching'
    WHEN n.status IN ('started', 'waiting', 'verifying', 'stopping', 'timing_out', 'startup_unknown', 'indeterminate_side_effect', 'paused_bridge_missing') THEN 'running'
    WHEN n.status = 'blocked_pre' THEN 'blocked'
    WHEN n.status = 'blocked_post' THEN 'review'
    WHEN n.status = 'done' THEN 'passed'
    WHEN n.status = 'timeout' THEN 'timeout'
    WHEN n.status IN ('failed', 'failed_with_unverified') THEN 'failed'
    WHEN n.status IN ('abandoned_unverified', 'orphaned') THEN 'interrupted'
    WHEN n.status = 'superseded' THEN 'cancelled'
    ELSE 'queued'
  END,
  COALESCE(n.started_at, n.created_at),
  CASE
    WHEN n.status IN ('done', 'failed', 'timeout', 'abandoned_unverified', 'failed_with_unverified', 'orphaned', 'superseded')
      THEN n.updated_at
    ELSE NULL
  END,
  CASE
    WHEN n.status = 'done' THEN 'passed'
    WHEN n.status IN ('failed', 'failed_with_unverified') THEN 'failed'
    WHEN n.status = 'timeout' THEN 'timeout'
    WHEN n.status IN ('abandoned_unverified', 'orphaned') THEN 'interrupted'
    WHEN n.status = 'superseded' THEN 'cancelled'
    ELSE NULL
  END,
  CASE
    WHEN n.status IN ('failed', 'failed_with_unverified', 'timeout', 'abandoned_unverified', 'orphaned')
      THEN COALESCE(NULLIF(n.hang_reason, ''), NULLIF(n.cancel_result, ''), n.recovery_uncertain)
    ELSE NULL
  END,
  MAX(n.attempt - 1, COALESCE((SELECT t.retry_count FROM tasks t WHERE t.task_id = r.task_id), 0)),
  (SELECT json_extract(e.payload, '$.model_ref') FROM node_run_events e
    WHERE e.node_run_id = n.node_run_id AND e.type IN ('executor.model_selected', 'executor.model_confirmed', 'executor.model_used')
      AND json_valid(e.payload) AND json_extract(e.payload, '$.attempt') = n.attempt
    ORDER BY e.seq DESC LIMIT 1),
  (SELECT COALESCE(json_extract(e.payload, '$.usage.provider'), json_extract(e.payload, '$.provider')) FROM node_run_events e
    WHERE e.node_run_id = n.node_run_id AND e.type IN ('executor.model_selected', 'executor.model_confirmed', 'executor.model_used')
      AND json_valid(e.payload) AND json_extract(e.payload, '$.attempt') = n.attempt
    ORDER BY e.seq DESC LIMIT 1),
  (SELECT COALESCE(json_extract(e.payload, '$.usage.model'), json_extract(e.payload, '$.model')) FROM node_run_events e
    WHERE e.node_run_id = n.node_run_id AND e.type IN ('executor.model_selected', 'executor.model_confirmed', 'executor.model_used')
      AND json_valid(e.payload) AND json_extract(e.payload, '$.attempt') = n.attempt
    ORDER BY e.seq DESC LIMIT 1),
  (SELECT json_extract(e.payload, '$.reasoning_effort') FROM node_run_events e
    WHERE e.node_run_id = n.node_run_id AND e.type IN ('executor.model_selected', 'executor.model_confirmed', 'executor.model_used')
      AND json_valid(e.payload) AND json_extract(e.payload, '$.attempt') = n.attempt
    ORDER BY e.seq DESC LIMIT 1),
  NULL,
  n.created_at,
  n.updated_at
FROM node_runs n JOIN runs r ON r.run_id = n.run_id;

-- ---------------------------------------------------------------------------
-- 触发器：任何派发/状态变化都自动落到 attempt 历史。
-- ---------------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS task_attempt_after_node_run_insert
AFTER INSERT ON node_runs
BEGIN
  INSERT OR IGNORE INTO task_attempts
    (attempt_id, task_id, run_id, node_run_id, session_id, attempt_no, status,
     dispatched_at, retry_count, created_at, updated_at)
  SELECT
    NEW.node_run_id || ':' || NEW.attempt,
    r.task_id,
    NEW.run_id,
    NEW.node_run_id,
    NEW.executor_session_id,
    NEW.attempt,
    CASE
      WHEN NEW.status IN ('queued', 'leased') THEN 'queued'
      WHEN NEW.status = 'dispatched' THEN 'dispatching'
      WHEN NEW.status IN ('started', 'waiting', 'verifying', 'stopping', 'timing_out', 'startup_unknown', 'indeterminate_side_effect', 'paused_bridge_missing') THEN 'running'
      WHEN NEW.status = 'blocked_pre' THEN 'blocked'
      WHEN NEW.status = 'blocked_post' THEN 'review'
      WHEN NEW.status = 'done' THEN 'passed'
      WHEN NEW.status = 'timeout' THEN 'timeout'
      WHEN NEW.status IN ('failed', 'failed_with_unverified') THEN 'failed'
      WHEN NEW.status IN ('abandoned_unverified', 'orphaned') THEN 'interrupted'
      WHEN NEW.status = 'superseded' THEN 'cancelled'
      ELSE 'queued'
    END,
    NEW.started_at,
    MAX(NEW.attempt - 1, COALESCE((SELECT retry_count FROM tasks WHERE task_id = r.task_id), 0)),
    NEW.created_at,
    NEW.updated_at
  FROM runs r WHERE r.run_id = NEW.run_id;
  UPDATE tasks
  SET last_attempt_id = NEW.node_run_id || ':' || NEW.attempt,
      lifecycle_updated_at = COALESCE(lifecycle_updated_at, NEW.created_at)
  WHERE current_run_id = NEW.run_id
    AND task_id = (SELECT task_id FROM runs WHERE run_id = NEW.run_id);
END;

CREATE TRIGGER IF NOT EXISTS task_attempt_after_node_run_update
AFTER UPDATE OF status, executor_session_id, started_at, hang_reason, cancel_result, recovery_uncertain ON node_runs
BEGIN
  UPDATE task_attempts
  SET session_id = COALESCE(NEW.executor_session_id, session_id),
      status = CASE
        WHEN NEW.status IN ('queued', 'leased') THEN 'queued'
        WHEN NEW.status = 'dispatched' THEN 'dispatching'
        WHEN NEW.status IN ('started', 'waiting', 'verifying', 'stopping', 'timing_out', 'startup_unknown', 'indeterminate_side_effect', 'paused_bridge_missing') THEN 'running'
        WHEN NEW.status = 'blocked_pre' THEN 'blocked'
        WHEN NEW.status = 'blocked_post' THEN 'review'
        WHEN NEW.status = 'done' THEN 'passed'
        WHEN NEW.status = 'timeout' THEN 'timeout'
        WHEN NEW.status IN ('failed', 'failed_with_unverified') THEN 'failed'
        WHEN NEW.status IN ('abandoned_unverified', 'orphaned') THEN 'interrupted'
        WHEN NEW.status = 'superseded' THEN 'cancelled'
        ELSE status
      END,
      dispatched_at = COALESCE(dispatched_at, NEW.started_at),
      completed_at = CASE
        WHEN NEW.status IN ('done', 'failed', 'timeout', 'abandoned_unverified', 'failed_with_unverified', 'orphaned', 'superseded')
          THEN COALESCE(completed_at, NEW.updated_at)
        ELSE completed_at
      END,
      outcome = CASE
        WHEN NEW.status = 'done' THEN 'passed'
        WHEN NEW.status IN ('failed', 'failed_with_unverified') THEN 'failed'
        WHEN NEW.status = 'timeout' THEN 'timeout'
        WHEN NEW.status IN ('abandoned_unverified', 'orphaned') THEN 'interrupted'
        WHEN NEW.status = 'superseded' THEN 'cancelled'
        ELSE outcome
      END,
      error_message = CASE
        WHEN NEW.status IN ('failed', 'failed_with_unverified', 'timeout', 'abandoned_unverified', 'orphaned')
          THEN COALESCE(error_message, NULLIF(NEW.hang_reason, ''), NULLIF(NEW.cancel_result, ''), NEW.recovery_uncertain)
        ELSE error_message
      END,
      updated_at = NEW.updated_at
  WHERE attempt_id = NEW.node_run_id || ':' || NEW.attempt
    AND (completed_at IS NULL OR NEW.status IN ('done', 'failed', 'timeout', 'abandoned_unverified', 'failed_with_unverified', 'orphaned', 'superseded'));
END;

CREATE TRIGGER IF NOT EXISTS task_attempt_after_model_event
AFTER INSERT ON node_run_events
WHEN NEW.type IN ('executor.model_selected', 'executor.model_confirmed', 'executor.model_used', 'executor.model_rejected')
  AND json_valid(NEW.payload)
BEGIN
  UPDATE task_attempts
  SET model_ref = COALESCE(json_extract(NEW.payload, '$.model_ref'), model_ref),
      model_provider = COALESCE(json_extract(NEW.payload, '$.usage.provider'), json_extract(NEW.payload, '$.provider'), model_provider),
      model_name = COALESCE(json_extract(NEW.payload, '$.usage.model'), json_extract(NEW.payload, '$.model'), model_name),
      reasoning_effort = COALESCE(json_extract(NEW.payload, '$.reasoning_effort'), json_extract(NEW.payload, '$.effort'), reasoning_effort),
      updated_at = NEW.created_at
  WHERE node_run_id = NEW.node_run_id
    AND (json_extract(NEW.payload, '$.attempt') IS NULL OR attempt_no = json_extract(NEW.payload, '$.attempt'));
END;
