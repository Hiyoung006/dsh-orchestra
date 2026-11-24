-- 多 Agent 工作空间隔离：任务级配置、Git worktree/临时目录映射、合并与删除审计。
-- 只新增结构与列，不改动既有数据语义。

ALTER TABLE tasks ADD COLUMN workspace_mode TEXT NOT NULL DEFAULT 'none'
  CHECK (workspace_mode IN ('none', 'readonly', 'readwrite'));
ALTER TABLE tasks ADD COLUMN workspace_source TEXT;
ALTER TABLE tasks ADD COLUMN workspace_root TEXT;
ALTER TABLE tasks ADD COLUMN workspace_branch TEXT;

CREATE INDEX IF NOT EXISTS tasks_workspace_idx ON tasks(workspace_mode, workspace_source);

CREATE TABLE IF NOT EXISTS task_workspaces (
  workspace_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  run_id TEXT,
  node_run_id TEXT,
  attempt_id TEXT,
  kind TEXT NOT NULL CHECK (kind IN ('git_worktree', 'temp_dir', 'shared')),
  mode TEXT NOT NULL CHECK (mode IN ('readonly', 'readwrite')),
  source_root TEXT NOT NULL,
  real_path TEXT NOT NULL,
  allowed_root TEXT NOT NULL,
  allowed_roots TEXT,
  repo_root TEXT,
  worktree_path TEXT,
  branch TEXT,
  base_branch TEXT,
  base_commit TEXT,
  manifest_path TEXT,
  status TEXT NOT NULL DEFAULT 'creating'
    CHECK (status IN ('creating', 'ready', 'review', 'merged', 'merge_conflict', 'abandoned', 'removed', 'failed')),
  cleanup_policy TEXT NOT NULL DEFAULT 'keep' CHECK (cleanup_policy IN ('keep', 'remove_after_merge')),
  merged_at TEXT,
  merge_commit TEXT,
  merge_target TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS task_workspaces_task_path_idx ON task_workspaces(task_id, real_path);
CREATE INDEX IF NOT EXISTS task_workspaces_task_idx ON task_workspaces(task_id, created_at);
CREATE INDEX IF NOT EXISTS task_workspaces_branch_idx ON task_workspaces(repo_root, branch);
CREATE INDEX IF NOT EXISTS task_workspaces_status_idx ON task_workspaces(status, updated_at);

CREATE TABLE IF NOT EXISTS workspace_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workspace_id TEXT NOT NULL,
  task_id TEXT,
  kind TEXT NOT NULL,
  detail TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS workspace_events_workspace_idx ON workspace_events(workspace_id, id);
