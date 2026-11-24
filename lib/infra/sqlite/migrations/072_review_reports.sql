-- 独立质量审查报告：只依据系统事实，不依赖执行 Agent 的自我描述。
-- 报告是建议，不能替代人工最终审批；审查结论与验收审批分开存储。

CREATE TABLE IF NOT EXISTS review_reports (
  review_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  run_id TEXT NOT NULL,
  criteria_version INTEGER,
  reviewer TEXT NOT NULL,
  reviewer_kind TEXT NOT NULL CHECK (reviewer_kind IN ('independent_agent','service')),
  review_status TEXT NOT NULL CHECK (review_status IN ('pass','fail','need_evidence')),
  score INTEGER NOT NULL CHECK (score BETWEEN 0 AND 100),
  report_json TEXT NOT NULL CHECK (json_valid(report_json)),
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS review_reports_task_idx ON review_reports(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS review_reports_run_idx ON review_reports(run_id, created_at DESC);
