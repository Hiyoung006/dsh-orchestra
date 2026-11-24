-- 前端监控总览：只新增只读聚合所需索引，不改任何既有表结构与数据。
-- Token 统计只关心 executor.model_used 事件，使用部分索引避免全表扫描。
CREATE INDEX IF NOT EXISTS node_run_events_model_used_idx
  ON node_run_events(type) WHERE type = 'executor.model_used';

-- 总览按未归档任务统计生命周期；部分索引缩小扫描范围。
CREATE INDEX IF NOT EXISTS tasks_active_lifecycle_idx
  ON tasks(lifecycle_state) WHERE archived_at IS NULL;
