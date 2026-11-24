# 更新日志

本文件记录 DSH Orchestra 插件包的用户可见变更。版本号与 [`package.json`](package.json) 一致。

## 0.1.0-beta.3 — 开发中（M0）

### 工程

- 发布基线改为以 `lib/` 为唯一源码，移除不可执行的 `tsdown`/`tsc` 构建脚本。
- 新增 `pnpm-lock.yaml` 与 `pnpm-workspace.yaml`，关闭 peer 自动安装，不拉取或升级 DSH 宿主依赖。
- 新增可复现打包：双次 `pnpm pack` 比对、SHA256、CycloneDX SBOM 和关联 Git commit 的 `build-info.json`。
- 新增 GitHub Actions：Type Check、Unit Test、Integration Test、Security Test、Build、Package Integrity。

### 安全

- `zod` 从 `4.4.3` 升级到 `4.6.6`，修复 GHSA-mrhx-c7xc-m3w5（超大约束集合导致内存耗尽）。
- 依赖审计固定使用官方 npm registry，避免镜像缺少审计端点。

## 0.1.0-beta.2 — 2026-11-24

### 新增

- **总览仪表盘**：编排板数量、任务总数、执行中、待验收、已通过、失败、平均执行时间、重试次数、Token 消耗与实际费用。没有宿主实际费用时显示不可用，不用估算值代替。
- **独立任务详情页**：任务基本信息、执行 Agent、模型、工作目录、Git 分支、执行历史、子会话入口、执行日志、工具调用摘要、测试结果、验收结果与错误详情。
- **任务时间线**：按时间展示创建、派发、执行、重试、失败、验收和完成事件，并保留原始状态与操作者。
- **任务管理**：搜索、按状态筛选、批量派发/取消、手动重试、依赖查看、Git Diff 和执行报告导出。
- **画布增强**：自动布局、缩放与小地图、任务分组与折叠、依赖关系高亮、失败链路高亮和实时状态刷新。
- 新增 `POST /v1/tasks/batch` 与 `GET /v1/tasks/timeline` 服务接口，并加入个人面板路由白名单。
- 新增迁移 `074_frontend_monitoring.sql`，只添加只读聚合索引。

### 修复

- 调度器启动预留任务时没有把任务标题传给 `startRun`，导致 SQLite 绑定失败、任务无法真正开跑。
- 同一任务重复入队会生成多个调度项。现在只允许一个活动调度项，重复请求返回 `STALE_GENERATION`，避免重复派发。
- 个人面板代理未放行新的批量与时间线接口，导致请求返回 404。

### 测试

- 新增 `tests/task-dependencies.test.mjs`：依赖链、依赖反查、自依赖、缺失前置、环路、重复入队/派发、取消与索引迁移。
- 新增 `tests/service-integration.test.mjs`：编排板创建、任务创建、依赖入队、重复派发拒绝、人工验收、驳回重做、取消、重启恢复、认证、路径穿越、敏感信息、XSS 输出和任务结果解析。
- 扩展 `tests/task-acceptance.test.mjs`：驳回理由、整改意见、批准后禁止改判。
- 全量测试 95 项通过。

### 兼容性

- DSH 宿主：声明兼容 `>=0.2.2-0 <0.2.3-0`；本机验收 CLI 为 `0.2.2-rc.5`。
- Node.js：`^22.19.0 || >=24.0.0`；本机验证版本 `22.23.4`。
- 未升级任何 DSH 宿主依赖或 peerDependencies。
- 运行时依赖保持 `proper-lockfile@4.1.2` 与 `zod@4.4.3`。

## 0.1.0-beta.1

初始公开测试版本。跨会话、可恢复的工作流编排、确定性账本、任务生命周期、验收与工作区隔离。
