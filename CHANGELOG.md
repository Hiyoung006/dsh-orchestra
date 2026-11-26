# 更新日志

本文件记录 DSH Orchestra 插件包的用户可见变更。版本号与 [`package.json`](package.json) 一致。

## 0.1.0-beta.4 — 2026-11-26

### 移除

- **内置 Docker Sandbox**：删除 `createDockerAdapter`、`createRestrictedNetworkAdapter`、容器生命周期与 CID/命名管理、镜像/CPU/内存/PID/临时磁盘参数、Docker 网络出口策略、DNS Pin、代理注入与 `compileEgressPolicy`。`docker` 与 `restricted_network` 不再是可用执行模式，DSH Orchestra 不再依赖 Docker Desktop 或 Docker Engine。
- **Restricted Network**：移除域名/IP/端口白名单、受保护网段与 `networkHost` 断言链路。删除不再有真实隔离语义的网络策略代码与测试。
- **旧配置兼容层**：不接受 `sandbox.kind = "docker" | "restricted_network"`，也不再把旧模式静默映射为 `local_process`。

### 变更

- **执行权限与工作区**：执行模式收敛为 `read_only_workspace` 与 `local_process`。界面与文档不再使用「执行沙箱」，避免把普通进程权限隔离误认为操作系统沙箱。
- **任务风险模型解耦**：`sideEffectPolicy`（`pure` / `compensable` / `irreversible`）与 `executionTrust`（`trusted` / `untrusted`）独立决定执行权限。纯任务只读；可补偿任务可在显式授权后本地执行；不可逆任务必须人工审批；不可信代码禁止在宿主执行。风险标签不能绕过执行限制。
- **本地进程加固**：命令只接受 `executable + args`，不使用 shell 字符串；拦截危险命令与 shell；清理敏感环境变量与 `NODE_OPTIONS` 等注入变量；校验真实工作目录；支持超时、取消、退出状态与执行证据。
- **旧配置检测**：启动时检测旧版 `sandbox.kind` / `sandbox.risk`，命中后以 `LEGACY_EXECUTION_CONFIG` 明确拒绝并输出迁移建议；不会重置 SQLite，也不会改写历史任务与执行证据。
- **版本**：`0.1.0-beta.4`。没有新增数据库迁移，迁移链仍到 `075`。

### 保留

- 只读工作区的 realpath 校验、路径穿越与符号链接逃逸阻断、文件大小与类型限制、超时与取消。
- Git Worktree 的任务隔离、Diff、变更追踪、合并、失败保留与授权清理。
- 服务端执行决策、工具白名单/禁用清单、执行证据与人工审批。

### 测试

- 新增无 Docker 环境启动、Docker 命令缺失、只读权限、未授权本地执行、路径限制、符号链接逃逸、shell 注入、敏感环境变量清理、执行超时、进程取消、危险命令拦截、不可逆审批、不可信代码拒绝、旧 Docker 配置迁移与旧 SQLite 记录保留测试。
- 现有 Git Worktree 隔离与工作区安全测试继续执行。

## 0.1.0-beta.3 — 2026-11-25

### 新增

- **Web Profile 宿主适配**：在 Cordis 上下文提供 `orchestraPanelTransport`；只有 owner 页面获得 proof 与 owner 身份，非 owner 使用受限传输且授权被拒绝。Web 服务仅放行已登记的只读工具，Desktop 路径保持不变。
- **独立质量审查增强**：新增内置 SAST 规则与秘密检测，审查报告记录 `sast` 证据；新增必须由具名人工触发的 LLM 复核，模型结论不能替代或放宽确定性审查，也不能用于自动通过。
- **统一执行沙箱**：支持 `local_process`、`read_only_workspace`、`docker`、`restricted_network`。目录、网络与工具权限由服务端策略默认拒绝，高风险任务只接受容器级隔离，沙箱不可用时拒绝执行而非回退宿主。
- **成本控制**：区分实际费用、在途预留与 committed 合计，按剩余并发名额分摊预留并在实际费用到达时冲销；新增预算阈值告警（默认 80%）和 `block_new`/`cancel_running` 中断策略。不使用 Token 估算金额冒充真实费用。
- 新增迁移 `075_cost_controls.sql`：预算阈值、中断策略、通知级别与元数据，以及预算预留表。

### 工程

- 发布基线改为以 `lib/` 为唯一源码，移除不可执行的 `tsdown`/`tsc` 构建脚本。
- 新增 `pnpm-lock.yaml` 与 `pnpm-workspace.yaml`，关闭 peer 自动安装，不拉取或升级 DSH 宿主依赖。
- 新增可复现打包：双次 `pnpm pack` 比对、SHA256、CycloneDX SBOM 和关联 Git commit 的 `build-info.json`。
- 新增 GitHub Actions：Type Check、Unit Test、Integration Test、Security Test、Build、Package Integrity。
- 发布包新增文件级清单 `package-files.json`，记录每个成员的大小与 SHA256；完整性校验逐项复核，并与锁文件、SBOM、Git 提交交叉核对。
- `dist/` 只允许保留发布包、校验文件与清单，出现未登记文件即校验失败。
- CI 新增 Dependency Review 作业，PR 引入高危依赖时直接阻断；构建产物保留 14 天。
- 新增标签发布流水线：运行 `pnpm check`、生成 SLSA 来源证明，并将包、`SHA256SUMS`、`build-info.json`、SBOM 与文件清单上传到 GitHub Release。
- 新增 R0 观察 harness `scripts/r0-observe.mjs`：从候选包启动 S6 合成宿主，记录候选、包摘要、真实时间、单调计时、账本水位与只读工作区摘要，支持 `launch`、`status`、`verify`、`cancel` 和短时 dry-run。
- R0 核验器复算采样槽，拒绝时间或单调计时回退、采样缺口、工作区变化、候选漂移、离开 R0 和失败终态。72 小时正式观察与 24 小时浸泡仍是独立验收项。

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
