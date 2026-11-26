# 升级与降级说明

适用版本：`0.1.0-beta.3` / `0.1.0-beta.3.1` → `0.1.0-beta.4`。Beta 4 移除内置 Docker 与受限网络执行沙箱，执行能力收敛为「执行权限与工作区」：只读工作区与经显式授权的本地进程。本次没有新增数据库迁移，迁移链仍以 `075_cost_controls.sql` 为最新；既有任务、事件、费用与工作流版本记录保持不变。升级前必须先迁移旧版 `docker` / `restricted_network` 执行配置，否则插件会明确拒绝启动，而不是静默降级为宿主执行。无论升级还是降级，都先保留可恢复的数据备份。

## 1. 升级前准备

1. 退出 DSH，或至少停用 Orchestra 插件，确保没有服务持有账本单写者锁。
2. 确认 Node.js 满足 `^22.19.0 || >=24.0.0`，DSH CLI 仍在 `>=0.2.2-0 <0.2.3-0`。
3. 找到账本数据库。个人 Desktop 默认位于 DSH 数据目录下的 `orchestra/personal/*.sqlite`；自管宿主以 `service.db_path` 为准。不确定时先列出目录：

```powershell
node scripts/orchestra-persistence.mjs list --dir <DSH 数据目录>
```

4. 记录升级前状态：

```powershell
node scripts/orchestra-persistence.mjs status --db <账本.sqlite>
node scripts/orchestra-persistence.mjs verify --db <账本.sqlite>
```

## 2. 备份数据

至少保留一份 SQLite 快照和一份逻辑导出。快照用于原样回滚，逻辑导出用于跨版本核对数据。

```powershell
node scripts/orchestra-persistence.mjs backup --db <账本.sqlite> --keep 10
node scripts/orchestra-persistence.mjs export --db <账本.sqlite> --out <备份目录>\orchestra.json --keep 10
```

命令会输出快照清单路径。请把 `<manifest.json>` 和生成的 `.sqlite` 一起保存到 DSH 数据目录之外的位置。默认滚动保留 10 份，不要用 `--keep 0`。

## 3. 升级

1. 安装前核对发布包。将 `Get-FileHash` 的结果与发布附带的 `SHA256SUMS` 比较，确认一致后再安装：

```powershell
Get-FileHash <tgz 的绝对路径> -Algorithm SHA256
```

2. 安装新的 `dsh-orchestra-0.1.0-beta.4.tgz`：

```powershell
dsh plugin --profile <profile> add <tgz 的绝对路径>
```

3. 重新加载 DSH，打开 Orchestra 面板。服务启动时会自动应用尚未执行的迁移；Beta 4 没有新增迁移，最新版本仍为 `075`。若此前尚未升级到 Beta 3，`075` 会补充：

- `scheduler_settings.budget_warn_percent`，默认 `80`
- `scheduler_settings.budget_interrupt_policy`，默认 `block_new`
- `scheduler_notifications.severity` 与 `metadata_json`
- 预算预留表 `scheduler_budget_reservations` 及其索引

4. 迁移旧版执行配置。Beta 4 不再接受 `sandbox.kind = "docker" | "restricted_network"`，也不会把旧模式自动映射为 `local_process`。请按以下规则手工改写：

- 不涉及代码执行的任务：改为 `sandbox.mode = "read_only_workspace"`。
- 涉及代码执行的任务：必须先重新确认宿主执行授权；确认后再配置 `sandbox.mode = "local_process"`、`sandbox.trust = "trusted"`，并由宿主显式授予本地执行权限。不可信代码只能停留在只读工作区。
- 旧的 `sandbox.risk` 标签不再决定隔离方式；风险由任务声明的 `sideEffectPolicy`（`pure` / `compensable` / `irreversible`）与 `executionTrust`（`trusted` / `untrusted`）共同决定。不可逆任务仍需人工审批。
- 配置未迁移时，插件会以 `LEGACY_EXECUTION_CONFIG` 明确拒绝启动并列出待迁移字段；历史任务记录、执行证据和 SQLite 数据不会被重置或删除。

预算阈值、中断策略与执行权限都有安全默认值。

5. 升级后再次检查：

```powershell
node scripts/orchestra-persistence.mjs status --db <账本.sqlite>
node scripts/orchestra-persistence.mjs verify --db <账本.sqlite>
```

`pendingMigrations` 应为空，`dbSchemaVersion` 应为 `75`。在面板确认总览、任务详情、时间线和画布可用，并检查设置页的预算阈值与中断策略。

## 4. 降级

降级不依赖自动降级迁移。Beta 4 没有新增迁移，但旧版本不应被假设能安全识别更高版本的迁移记录。推荐流程：

1. 退出 DSH，停用插件。
2. 对当前库再做一次快照，保留现场：

```powershell
node scripts/orchestra-persistence.mjs backup --db <账本.sqlite> --keep 10
```

3. 用升级前的快照恢复：

```powershell
node scripts/orchestra-persistence.mjs restore --db <账本.sqlite> --from <升级前 manifest.json> --keep 10
```

恢复前脚本会自动创建 `pre-restore` 快照；恢复失败时原库不会被静默删除。

4. 验证恢复结果：

```powershell
node scripts/orchestra-persistence.mjs verify --db <账本.sqlite>
node scripts/orchestra-persistence.mjs status --db <账本.sqlite>
```

5. 安装旧的 `dsh-orchestra-0.1.0-beta.3.tgz` 并重新加载。确认健康页和任务列表后，再恢复执行。旧版本的 Docker 执行配置需要一并恢复；Beta 4 不会替你改写旧配置。

## 5. 失败处理

- 迁移失败：保留原始库、失败库和快照，不要手工删除 `schema_migrations` 记录。按输出的快照清单恢复，并保留日志。
- 数据校验失败：先停止写入，运行 `verify --db <账本.sqlite>`；需要恢复时使用最近一份通过校验的快照。
- 面板接口 404 或空白：确认安装的是 `0.1.0-beta.4`，并检查面板请求是否经由 DSH 宿主代理；不要绕过宿主认证直接暴露服务端口。
- 启动报 `LEGACY_EXECUTION_CONFIG`：按第 3 节第 4 步迁移 `sandbox.kind` / `sandbox.risk`；不要通过删除配置或关闭校验来强行启动。
- 不要为了排障升级 DSH 宿主依赖或 `peerDependencies`。本版本未升级任何宿主依赖。

## 6. 兼容性记录

| 项目 | 本次验证 |
| --- | --- |
| DSH CLI | `0.2.2-rc.5`，在声明范围 `>=0.2.2-0 <0.2.3-0` 内 |
| Node.js | `22.23.4`，满足 `^22.19.0 || >=24.0.0` |
| 运行时依赖 | `proper-lockfile@4.1.2`、`zod@4.6.6`，未变更 |
| 数据库迁移 | 仍为 `001`–`075`，Beta 4 没有新增迁移，全量测试通过 |
| 执行配置 | `read_only_workspace` / `local_process`；旧 `docker` / `restricted_network` 必须手工迁移 |
| 外部依赖 | 不再需要 Docker Desktop 或 Docker Engine |
| 发布包完整性 | 双次打包一致，SHA256、CycloneDX SBOM、文件级清单与 Git commit 交叉校验通过 |
| 旧版 `orchestra.json` | 由持久化测试覆盖版本升级、损坏隔离与回滚 |
