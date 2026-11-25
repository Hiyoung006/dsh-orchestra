# DSH Orchestra Beta 3.1 专项安全与质量改进 · 验收报告

- 基线版本：0.1.0-beta.3（tag `v0.1.0-beta.3` / `a7de7eb`）
- 当前提交：`c912a60`
- 检查时间：2026-11-25（Asia/Shanghai）
- 结论：任务一至任务五已完成并通过回归测试；任务六已启动正式 72 小时观察，**当前处于进行中，尚未验收通过**。

## 1. 阶段提交与变更摘要

| 阶段 | 提交 | 范围 | 摘要 |
| --- | --- | --- | --- |
| 一 | `d2cb66e` | Web Profile 身份认证 | `ownerOf(peer)` 缺失 peer/principal 一律拒绝；owner 只来自宿主绑定 principal；anonymous/guest 不获得管理权限；新增缺失 peer、缺失 principal、伪造身份回归测试；Desktop Profile 路径不变。 |
| 二 | `61aa35c` | 执行沙箱安全 | 文件操作 realpath 校验与符号链接逃逸拒绝；TOCTOU 窗口收敛；容器 ID 跟踪；取消/超时停止并清理容器；CPU、内存、PID、临时磁盘限制校验。 |
| 三 | `c7eb595` | Docker 网络出口控制 | `restricted_network` 必须配置出口策略；域名/IP/端口白名单；DNS pin 与外部 DNS 禁用；拒绝云元数据等受保护网段；`networkHost` 只作断言、不参与授权；未配置出口控制时拒绝启用。 |
| 四 | `549f12a` | LLM Review 接入 | 新增 `POST /v1/tasks/acceptance/review` 人工入口与 UI 按钮；显式配置 provider/model；只读证据直连模型；结论落库为子报告；模型意见只能收紧不能放宽；不执行最终人工审批；未配置返回能力不可用（503），不伪造成功。 |
| 五 | `89dd01b` | AST 与依赖漏洞扫描 | TypeScript 正式 Parser Provider；package-lock v1/v2/v3、pnpm-lock、yarn.lock、package.json 解析；OSV Advisory Provider；结果含组件、版本、漏洞编号、严重程度、修复建议；`ok / failed / unconfigured` 严格区分；新增 `POST /v1/tasks/acceptance/scan` 与 UI 入口；未执行的扫描不会判为通过。 |
| 六 | `61ff3c7` | 72 小时运行稳定性 | worker 采样资源占用、磁盘余量、任务状态快照、未处理异常；启动时执行并记录任务恢复；`verify` 默认硬性要求 soak 模式、完成态、实际与配置均 ≥72h、采样完整、零未处理异常、恢复/任务/资源快照齐全；新增 Ed25519 签名验收报告与离线复核。 |
| 杂项 | `c912a60` | 工程 | 忽略 R0 观察运行目录 `.r0/`。 |

数据兼容：本批次未新增或修改数据库迁移，沿用 schema 75，既有任务、账本与工作区记录可直接读取。

## 2. 测试结果

执行命令：`pnpm check`（typecheck + 全量测试 + 可复现构建 + 包完整性校验）

- 类型/语法检查：35 个 JS 文件通过，公开导出与声明文件完整。
- 测试：187 项，通过 186，失败 0，跳过 1。
  - 跳过项：`tests/execution-sandbox.test.mjs` 中的真实网络出口测试，需显式设置 `DSH_TEST_NETWORK_EGRESS=1` 且宿主机 Docker daemon 可用。
  - **补跑结果（2026-11-25）**：启动 Docker Desktop 后，使用
    `DSH_TEST_NETWORK_EGRESS=1 DOCKER_HOST=unix:///Users/young/.docker/run/docker.sock node --no-warnings --test tests/execution-sandbox.test.mjs`
    执行，21 项全部通过（0 失败、0 跳过，耗时 22.5s）。真实网络用例验证了固定域名写入 `/etc/hosts` 可解析、未固定域名无法通过外部 DNS 解析。
- 打包：两次 `pnpm pack` 内容一致（可复现），tarball SHA256 `4192af41…81b6`，104 个文件，`verify:package` 通过。
- 关键回归：
  - Web Profile：缺失 peer、缺失 principal、伪造身份、anonymous/guest 均被拒绝。
  - 沙箱：只读边界、符号链接逃逸、取消/超时清理、资源限制。
  - 出口控制：未配置出口拒绝启用；伪造 `networkHost` 不改变网络；域名/IP/端口/pin 断言。
  - LLM Review：未配置时 UI/接口明确不可用；人工触发记录 `requested_by` 与 `parent_review_id`，确定性 `fail` 不会被放宽。
  - 扫描：AST 成功/失败、锁文件组件版本、OSV 映射、扫描失败不放宽结论、未配置拒绝触发。
  - R0：严格拒绝 dry-run/短时观察与异常样本；签名报告防篡改。

## 3. 任务六：72 小时观察（进行中）

- 观察目录：`.r0/beta3.1-c912a60`
- 候选提交：`c912a6006235`
- 候选包 SHA256：`4192af41faa1fcc6518dda871573b277411c7de9deb4f815b2355adb571a81b6`
- 启动时间：2026-11-25T09:11:42Z（约 17:11 Asia/Shanghai）
- 计划结束：2026-11-28T09:11:42Z
- 采样间隔：60 秒；状态：worker 运行中，已记录任务恢复、账本水位、资源与磁盘快照。

结束后需依次执行：

```bash
node --no-warnings scripts/r0-observe.mjs verify .r0/beta3.1-c912a60
node --no-warnings scripts/r0-observe.mjs report .r0/beta3.1-c912a60 --key <ed25519-private.pem>
node --no-warnings scripts/r0-observe.mjs verify-report .r0/beta3.1-c912a60/report.json
```

`verify` 或报告中的任一硬条件失败时，`accepted` 必为 `false`，不得作为验收通过证据。

## 4. 未解决风险

1. **真实网络隔离已验证，但依赖 Docker Desktop 上下文**：默认 `/var/run/docker.sock` 未运行，补跑时通过 `DOCKER_HOST=unix:///Users/young/.docker/run/docker.sock` 指向 Docker Desktop。CI 或其它主机需确保 daemon 可用后以 `DSH_TEST_NETWORK_EGRESS=1` 重跑，否则该项仍会被跳过而不能计为通过。
2. **72 小时观察尚未完成**：当前只有运行中的现场，未生成签名验收报告；期间若宿主机休眠、进程被杀或磁盘不足，观察应判为失败并重新运行。
3. **出口控制依赖容器 DNS 与代理配合**：无代理时仅支持 pinned 域名；未 pin 的通配域名一律拒绝，可能影响依赖多域名的任务，需要在真实任务中逐步放行。
4. **OSV 依赖扫描依赖外网**：`api.osv.dev` 不可达时会标记 `failed`（不会判通过），需要离线镜像时另配 Advisory Provider。
5. **扫描范围有上限**：单文件 256KB、最多 400 个文件、深度 8；超大仓库会记录跳过项，不能视为全量覆盖。
6. **LLM Review 依赖宿主模型配置**：未配置时明确不可用；模型输出仍可能存在误判，因此只作为收紧项，不替代人工审批。
7. **TOCTOU 残余**：沙箱与扫描均在读取前后做 realpath/边界复核，但宿主文件系统本身不受控，极端并发替换仍需依赖只读挂载与宿主机权限。

## 5. 回滚方案

本批次提交相互独立，可按阶段回滚，不涉及数据迁移：

```bash
# 整体回滚到 Beta 3 基线
git revert --no-commit c912a60 61ff3c7 89dd01b 549f12a c7eb595 61aa35c d2cb66e
git commit -m "revert: 回滚 Beta 3.1 专项改进"

# 或仅回滚单个阶段，例如扫描接入
git revert 89dd01b
```

- 回滚后无需降级数据库；`review_reports` 中的新增子报告仅作为历史记录保留，旧代码忽略未知字段即可。
- 观察目录 `.r0/` 不参与打包与提交，可原样保留用于事后审计；如需停止观察：`node --no-warnings scripts/r0-observe.mjs cancel .r0/beta3.1-c912a60`。
- 回滚后建议重新执行 `pnpm check`，并确认 Docker 环境下 `DSH_TEST_NETWORK_EGRESS=1 pnpm test:sandbox` 的行为与回滚前一致。
