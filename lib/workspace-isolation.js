// 多 Agent 工作空间隔离：Git worktree、非 Git 临时目录、路径真实解析与合并授权。
//
// 安全边界说明：Git worktree 不是操作系统级沙箱。这里的控制点是
// 1) 服务端决定子会话 cwd；2) 所有路径做 realpath + 允许根校验；
// 3) 只读模式实际修改文件权限；4) 合并/删除必须显式授权。
// 同一系统用户仍可用自己的命令改回权限，因此生产环境应叠加 OS 沙箱。

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
	chmodSync,
	cpSync,
	existsSync,
	lstatSync,
	mkdirSync,
	readdirSync,
	readFileSync,
	realpathSync,
	renameSync,
	rmSync,
	statSync,
	writeFileSync
} from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { tmpdir } from "node:os";

export const WORKSPACE_MODES = Object.freeze(["none", "readonly", "readwrite"]);
export const WORKSPACE_KINDS = Object.freeze(["git_worktree", "temp_dir", "shared"]);
export const WORKSPACE_STATUSES = Object.freeze(["creating", "ready", "review", "merged", "merge_conflict", "abandoned", "removed", "failed"]);

const COPY_EXCLUDES = new Set([".git", "node_modules", ".venv", "venv", "dist", "build", ".next", ".cache", ".DS_Store"]);
const MAX_PATCH_BYTES = 512 * 1024;
const MAX_DIFF_FILES = 2000;
const SAFE_SEGMENT = /[^a-zA-Z0-9._-]+/gu;

export class WorkspaceError extends Error {
	constructor(code, message, details = {}) {
		super(message);
		this.name = "WorkspaceError";
		this.code = code;
		this.details = details;
	}
}

function nowStamp() {
	return new Date().toISOString().replace(/[-:.TZ]/gu, "").slice(0, 14);
}

export function safeSegment(value, fallback = "workspace") {
	const cleaned = String(value ?? "").trim().replace(SAFE_SEGMENT, "-").replace(/^-+|-+$/gu, "").slice(0, 48);
	return cleaned || fallback;
}

function isWithin(root, target) {
	const rel = relative(root, target);
	return rel === "" || (!rel.startsWith("..") && !isAbsolute(rel));
}

/** realpath 解析；路径不存在时解析最近的已存在祖先，再拼回剩余段。 */
export function realPathLoose(target) {
	const absolute = resolve(target);
	let cursor = absolute;
	const missing = [];
	for (;;) {
		try {
			const real = realpathSync.native(cursor);
			return missing.length ? join(real, ...missing.reverse()) : real;
		} catch {
			const parent = dirname(cursor);
			if (parent === cursor) return absolute;
			missing.push(basename(cursor));
			cursor = parent;
		}
	}
}

/** 拒绝符号链接逃逸：逐段解析，任何已存在段必须是真实目录/文件且最终仍在 root 内。 */
export function assertRealPathInside(root, target, label = "路径") {
	const realRoot = realPathLoose(root);
	const realTarget = realPathLoose(target);
	if (!isWithin(realRoot, realTarget)) {
		throw new WorkspaceError("FORBIDDEN", `${label}逃逸工作目录`, { root: realRoot, target: realTarget });
	}
	let cursor = realRoot;
	const rel = relative(realRoot, realTarget);
	for (const segment of rel.split(sep).filter(Boolean)) {
		cursor = join(cursor, segment);
		let info;
		try {
			info = lstatSync(cursor);
		} catch {
			break;
		}
		if (!info.isSymbolicLink()) continue;
		const real = realpathSync.native(cursor);
		if (!isWithin(realRoot, real)) throw new WorkspaceError("FORBIDDEN", `${label}通过符号链接逃逸工作目录`, { link: cursor, target: real, root: realRoot });
	}
	return realTarget;
}

export function assertAllowedRoot(allowedRoots, target, label = "工作目录") {
	const roots = allowedRoots.map((root) => realPathLoose(root)).filter((root) => root !== "/");
	if (roots.length === 0) throw new WorkspaceError("FORBIDDEN", "没有配置允许的工作目录根");
	const realTarget = realPathLoose(target);
	if (!roots.some((root) => isWithin(root, realTarget))) {
		throw new WorkspaceError("FORBIDDEN", `${label}不在允许的工作目录根内`, { target: realTarget, allowedRoots: roots });
	}
	return realTarget;
}

export function defaultWorkspaceRoot(home) {
	return join(home, "orchestra", "workspaces");
}

function git(args, cwd, options = {}) {
	try {
		return execFileSync("git", args, {
			cwd,
			encoding: "utf8",
			stdio: ["ignore", "pipe", "pipe"],
			env: { ...process.env, GIT_TERMINAL_PROMPT: "0", GIT_ASKPASS: "true" },
			maxBuffer: options.maxBuffer ?? 32 * 1024 * 1024
		}).trim();
	} catch (error) {
		const stderr = error?.stderr ? String(error.stderr) : "";
		throw new WorkspaceError("GIT_FAILED", `git ${args[0]} 失败`, {
			cwd,
			args,
			exitCode: error?.status ?? null,
			stderr: stderr.slice(0, 4000)
		});
	}
}

export function detectGitRepo(dir) {
	const real = realPathLoose(dir);
	let probe = real;
	for (;;) {
		if (existsSync(join(probe, ".git"))) {
			const top = git(["rev-parse", "--show-toplevel"], probe);
			const common = git(["rev-parse", "--git-common-dir"], probe);
			return {
				root: realpathSync.native(top),
				gitDir: realpathSync.native(resolve(probe, common)),
				bare: git(["rev-parse", "--is-bare-repository"], probe) === "true"
			};
		}
		const parent = dirname(probe);
		if (parent === probe) return null;
		probe = parent;
	}
}

export function gitBranchName(taskId, title = "") {
	return `dsh/${safeSegment(title, "task")}-${safeSegment(taskId, "task").slice(-12)}`;
}

export function gitHead(repo) {
	return git(["rev-parse", "HEAD"], repo);
}

export function gitCurrentBranch(repo) {
	try {
		return git(["rev-parse", "--abbrev-ref", "HEAD"], repo);
	} catch {
		return null;
	}
}

function chmodTreeReadonly(root) {
	const stack = [root];
	while (stack.length) {
		const current = stack.pop();
		let entries;
		try {
			entries = readdirSync(current, { withFileTypes: true });
		} catch {
			continue;
		}
		for (const entry of entries) {
			if (entry.name === ".git") continue;
			const target = join(current, entry.name);
			if (entry.isSymbolicLink()) continue;
			if (entry.isDirectory()) {
				stack.push(target);
				try {
					chmodSync(target, 0o555);
				} catch {}
			} else if (entry.isFile()) {
				try {
					chmodSync(target, statSync(target).mode & 0o111 ? 0o555 : 0o444);
				} catch {}
			}
		}
	}
	try {
		chmodSync(root, 0o555);
	} catch {}
}

/** 只读 -> 可写时恢复权限；只影响工作区文件，不碰 .git 元数据。 */
export function chmodTreeWritable(root) {
	const stack = [root];
	while (stack.length) {
		const current = stack.pop();
		let entries;
		try {
			entries = readdirSync(current, { withFileTypes: true });
		} catch {
			continue;
		}
		for (const entry of entries) {
			if (entry.name === ".git") continue;
			const target = join(current, entry.name);
			if (entry.isSymbolicLink()) continue;
			if (entry.isDirectory()) {
				stack.push(target);
				try {
					chmodSync(target, 0o755);
				} catch {}
			} else if (entry.isFile()) {
				try {
					chmodSync(target, statSync(target).mode & 0o111 ? 0o755 : 0o644);
				} catch {}
			}
		}
	}
	try {
		chmodSync(root, 0o755);
	} catch {}
}

function copySourceTree(source, target) {
	mkdirSync(target, { recursive: true });
	cpSync(source, target, {
		recursive: true,
		dereference: false,
		errorOnExist: false,
		filter: (from) => {
			const rel = relative(source, from);
			if (!rel) return true;
			const first = rel.split(sep)[0];
			if (COPY_EXCLUDES.has(first)) return false;
			try {
				if (lstatSync(from).isSymbolicLink()) {
					const real = realPathLoose(from);
					if (!isWithin(realPathLoose(source), real)) return false;
				}
			} catch {}
			return true;
		}
	});
}

function manifestOf(root) {
	const files = {};
	const stack = [""];
	while (stack.length) {
		const relDir = stack.pop();
		let entries;
		try {
			entries = readdirSync(join(root, relDir), { withFileTypes: true });
		} catch {
			continue;
		}
		for (const entry of entries) {
			if (entry.name === ".git") continue;
			const rel = relDir ? join(relDir, entry.name) : entry.name;
			if (entry.isDirectory()) {
				stack.push(rel);
				continue;
			}
			if (!entry.isFile()) continue;
			try {
				files[rel] = createHash("sha256").update(readFileSync(join(root, rel))).digest("hex");
			} catch {}
		}
	}
	return files;
}

function writeManifest(path, files) {
	writeFileSync(path, JSON.stringify({ schemaVersion: 1, files }), "utf8");
}

function readManifest(path) {
	try {
		const parsed = JSON.parse(readFileSync(path, "utf8"));
		return parsed?.files && typeof parsed.files === "object" ? parsed.files : {};
	} catch {
		return {};
	}
}

export function ensureAllowedRoots(allowedRoots, sourceRoot, workspaceRoot) {
	const roots = [...allowedRoots];
	// 隔离根由服务持有，可隐式允许；源目录必须落在显式允许根内。
	if (workspaceRoot) roots.push(workspaceRoot);
	return [...new Set(roots.map((root) => realPathLoose(root)))];
}

function workspaceIdFor(taskId, branch) {
	return `ws_${safeSegment(taskId, "task").slice(-24)}_${createHash("sha256").update(`${taskId}\0${branch ?? ""}`).digest("hex").slice(0, 10)}`;
}

/**
 * 为任务建立隔离工作区（幂等）。
 * 输入 p: { taskId, taskTitle, sourceRoot, workspaceRoot, allowedRoots, mode, branch, baseBranch }
 */
export function createTaskWorkspace(p) {
	const mode = WORKSPACE_MODES.includes(p.mode) ? p.mode : "none";
	if (mode === "none") return null;
	const sourceRoot = realPathLoose(p.sourceRoot);
	const workspaceRoot = realPathLoose(p.workspaceRoot);
	const allowedRoots = ensureAllowedRoots(p.allowedRoots ?? [], sourceRoot, workspaceRoot);
	assertAllowedRoot(allowedRoots, sourceRoot, "源工作目录");
	assertAllowedRoot(allowedRoots, workspaceRoot, "隔离工作区根");
	mkdirSync(workspaceRoot, { recursive: true });
	const repo = detectGitRepo(sourceRoot);
	const branch = p.branch || (repo ? gitBranchName(p.taskId, p.taskTitle) : null);
	const workspaceId = workspaceIdFor(p.taskId, branch);
	const target = join(workspaceRoot, safeSegment(p.taskId, "task").slice(-40) || workspaceId, branch ? "worktree" : "copy");
	const realTarget = realPathLoose(target);
	assertAllowedRoot(allowedRoots, realTarget, "任务工作区");
	if (!isWithin(realPathLoose(workspaceRoot), realTarget)) throw new WorkspaceError("FORBIDDEN", "任务工作区不在隔离根内", { target: realTarget, workspaceRoot });

	if (repo) {
		const baseBranch = p.baseBranch || gitCurrentBranch(repo.root) || "HEAD";
		const baseCommit = gitHead(repo.root);
		if (!existsSync(realTarget)) {
			mkdirSync(dirname(realTarget), { recursive: true });
			let branchExists = false;
			try {
				git(["rev-parse", "--verify", `refs/heads/${branch}`], repo.root);
				branchExists = true;
			} catch {}
			git(["worktree", "add", ...branchExists ? [realTarget, branch] : ["-b", branch, realTarget, baseCommit]], repo.root);
		} else if (git(["rev-parse", "--is-inside-work-tree"], realTarget) !== "true") {
			throw new WorkspaceError("WORKSPACE_EXISTS", "目标路径已存在且不是 Git worktree", { target: realTarget });
		}
		if (mode === "readonly") chmodTreeReadonly(realTarget);
		return {
			workspaceId,
			taskId: p.taskId,
			kind: "git_worktree",
			mode,
			sourceRoot,
			workspaceRoot,
			realPath: realTarget,
			allowedRoots,
			repoRoot: repo.root,
			gitDir: repo.gitDir,
			branch,
			baseBranch,
			baseCommit,
			status: "ready",
			cleanupPolicy: "keep"
		};
	}

	// 非 Git：复制一份独立目录；只读模式在复制完成后收紧权限。
	if (!existsSync(realTarget)) {
		mkdirSync(dirname(realTarget), { recursive: true });
		copySourceTree(sourceRoot, realTarget);
	}
	const manifestPath = join(dirname(realTarget), ".dsh-workspace-manifest.json");
	if (!existsSync(manifestPath)) writeManifest(manifestPath, manifestOf(realTarget));
	if (mode === "readonly") chmodTreeReadonly(realTarget);
	return {
		workspaceId,
		taskId: p.taskId,
		kind: "temp_dir",
		mode,
		sourceRoot,
		workspaceRoot,
		realPath: realTarget,
		allowedRoots,
		repoRoot: null,
		gitDir: null,
		branch: null,
		baseBranch: null,
		baseCommit: null,
		manifestPath,
		status: "ready",
		cleanupPolicy: "keep"
	};
}

function allowedRootsOf(workspace) {
	if (typeof workspace.allowed_roots === "string") {
		try {
			const parsed = JSON.parse(workspace.allowed_roots);
			if (Array.isArray(parsed) && parsed.length) return parsed;
		} catch {}
	}
	if (Array.isArray(workspace.allowed_roots) && workspace.allowed_roots.length) return workspace.allowed_roots;
	return workspace.allowedRoots ?? (workspace.allowed_root ? [workspace.allowed_root] : []);
}

/** Git diff（含未提交修改）与非 Git 文件清单差异。 */
export function workspaceDiff(workspace, options = {}) {
	const realRoot = realPathLoose(workspace.real_path ?? workspace.realPath);
	assertAllowedRoot(allowedRootsOf(workspace), realRoot, "工作区");
	if (workspace.kind === "git_worktree") {
		const base = workspace.base_commit ?? workspace.baseCommit ?? "HEAD";
		const status = git(["status", "--porcelain=v1", "-z"], realRoot).split("\0").filter(Boolean);
		let changedFiles = [];
		try {
			changedFiles = git(["diff", "--name-only", `${base}...HEAD`], realRoot).split("\n").filter(Boolean);
		} catch {}
		for (const entry of status) changedFiles.push(entry.slice(3));
		changedFiles = [...new Set(changedFiles)];
		let stat = "";
		let patch = "";
		try {
			stat = git(["diff", "--stat", `${base}...HEAD`], realRoot);
			patch = git(["diff", "--no-ext-diff", `${base}...HEAD`], realRoot);
		} catch (error) {
			if (error instanceof WorkspaceError) {
				stat = git(["diff", "--stat", base], realRoot);
				patch = git(["diff", "--no-ext-diff", base], realRoot);
			} else throw error;
		}
		if (status.length) {
			stat += `${stat ? "\n" : ""}未提交变更：${status.length} 项`;
			try {
				patch += `\n${git(["diff", "--no-ext-diff"], realRoot)}`;
			} catch {}
		}
		return {
			kind: "git_worktree",
			branch: workspace.branch,
			baseBranch: workspace.base_branch ?? workspace.baseBranch,
			baseCommit: base,
			uncommitted: status.slice(0, options.maxFiles ?? MAX_DIFF_FILES),
			changedFiles: changedFiles.slice(0, options.maxFiles ?? MAX_DIFF_FILES),
			stat: stat.slice(0, options.maxBytes ?? MAX_PATCH_BYTES),
			patch: patch.slice(0, options.maxBytes ?? MAX_PATCH_BYTES),
			truncated: patch.length > (options.maxBytes ?? MAX_PATCH_BYTES)
		};
	}
	const baseline = readManifest(workspace.manifest_path ?? workspace.manifestPath);
	const current = manifestOf(realRoot);
	const files = [];
	for (const [file, hash] of Object.entries(current)) {
		if (baseline[file] !== hash) files.push({ file, change: baseline[file] ? "modified" : "added" });
	}
	for (const file of Object.keys(baseline)) {
		if (!(file in current)) files.push({ file, change: "deleted" });
	}
	return {
		kind: "temp_dir",
		branch: null,
		baseBranch: null,
		baseCommit: null,
		uncommitted: [],
		changedFiles: files.map((item) => item.file),
		stat: `${files.length} 个文件发生变化`,
		patch: files.map((item) => `${item.change}\t${item.file}`).join("\n").slice(0, options.maxBytes ?? MAX_PATCH_BYTES),
		files
	};
}

/**
 * 合并任务分支：在一个独立的集成分支上进行，不触碰用户当前工作区。
 * 失败时保留任务 worktree、集成分支和所有修改。
 */
export function mergeTaskWorkspace(workspace, p) {
	if (p.authorize !== true) throw new WorkspaceError("CONFIRMATION_REQUIRED", "合并需要明确授权", { workspaceId: workspace.workspace_id ?? workspace.workspaceId });
	if (workspace.kind !== "git_worktree") throw new WorkspaceError("FORBIDDEN", "只有 Git worktree 工作区支持自动合并", { kind: workspace.kind });
	const repoRoot = realPathLoose(workspace.repo_root ?? workspace.repoRoot);
	assertAllowedRoot(allowedRootsOf(workspace), repoRoot, "仓库根");
	const taskRoot = realPathLoose(workspace.real_path ?? workspace.realPath);
	assertAllowedRoot(allowedRootsOf(workspace), taskRoot, "任务工作区");
	const dirty = git(["status", "--porcelain"], taskRoot).trim();
	if (dirty) {
		return {
			merged: false,
			integrationBranch: null,
			integrationRoot: null,
			baseBranch: workspace.base_branch,
			baseCommit: workspace.base_commit,
			mergeCommit: null,
			conflicts: [],
			dirty: dirty.split("\n").slice(0, 100),
			error: { code: "WORKSPACE_DIRTY", message: "任务工作区有未提交修改，请先提交后再合并" }
		};
	}
	const base = p.targetBranch || workspace.base_branch || workspace.baseBranch || gitCurrentBranch(repoRoot) || "HEAD";
	const unique = `${nowStamp()}-${Math.random().toString(16).slice(2, 8)}`;
	const integrationBranch = p.integrationBranch || `dsh/integrate/${safeSegment(workspace.task_id ?? workspace.taskId, "task").slice(-24)}-${unique}`;
	const integrationRoot = join(dirname(taskRoot), `integration-${unique}`);
	const realIntegration = realPathLoose(integrationRoot);
	assertAllowedRoot(allowedRootsOf(workspace), realIntegration, "集成工作区");
	if (!existsSync(realIntegration)) {
		mkdirSync(dirname(realIntegration), { recursive: true });
		git(["worktree", "add", "-b", integrationBranch, realIntegration, base], repoRoot);
	}
	const baseCommit = gitHead(realIntegration);
	try {
		git(["merge", "--no-ff", "--no-edit", workspace.branch], realIntegration, { maxBuffer: 64 * 1024 * 1024 });
		const mergeCommit = gitHead(realIntegration);
		return {
			merged: true,
			integrationBranch,
			integrationRoot: realIntegration,
			baseBranch: base,
			baseCommit,
			mergeCommit,
			conflicts: []
		};
	} catch (error) {
		let conflicts = [];
		try {
			conflicts = git(["diff", "--name-only", "--diff-filter=U"], realIntegration).split("\n").filter(Boolean);
		} catch {}
		return {
			merged: false,
			integrationBranch,
			integrationRoot: realIntegration,
			baseBranch: base,
			baseCommit,
			mergeCommit: null,
			conflicts,
			error: error instanceof WorkspaceError ? error.details : {}
		};
	}
}

/** 删除工作区必须显式授权；失败/取消的任务不允许自动删除。 */
export function removeTaskWorkspace(workspace, p) {
	if (p.authorize !== true) throw new WorkspaceError("CONFIRMATION_REQUIRED", "删除工作区需要明确授权", { workspaceId: workspace.workspace_id ?? workspace.workspaceId });
	const status = workspace.status ?? "ready";
	if (!["merged", "abandoned", "removed"].includes(status) && p.force !== true) {
		throw new WorkspaceError("FORBIDDEN", "只有已合并或已放弃的工作区可以删除", { status });
	}
	const real = realPathLoose(workspace.real_path ?? workspace.realPath);
	assertAllowedRoot(allowedRootsOf(workspace), real, "工作区");
	if (workspace.kind === "git_worktree") {
		const repoRoot = realPathLoose(workspace.repo_root ?? workspace.repoRoot);
		git(["worktree", "remove", "--force", real], repoRoot);
		git(["branch", "-D", workspace.branch], repoRoot);
	} else {
		rmSync(real, { recursive: true, force: false });
	}
	return { removed: true, path: real };
}

/** 只读工作区写保护：服务端自身写文件前必须调用。 */
export function assertWorkspaceWritable(workspace) {
	if (workspace.mode === "readonly") throw new WorkspaceError("FORBIDDEN", "当前工作区是只读模式", { workspaceId: workspace.workspace_id ?? workspace.workspaceId });
	return true;
}

// ---------------------------------------------------------------------------
// 持久化辅助（tx = { all, get, run }）
// ---------------------------------------------------------------------------

export function listTaskWorkspaces(tx, taskId) {
	return tx.all("SELECT * FROM task_workspaces WHERE task_id = ? ORDER BY created_at DESC", [taskId]);
}

export function getWorkspace(tx, workspaceId) {
	return tx.get("SELECT * FROM task_workspaces WHERE workspace_id = ?", [workspaceId]);
}

export function taskWorkspaceConfig(tx, taskId) {
	const task = tx.get("SELECT task_id, title, current_run_id, workspace_mode, workspace_root, workspace_branch FROM tasks WHERE task_id = ?", [taskId]);
	if (!task) throw new WorkspaceError("NOT_FOUND", "任务不存在", { taskId });
	return task;
}

export function saveWorkspace(tx, w, now) {
	const roots = allowedRootsOf(w);
	const container = w.workspaceRoot ?? roots[0] ?? w.realPath;
	const existing = tx.get("SELECT workspace_id, status FROM task_workspaces WHERE workspace_id = ?", [w.workspaceId]);
	if (existing) {
		tx.run(
			`UPDATE task_workspaces SET mode = ?, source_root = ?, real_path = ?, allowed_root = ?, allowed_roots = ?, repo_root = ?,
			 worktree_path = ?, branch = ?, base_branch = ?, base_commit = ?, status = ?, cleanup_policy = ?,
			 manifest_path = ?, attempt_id = COALESCE(?, attempt_id), updated_at = ?
			 WHERE workspace_id = ?`,
			[
				w.mode, w.sourceRoot, w.realPath, container, JSON.stringify(roots), w.repoRoot ?? null,
				w.kind === "git_worktree" ? w.realPath : null, w.branch ?? null, w.baseBranch ?? null, w.baseCommit ?? null,
				w.status, w.cleanupPolicy ?? "keep", w.manifestPath ?? null, w.attemptId ?? null, now, w.workspaceId
			]
		);
		return { ...w, created: false };
	}
	tx.run(
		`INSERT INTO task_workspaces
		 (workspace_id, task_id, run_id, node_run_id, attempt_id, kind, mode, source_root, real_path, allowed_root,
		  allowed_roots, repo_root, worktree_path, branch, base_branch, base_commit, manifest_path, status, cleanup_policy, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[
			w.workspaceId, w.taskId, w.runId ?? null, w.nodeRunId ?? null, w.attemptId ?? null, w.kind, w.mode,
			w.sourceRoot, w.realPath, container, JSON.stringify(roots), w.repoRoot ?? null,
			w.kind === "git_worktree" ? w.realPath : null, w.branch ?? null, w.baseBranch ?? null, w.baseCommit ?? null,
			w.manifestPath ?? null, w.status, w.cleanupPolicy ?? "keep", now, now
		]
	);
	return { ...w, created: true };
}

export function updateWorkspaceStatus(tx, workspaceId, status, now, extra = {}) {
	tx.run(
		"UPDATE task_workspaces SET status = ?, merged_at = COALESCE(?, merged_at), merge_commit = COALESCE(?, merge_commit), merge_target = COALESCE(?, merge_target), updated_at = ? WHERE workspace_id = ?",
		[status, extra.mergedAt ?? null, extra.mergeCommit ?? null, extra.mergeTarget ?? null, now, workspaceId]
	);
	tx.run("INSERT INTO workspace_events (workspace_id, task_id, kind, detail, created_at) VALUES (?, ?, ?, ?, ?)", [
		workspaceId,
		extra.taskId ?? null,
		`status:${status}`,
		JSON.stringify(extra.detail ?? {}),
		now
	]);
}

/** 从 nodeRun 反查任务工作区；无配置时返回 null，保持旧行为。 */
export function workspaceForNodeRun(tx, nodeRunId) {
	const row = tx.get(
		`SELECT w.* FROM node_runs n JOIN runs r ON r.run_id = n.run_id
		 JOIN task_workspaces w ON w.task_id = r.task_id
		 WHERE n.node_run_id = ? AND w.status IN ('creating','ready','review','merge_conflict')
		 ORDER BY w.created_at DESC LIMIT 1`,
		[nodeRunId]
	);
	return row ?? null;
}

export function assertWorkspaceOwnedByTask(tx, workspaceId, taskId) {
	const row = getWorkspace(tx, workspaceId);
	if (!row) throw new WorkspaceError("NOT_FOUND", "工作区不存在", { workspaceId });
	if (row.task_id !== taskId) throw new WorkspaceError("FORBIDDEN", "不能访问其他任务的工作区", { workspaceId, taskId });
	return row;
}

/** 解析并校验子会话 cwd；所有失败都回退到 null，由调用方决定拒绝或继承。 */
export function resolveNodeWorkspaceCwd(tx, nodeRunId, allowedRoots = []) {
	const workspace = workspaceForNodeRun(tx, nodeRunId);
	if (!workspace) return null;
	const real = realPathLoose(workspace.real_path);
	assertAllowedRoot([...allowedRootsOf(workspace), ...allowedRoots], real, "节点工作区");
	assertRealPathInside(workspace.real_path, real, "节点工作区");
	return { path: real, mode: workspace.mode, workspaceId: workspace.workspace_id, taskId: workspace.task_id };
}
