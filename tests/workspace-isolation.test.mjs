import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
	WorkspaceError,
	assertAllowedRoot,
	assertRealPathInside,
	assertWorkspaceOwnedByTask,
	assertWorkspaceWritable,
	chmodTreeWritable,
	createTaskWorkspace,
	getWorkspace,
	mergeTaskWorkspace,
	realPathLoose,
	removeTaskWorkspace,
	resolveNodeWorkspaceCwd,
	saveWorkspace,
	updateWorkspaceStatus,
	workspaceDiff
} from "../lib/workspace-isolation.js";

const MIGRATIONS = join(dirname(fileURLToPath(import.meta.url)), "..", "lib", "infra", "sqlite", "migrations");
const NOW = "2026-01-01T00:00:00.000Z";

function tempRoot(label) {
	return mkdtempSync(join(tmpdir(), `dsh-ws-${label}-`));
}

function makeTx(db) {
	return {
		all: (sql, params = []) => db.prepare(sql).all(...params),
		get: (sql, params = []) => db.prepare(sql).get(...params),
		run: (sql, params = []) => {
			const result = db.prepare(sql).run(...params);
			return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) };
		}
	};
}

function freshDb() {
	const db = new DatabaseSync(":memory:");
	for (const file of readdirSync(MIGRATIONS).filter((name) => name.endsWith(".sql")).sort()) db.exec(readFileSync(join(MIGRATIONS, file), "utf8"));
	return makeTx(db);
}

function git(args, cwd) {
	return execFileSync("git", args, { cwd, encoding: "utf8", env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } }).trim();
}

function initRepo(root) {
	git(["init", "-q"], root);
	git(["config", "user.email", "test@example.com"], root);
	git(["config", "user.name", "Test"], root);
	writeFileSync(join(root, "app.txt"), "base\n", "utf8");
	git(["add", "."], root);
	git(["commit", "-q", "-m", "init"], root);
	return git(["rev-parse", "--abbrev-ref", "HEAD"], root);
}

function cleanup(...dirs) {
	for (const dir of dirs) {
		try {
			chmodWritableDeep(dir);
			rmSync(dir, { recursive: true, force: true });
		} catch {}
	}
}

function chmodWritableDeep(dir) {
	try {
		if (statSync(dir).isDirectory()) {
			execFileSync("chmod", ["-R", "u+rwX", dir]);
		}
	} catch {}
}

test("真实路径校验拒绝符号链接逃逸与越界目录", () => {
	const root = tempRoot("path");
	const outside = tempRoot("outside");
	try {
		const link = join(root, "escape");
		symlinkSync(outside, link);
		assert.throws(() => assertRealPathInside(root, link), (error) => error instanceof WorkspaceError && error.code === "FORBIDDEN");
		assert.throws(() => assertAllowedRoot([root], outside), (error) => error instanceof WorkspaceError && error.code === "FORBIDDEN");
		assert.equal(realPathLoose(join(root, "not-yet-created")), join(realPathLoose(root), "not-yet-created"));
	} finally {
		cleanup(root, outside);
	}
});

test("非 Git 项目创建独立临时目录，修改不污染源目录，并支持 diff/只读", () => {
	const base = tempRoot("nongit");
	const source = join(base, "source");
	const workspaceRoot = join(base, "workspaces");
	mkdirSync(source, { recursive: true });
	writeFileSync(join(source, "a.txt"), "original\n", "utf8");
	try {
		const workspace = createTaskWorkspace({
			taskId: "task-1",
			taskTitle: "隔离任务",
			sourceRoot: source,
			workspaceRoot,
			allowedRoots: [source],
			mode: "readwrite"
		});
		assert.equal(workspace.kind, "temp_dir");
		assert.equal(workspace.realPath.startsWith(realPathLoose(workspaceRoot)), true);
		writeFileSync(join(workspace.realPath, "a.txt"), "changed\n", "utf8");
		writeFileSync(join(workspace.realPath, "b.txt"), "new\n", "utf8");
		assert.equal(readFileSync(join(source, "a.txt"), "utf8"), "original\n");
		const diff = workspaceDiff(workspace);
		assert.equal(diff.kind, "temp_dir");
		assert.ok(diff.files.some((item) => item.file === "a.txt" && item.change === "modified"));
		assert.ok(diff.files.some((item) => item.file === "b.txt" && item.change === "added"));
		assert.throws(() => workspaceDiff({ ...workspace, allowed_roots: [], allowedRoots: [] }), (error) => error instanceof WorkspaceError);

		const readonly = createTaskWorkspace({
			taskId: "task-2",
			taskTitle: "只读任务",
			sourceRoot: source,
			workspaceRoot,
			allowedRoots: [source],
			mode: "readonly"
		});
		assert.equal(readonly.mode, "readonly");
		assert.throws(() => assertWorkspaceWritable(readonly), (error) => error instanceof WorkspaceError && error.code === "FORBIDDEN");
		assert.equal((statSync(join(readonly.realPath, "a.txt")).mode & 0o222), 0);
		chmodTreeWritable(readonly.realPath);
		assert.notEqual((statSync(join(readonly.realPath, "a.txt")).mode & 0o222), 0);
	} finally {
		cleanup(base);
	}
});

test("Git 项目创建独立 worktree/分支，diff、授权合并与拒绝未授权操作", () => {
	const base = tempRoot("git");
	const repo = join(base, "repo");
	const workspaceRoot = join(base, "workspaces");
	mkdirSync(repo, { recursive: true });
	const baseBranch = initRepo(repo);
	try {
		const workspace = createTaskWorkspace({
			taskId: "task-git",
			taskTitle: "Git 隔离",
			sourceRoot: repo,
			workspaceRoot,
			allowedRoots: [repo],
			mode: "readwrite"
		});
		assert.equal(workspace.kind, "git_worktree");
		assert.ok(workspace.branch.startsWith("dsh/"));
		assert.equal(git(["rev-parse", "--abbrev-ref", "HEAD"], workspace.realPath), workspace.branch);
		assert.ok(existsSync(join(workspace.realPath, "app.txt")));

		writeFileSync(join(workspace.realPath, "app.txt"), "task-change\n", "utf8");
		assert.match(workspaceDiff(workspace).patch, /task-change/u);
		git(["add", "."], workspace.realPath);
		git(["commit", "-q", "-m", "task change"], workspace.realPath);
		const diff = workspaceDiff(workspace);
		assert.match(diff.patch, /task-change/u);
		assert.equal(diff.branch, workspace.branch);

		assert.throws(() => mergeTaskWorkspace(workspace, { authorize: false }), (error) => error instanceof WorkspaceError && error.code === "CONFIRMATION_REQUIRED");
		const merged = mergeTaskWorkspace(workspace, { authorize: true, targetBranch: baseBranch });
		assert.equal(merged.merged, true);
		assert.ok(merged.mergeCommit);
		assert.equal(readFileSync(join(merged.integrationRoot, "app.txt"), "utf8"), "task-change\n");
		assert.ok(existsSync(workspace.realPath));

		assert.throws(() => removeTaskWorkspace({ ...workspace, status: "ready" }, { authorize: true }), (error) => error instanceof WorkspaceError && error.code === "FORBIDDEN");
		assert.throws(() => removeTaskWorkspace({ ...workspace, status: "merged" }, { authorize: false }), (error) => error instanceof WorkspaceError && error.code === "CONFIRMATION_REQUIRED");
	} finally {
		cleanup(base);
	}
});

test("合并冲突时保留任务 worktree、集成分支与全部修改", () => {
	const base = tempRoot("conflict");
	const repo = join(base, "repo");
	const workspaceRoot = join(base, "workspaces");
	mkdirSync(repo, { recursive: true });
	const baseBranch = initRepo(repo);
	try {
		const workspace = createTaskWorkspace({
			taskId: "task-conflict",
			taskTitle: "冲突任务",
			sourceRoot: repo,
			workspaceRoot,
			allowedRoots: [repo],
			mode: "readwrite"
		});
		writeFileSync(join(workspace.realPath, "app.txt"), "task-side\n", "utf8");
		git(["add", "."], workspace.realPath);
		git(["commit", "-q", "-m", "task side"], workspace.realPath);
		writeFileSync(join(repo, "app.txt"), "base-side\n", "utf8");
		git(["add", "."], repo);
		git(["commit", "-q", "-m", "base side"], repo);
		const out = mergeTaskWorkspace(workspace, { authorize: true, targetBranch: baseBranch });
		assert.equal(out.merged, false);
		assert.ok(out.conflicts.includes("app.txt"));
		assert.ok(existsSync(workspace.realPath));
		assert.equal(readFileSync(join(workspace.realPath, "app.txt"), "utf8"), "task-side\n");
		assert.ok(existsSync(out.integrationRoot));
	} finally {
		cleanup(base);
	}
});

test("任务/运行与工作区映射可反查，且拒绝跨任务访问", () => {
	const tx = freshDb();
	tx.run("INSERT INTO tasks (task_id, title, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)", ["t1", "任务一", "running", NOW, NOW]);
	tx.run("INSERT INTO tasks (task_id, title, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)", ["t2", "任务二", "running", NOW, NOW]);
	tx.run("INSERT INTO runs (run_id, task_id, status, created_at, updated_at, root_run_id, subworkflow_depth) VALUES (?, ?, ?, ?, ?, ?, ?)", ["r1", "t1", "running", NOW, NOW, "r1", 0]);
	tx.run("INSERT INTO node_runs (node_run_id, run_id, node_id, seq, node_type, attempt, status, revision, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ["n1", "r1", "a", 1, "agent", 1, "queued", 0, NOW, NOW]);
	const root = tempRoot("db");
	const real = join(root, "worktree");
	mkdirSync(real, { recursive: true });
	try {
		const saved = saveWorkspace(tx, {
			workspaceId: "ws_t1",
			taskId: "t1",
			kind: "temp_dir",
			mode: "readwrite",
			sourceRoot: root,
			workspaceRoot: root,
			realPath: real,
			allowedRoots: [root],
			status: "ready"
		}, NOW);
		assert.equal(saved.created, true);
		const resolved = resolveNodeWorkspaceCwd(tx, "n1");
		assert.equal(resolved.path, realPathLoose(real));
		assert.equal(resolved.taskId, "t1");
		assert.throws(() => assertWorkspaceOwnedByTask(tx, "ws_t1", "t2"), (error) => error instanceof WorkspaceError && error.code === "FORBIDDEN");
		updateWorkspaceStatus(tx, "ws_t1", "merged", NOW, { taskId: "t1", mergedAt: NOW, mergeCommit: "abc", mergeTarget: "dsh/integrate/x" });
		assert.equal(getWorkspace(tx, "ws_t1").status, "merged");
	} finally {
		cleanup(root);
	}
});
