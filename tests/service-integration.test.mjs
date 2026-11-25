import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

import { serveOrchestra } from "../lib/index.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SECRET = "integration-secret-".padEnd(64, "x");
const HUMAN = { id: "human:integration" };
const NOW = "2026-01-01T00:00:00.000Z";

function tempDir(prefix = "dsh-orchestra-it-") {
	return mkdtempSync(join(tmpdir(), prefix));
}

function options(dir, overrides = {}) {
	return {
		dbFile: join(dir, "service.sqlite"),
		backupDir: join(dir, "backups"),
		secret: SECRET,
		phase: "S6",
		preferNamedPipe: false,
		configPath: join(ROOT, "orchestra.config.json"),
		asOfSeq: () => 0,
		listTasks: () => [],
		listEvents: () => [],
		appendLedgerEvent: () => {},
		resolveHttpHuman: () => HUMAN,
		...overrides
	};
}

function operation(n) {
	return `01900000-0000-7000-8000-${String(n).padStart(12, "0")}`;
}

async function api(service, path, { method = "GET", body, secret = SECRET, headers = {} } = {}) {
	const response = await fetch(service.endpoint + path, {
		method,
		headers: {
			accept: "application/json",
			...(body === undefined ? {} : { "content-type": "application/json" }),
			...(secret ? { "x-orchestra-secret": secret } : {}),
			...headers
		},
		body: body === undefined ? undefined : JSON.stringify(body)
	});
	const text = await response.text();
	let json;
	try {
		json = JSON.parse(text);
	} catch {
		json = undefined;
	}
	return { status: response.status, text, json, contentType: response.headers.get("content-type") };
}

function withDb(dbFile, fn) {
	const db = new DatabaseSync(dbFile);
	db.exec("PRAGMA busy_timeout=5000");
	try {
		return fn(db);
	} finally {
		db.close();
	}
}

function currentEpoch(db) {
	return Number(db.prepare("SELECT value FROM service_meta WHERE key = 'instance_epoch'").get()?.value ?? 1);
}

function seedTask(dbFile, { taskId, lifecycle = "draft", status = "reserved", runId = null, runStatus = "running", nodeStatus = "started", workflow = true, ownerEpoch = null } = {}) {
	return withDb(dbFile, (db) => {
		const epoch = ownerEpoch ?? currentEpoch(db);
		db.prepare("INSERT INTO tasks (task_id, title, status, current_run_id, lifecycle_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
			.run(taskId, "任务 " + taskId, status, runId, lifecycle, NOW, NOW);
		if (runId) {
			db.prepare("INSERT INTO runs (run_id, task_id, status, created_at, updated_at, root_run_id, subworkflow_depth, workflow_id, workflow_version, workflow_definition_hash, started_at, ended_at) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)")
				.run(runId, taskId, runStatus, NOW, NOW, runId, workflow ? "wf_board" : null, workflow ? 1 : null, workflow ? "definition-hash" : null, NOW, runStatus === "running" ? null : NOW);
			db.prepare("INSERT INTO node_runs (node_run_id, run_id, node_id, seq, node_type, attempt, status, revision, created_at, updated_at, owner_epoch, action_generation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
				.run("n_" + taskId, runId, "work", 1, "agent", 1, nodeStatus, 0, NOW, NOW, epoch, 0);
		}
		return epoch;
	});
}

function seedReviewReport(dbFile, {
	taskId, runId, reviewId = "rv_det", reviewStatus = "pass", createdAt = NOW
} = {}) {
	const report = {
		schema_version: 1,
		review_id: reviewId,
		task_id: taskId,
		run_id: runId,
		criteria_version: 1,
		reviewer: "independent-review-agent:v1",
		reviewer_kind: "independent_agent",
		reviewStatus,
		score: reviewStatus === "pass" ? 92 : 10,
		passedCriteria: [],
		failedCriteria: [],
		missingEvidence: [],
		securityIssues: [],
		resultKinds: {},
		checks: { aiReview: "disabled" },
		llm: { configured: false, model: null },
		created_at: createdAt
	};
	withDb(dbFile, (db) => {
		db.prepare("INSERT INTO review_reports (review_id, task_id, run_id, criteria_version, reviewer, reviewer_kind, review_status, score, report_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			.run(reviewId, taskId, runId, 1, report.reviewer, report.reviewer_kind, reviewStatus, report.score, JSON.stringify(report), createdAt);
	});
	return report;
}

test("集成：创建编排板、任务、依赖、总览与时间线", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());

	const definition = {
		schema_version: "1",
		workflow_id: "wf_board",
		name: "测试编排板",
		scope: "reusable",
		nodes: [
			{ node_id: "start", type: "trigger", title: "开始", position: { x: 0, y: 0 } },
			{ node_id: "work", type: "agent", title: "执行", position: { x: 200, y: 0 }, config: { goal: "完成测试" } }
		],
		edges: [{ from: "start", to: "work" }]
	};
	const created = await api(service, "/v1/workflows", { method: "POST", body: { workflow_id: "wf_board", name: "测试编排板", definition, operation_id: operation(1) } });
	assert.equal(created.status, 200);
	assert.equal(created.json.data.validation.errors.length, 0);

	const tested = await api(service, "/v1/workflows/test", { method: "POST", body: { workflow_id: "wf_board", revision: 1 } });
	assert.equal(tested.status, 200);
	assert.equal(tested.json.data.passed, true);
	const published = await api(service, "/v1/workflows/publish", { method: "POST", body: { workflow_id: "wf_board", revision: 1, base_version: 0, publish_note: "集成测试", operation_id: operation(2) } });
	assert.equal(published.status, 200);

	const overview = await api(service, "/v1/workspace/overview");
	assert.equal(overview.status, 200);
	assert.equal(overview.json.data.metrics.boards, 1);
	assert.equal(overview.json.data.metrics.tasks_total, 0);

	const reserved = await api(service, "/v1/tasks/reserve", { method: "POST", body: { title: "子任务", operation_id: operation(3) } });
	assert.equal(reserved.status, 200);
	const taskId = reserved.json.data.task_id;

	const detail = await api(service, `/v1/tasks/get?task_id=${taskId}`);
	assert.equal(detail.status, 200);
	assert.deepEqual(detail.json.data.dependencies, []);
	assert.deepEqual(detail.json.data.dependents, []);

	const timeline = await api(service, `/v1/tasks/timeline?task_id=${taskId}`);
	assert.equal(timeline.status, 200);
	assert.equal(timeline.json.data.events[0].kind, "created");

	const afterReserve = await api(service, "/v1/workspace/overview");
	assert.equal(afterReserve.json.data.metrics.tasks_total, 1);
	assert.equal(afterReserve.json.data.metrics.tasks_pending_acceptance, 0);

	const enqueued = await api(service, "/v1/scheduler/jobs", { method: "POST", body: { taskId, workflowId: "wf_board", workflowVersion: "1", nodeIds: ["work"] } });
	assert.equal(enqueued.status, 200);
	const ticked = await api(service, "/v1/scheduler/tick", { method: "POST", body: {} });
	assert.equal(ticked.status, 200);
	const start = ticked.json.data.executed.find((item) => item.type === "start");
	assert.ok(start, "调度应创建运行");
	const run = await api(service, `/v1/runs/get?run_id=${start.run_id}`);
	assert.equal(run.status, 200);
	assert.equal(run.json.data.task_id, taskId);
	const nodes = await api(service, `/v1/node-runs?run_id=${start.run_id}`);
	assert.equal(nodes.status, 200);
	assert.equal(nodes.json.data.items.length, 1);
	assert.equal(nodes.json.data.items[0].node_type, "agent");
	const dispatchedDetail = await api(service, `/v1/tasks/get?task_id=${taskId}`);
	assert.equal(dispatchedDetail.json.data.current_run_id, start.run_id);
	assert.equal(dispatchedDetail.json.data.run_count, 1);
});

test("集成：任务入队、依赖阻塞、重复派发被拒绝、取消", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());

	const parent = (await api(service, "/v1/tasks/reserve", { method: "POST", body: { title: "前置", operation_id: operation(11) } })).json.data.task_id;
	const child = (await api(service, "/v1/tasks/reserve", { method: "POST", body: { title: "后续", operation_id: operation(12) } })).json.data.task_id;
	const enqueueBody = (taskId, extra = {}) => ({ taskId, workflowId: "wf_board", workflowVersion: "1", nodeIds: ["work"], ...extra });

	const parentJob = await api(service, "/v1/scheduler/jobs", { method: "POST", body: enqueueBody(parent, { priority: 5 }) });
	assert.equal(parentJob.status, 200);
	assert.equal(parentJob.json.data.job.status, "queued");
	const duplicate = await api(service, "/v1/scheduler/jobs", { method: "POST", body: enqueueBody(parent, { priority: 5 }) });
	assert.equal(duplicate.status, 409);
	assert.equal(duplicate.json.error.code, "STALE_GENERATION");

	const childJob = await api(service, "/v1/scheduler/jobs", { method: "POST", body: enqueueBody(child, { dependsOn: [parent] }) });
	assert.equal(childJob.status, 200);

	const snapshot = await api(service, "/v1/scheduler");
	assert.equal(snapshot.status, 200);
	assert.equal(snapshot.json.data.jobs.length, 2);
	const childDetail = await api(service, `/v1/tasks/get?task_id=${child}`);
	assert.deepEqual(childDetail.json.data.dependencies, [parent]);

	const cancelled = await api(service, "/v1/scheduler/jobs/cancel", { method: "POST", body: { taskIds: [child], reason: "集成测试取消" } });
	assert.equal(cancelled.status, 200);
	assert.equal(cancelled.json.data.cancelled.length, 1);
	const after = await api(service, "/v1/scheduler");
	assert.equal(after.json.data.jobs.find((job) => job.job_id === childJob.json.data.job.job_id).status, "cancelled");
});

test("集成：人工验收驳回、禁止改判、驳回后重做", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());
	seedTask(join(dir, "service.sqlite"), { taskId: "t_review", lifecycle: "review", status: "succeeded", runId: "r_review", runStatus: "succeeded", nodeStatus: "done" });

	const noReason = await api(service, "/v1/tasks/acceptance/reject", { method: "POST", body: { taskId: "t_review" } });
	assert.equal(noReason.status, 400);
	assert.equal(noReason.json.error.code, "PAYLOAD_SCHEMA_INVALID");

	const rejected = await api(service, "/v1/tasks/acceptance/reject", { method: "POST", body: { taskId: "t_review", reason: "交付物不完整" } });
	assert.equal(rejected.status, 200);
	assert.equal(rejected.json.data.status, "rejected");
	const rejectedDetail = await api(service, "/v1/tasks/get?task_id=t_review");
	assert.equal(rejectedDetail.json.data.lifecycle_state, "rejected");

	const approve = await api(service, "/v1/tasks/acceptance/approve", { method: "POST", body: { taskId: "t_review", reason: "改判" } });
	assert.equal(approve.status, 409);
	assert.equal(approve.json.error.code, "ACCEPTANCE_NOT_READY");

	const retried = await api(service, "/v1/tasks/retry", { method: "POST", body: { taskId: "t_review", mode: "manual", explicit: true, reason: "按驳回意见返工" } });
	assert.equal(retried.status, 200);
	assert.equal(retried.json.data.retry_count, 1);
	const retriedDetail = await api(service, "/v1/tasks/get?task_id=t_review");
	assert.equal(retriedDetail.json.data.lifecycle_state, "queued");
	assert.notEqual(retriedDetail.json.data.current_run_id, "r_review");

	const timeline = await api(service, "/v1/tasks/timeline?task_id=t_review");
	assert.ok(timeline.json.data.events.some((event) => event.title === "review → rejected"));
	assert.ok(timeline.json.data.events.some((event) => event.kind === "retry"));
});

test("集成：LLM Review 未配置时公开不可用并拒绝触发", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());
	seedTask(join(dir, "service.sqlite"), { taskId: "t_llm_off", lifecycle: "review", status: "succeeded", runId: "r_llm_off", runStatus: "succeeded", nodeStatus: "done" });
	seedReviewReport(join(dir, "service.sqlite"), { taskId: "t_llm_off", runId: "r_llm_off" });

	const acceptance = await api(service, "/v1/tasks/acceptance?task_id=t_llm_off");
	assert.equal(acceptance.status, 200);
	assert.equal(acceptance.json.data.review.available, false);
	assert.equal(acceptance.json.data.review.reason, "llm_review_disabled");

	const triggered = await api(service, "/v1/tasks/acceptance/review", { method: "POST", body: { taskId: "t_llm_off" } });
	assert.equal(triggered.status, 503);
	assert.equal(triggered.json.error.code, "SERVICE_UNAVAILABLE");
	withDb(join(dir, "service.sqlite"), (db) => {
		assert.equal(db.prepare("SELECT COUNT(*) AS n FROM review_reports WHERE task_id = 't_llm_off'").get().n, 1);
	});
});

test("集成：人工触发 LLM Review 记录结果且不能放宽确定性结论", async (t) => {
	const dir = tempDir();
	let calls = 0;
	const service = await serveOrchestra(options(dir, {
		reviewLlm: {
			enabled: true,
			provider: "test-provider",
			model: "test-reviewer",
			timeoutMs: 5000,
			async complete({ system, user, model }) {
				calls += 1;
				assert.equal(model, "test-reviewer");
				assert.ok(system.includes("只读证据"));
				const evidence = JSON.parse(user);
				assert.equal(evidence.constraints.read_only, true);
				assert.equal(evidence.constraints.workspace_mounted, false);
				return JSON.stringify({
					correctness: { status: "pass", summary: "看起来正确" },
					security: { status: "pass" },
					score: 100
				});
			}
		}
	}));
	t.after(async () => service.close());
	seedTask(join(dir, "service.sqlite"), { taskId: "t_llm_on", lifecycle: "review", status: "succeeded", runId: "r_llm_on", runStatus: "succeeded", nodeStatus: "done" });
	seedReviewReport(join(dir, "service.sqlite"), { taskId: "t_llm_on", runId: "r_llm_on", reviewStatus: "fail" });

	const acceptance = await api(service, "/v1/tasks/acceptance?task_id=t_llm_on");
	assert.equal(acceptance.json.data.review.available, true);
	assert.equal(acceptance.json.data.review.provider, "test-provider");
	assert.equal(acceptance.json.data.review.model, "test-reviewer");

	const triggered = await api(service, "/v1/tasks/acceptance/review", { method: "POST", body: { taskId: "t_llm_on" } });
	assert.equal(triggered.status, 200);
	assert.equal(triggered.json.data.available, true);
	assert.equal(triggered.json.data.model, "test-reviewer");
	// 模型给出满分也不能把确定性 fail 改为 pass。
	assert.equal(triggered.json.data.reviewStatus, "fail");
	assert.equal(triggered.json.data.reviewReport.approval_eligible, false);
	assert.equal(triggered.json.data.reviewReport.llm.model, "test-reviewer");
	assert.equal(triggered.json.data.reviewReport.trigger.requested_by, HUMAN.id);
	assert.equal(triggered.json.data.reviewReport.parent_review_id, "rv_det");
	assert.equal(calls, 1);

	const after = await api(service, "/v1/tasks/acceptance?task_id=t_llm_on");
	assert.equal(after.json.data.reviewReport.review_id, triggered.json.data.review_id);
	assert.equal(after.json.data.reviewReport.llmReview.available, true);
	assert.equal(after.json.data.reviewReport.llm.configured, true);
	withDb(join(dir, "service.sqlite"), (db) => {
		assert.equal(db.prepare("SELECT COUNT(*) AS n FROM review_reports WHERE task_id = 't_llm_on'").get().n, 2);
	});
});

test("集成：任务取消与未授权操作", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());
	seedTask(join(dir, "service.sqlite"), { taskId: "t_cancel", lifecycle: "draft", status: "reserved" });
	seedTask(join(dir, "service.sqlite"), { taskId: "t_passed", lifecycle: "passed", status: "succeeded", runId: "r_passed", runStatus: "succeeded", nodeStatus: "done" });

	const unauthorized = await api(service, "/v1/tasks/cancel", { method: "POST", body: { taskId: "t_cancel", reason: "越权" }, secret: "wrong-secret" });
	assert.equal(unauthorized.status, 401);
	assert.equal(unauthorized.json.error.code, "UNAUTHORIZED");
	withDb(join(dir, "service.sqlite"), (db) => {
		assert.equal(db.prepare("SELECT lifecycle_state FROM tasks WHERE task_id = 't_cancel'").get().lifecycle_state, "draft");
	});

	const cancelled = await api(service, "/v1/tasks/cancel", { method: "POST", body: { taskId: "t_cancel", reason: "测试取消" } });
	assert.equal(cancelled.status, 200);
	assert.equal(cancelled.json.data.lifecycle_state, "cancelled");

	const terminal = await api(service, "/v1/tasks/cancel", { method: "POST", body: { taskId: "t_passed", reason: "不应允许" } });
	assert.equal(terminal.status, 403);
	assert.equal(terminal.json.error.code, "FORBIDDEN");
});

test("集成：DSH 重启后恢复中断任务并保留执行尝试", async () => {
	const dir = tempDir();
	const first = await serveOrchestra(options(dir));
	seedTask(join(dir, "service.sqlite"), { taskId: "t_restart", lifecycle: "running", status: "running", runId: "r_restart", runStatus: "running", nodeStatus: "started" });
	await first.close();

	const second = await serveOrchestra(options(dir));
	try {
		const detail = await api(second, "/v1/tasks/get?task_id=t_restart");
		assert.equal(detail.status, 200);
		assert.equal(detail.json.data.lifecycle_state, "interrupted");
		assert.notEqual(detail.json.data.lifecycle_state, "draft");
		assert.equal(detail.json.data.last_attempt_id, "n_t_restart:1");
		withDb(join(dir, "service.sqlite"), (db) => {
			assert.equal(db.prepare("SELECT COUNT(*) AS n FROM task_attempts WHERE task_id = 't_restart'").get().n, 1);
			assert.equal(db.prepare("SELECT lifecycle_state FROM tasks WHERE task_id = 't_restart'").get().lifecycle_state, "interrupted");
		});
	} finally {
		await second.close();
	}
});

test("集成：批量派发与批量取消逐项汇总，单项失败不中断", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());
	seedTask(join(dir, "service.sqlite"), { taskId: "t_batch_a", lifecycle: "rejected", status: "failed", runId: "r_batch_a", runStatus: "failed", nodeStatus: "failed" });
	seedTask(join(dir, "service.sqlite"), { taskId: "t_batch_b", lifecycle: "rejected", status: "failed", runId: "r_batch_b", runStatus: "failed", nodeStatus: "failed" });

	const dispatched = await api(service, "/v1/tasks/batch", { method: "POST", body: { action: "dispatch", taskIds: ["t_batch_a", "t_batch_b", "t_missing"], reason: "批量返工" } });
	assert.equal(dispatched.status, 200);
	assert.equal(dispatched.json.data.applied, 2);
	assert.equal(dispatched.json.data.failed, 1);
	assert.equal(dispatched.json.data.items.find((item) => item.task_id === "t_missing").error.code, "NOT_FOUND");
	const newRuns = dispatched.json.data.items.filter((item) => item.ok).map((item) => item.result.run_id);
	assert.equal(new Set(newRuns).size, 2);

	const cancelled = await api(service, "/v1/tasks/batch", { method: "POST", body: { action: "cancel", taskIds: ["t_batch_a", "t_batch_b"], reason: "批量取消" } });
	assert.equal(cancelled.status, 200);
	assert.equal(cancelled.json.data.applied, 2);
	assert.equal(cancelled.json.data.failed, 0);
	withDb(join(dir, "service.sqlite"), (db) => {
		assert.deepEqual(db.prepare("SELECT lifecycle_state FROM tasks WHERE task_id IN ('t_batch_a','t_batch_b') ORDER BY task_id").all().map((row) => row.lifecycle_state), ["cancelled", "cancelled"]);
	});
});

test("安全：本机绑定、认证、路径穿越、敏感信息与 XSS 输出", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());
	assert.match(service.endpoint, /^http:\/\/127\.0\.0\.1:\d+$/);

	const anonymous = await api(service, "/v1/tasks/get?task_id=x", { secret: "" });
	assert.equal(anonymous.status, 401);
	assert.equal(anonymous.json.error.code, "UNAUTHORIZED");

	const wrong = await api(service, "/v1/tasks/get?task_id=x", { secret: "another-secret" });
	assert.equal(wrong.status, 401);
	assert.ok(!wrong.text.includes(SECRET));
	assert.ok(!/at\s+\w+\s+\(/u.test(wrong.text), "错误响应不应包含堆栈");
	assert.ok(!wrong.text.includes(join(dir, "service.sqlite")));

	const traversal = await api(service, "/v1/tasks/get?task_id=../../../../etc/passwd");
	assert.equal(traversal.status, 404);
	assert.equal(traversal.json.error.code, "NOT_FOUND");
	assert.ok(!traversal.text.includes("root:x:0:0"));

	const xssTitle = '<img src=x onerror="alert(1)">';
	const reserved = await api(service, "/v1/tasks/reserve", { method: "POST", body: { title: xssTitle, operation_id: operation(21) } });
	assert.equal(reserved.status, 200);
	const detail = await api(service, `/v1/tasks/get?task_id=${reserved.json.data.task_id}`);
	assert.match(detail.contentType, /^application\/json/u);
	assert.equal(detail.json.data.title, xssTitle);
	const client = readFileSync(join(ROOT, "lib", "client.js"), "utf8");
	assert.ok(!client.includes("dangerouslySetInnerHTML"), "面板不得直接注入未转义 HTML");
});

test("单元/边界：任务结果上报先解析结构，再按当前尝试裁决", async (t) => {
	const dir = tempDir();
	const service = await serveOrchestra(options(dir));
	t.after(async () => service.close());
	seedTask(join(dir, "service.sqlite"), { taskId: "t_report", lifecycle: "running", status: "running", runId: "r_report", runStatus: "running", nodeStatus: "started" });
	const nodeRunId = "n_t_report";

	const malformed = await api(service, "/v1/node-runs/report", {
		method: "POST",
		body: { nodeRunId, attempt: 1, expectedStatus: "started", generation: 0, report: {} }
	});
	assert.equal(malformed.status, 400);
	assert.equal(malformed.json.error.code, "PAYLOAD_SCHEMA_INVALID");
	assert.ok(malformed.json.error.details.issues.some((issue) => issue.path === "report.report_id"));

	const report = {
		report_id: `rep_${nodeRunId}_1_succeeded`,
		node_run_id: nodeRunId,
		attempt: 1,
		status: "succeeded",
		evidence: { source: "executor", exitCode: 0 },
		output: { ok: true }
	};
	const parsed = await api(service, "/v1/node-runs/report", { method: "POST", body: { nodeRunId, attempt: 1, expectedStatus: "started", generation: 0, report } });
	assert.equal(parsed.status, 200);
	assert.equal(parsed.json.data.accepted, false);
	assert.equal(parsed.json.data.reason, "illegal_transition");
	assert.equal(parsed.json.data.registry_state, "rejected");

	const replay = await api(service, "/v1/node-runs/report", { method: "POST", body: { nodeRunId, attempt: 1, expectedStatus: "started", generation: 0, report } });
	assert.equal(replay.status, 200);
	assert.equal(replay.json.data.accepted, false);
	withDb(join(dir, "service.sqlite"), (db) => {
		assert.equal(db.prepare("SELECT COUNT(*) AS n FROM report_registry WHERE node_run_id = ?").get(nodeRunId).n, 1);
	});
});
