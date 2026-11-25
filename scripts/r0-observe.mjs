// R0 观察 harness：从候选包启动 S6 试用圈，按固定间隔采集真实时间、单调计时、
// 账本水位与合成工作区摘要；支持短时 dry-run、状态、取消与离线核验。
import { spawn, spawnSync } from "node:child_process";
import { createHash, createPrivateKey, createPublicKey, generateKeyPairSync, randomBytes, sign, verify } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync, statfsSync, statSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SCRIPT = fileURLToPath(import.meta.url);
const DEFAULT_DURATION_MS = 72 * 60 * 60 * 1000;
const DEFAULT_SAMPLE_MS = 60 * 1000;
const MIN_SAMPLE_MS = 200;
const MAX_SAMPLE_MS = 60 * 60 * 1000;
const START_TIMEOUT_MS = 20_000;

const FILES = Object.freeze({
	launch: "launch.json",
	config: "soak-config.json",
	worker: "worker.json",
	samples: "samples.ndjson",
	end: "end.json",
	ledger: "ledger.db",
	workspace: "workspace",
	sample: "workspace/sample.txt",
	report: "report.json"
});
const MIN_SOAK_MS = 72 * 60 * 60 * 1000;
const UNFINISHED_STATES = Object.freeze(["draft", "blocked", "queued", "dispatching", "running", "review", "retrying"]);

function canonicalJson(value) {
	if (value === null) return "null";
	if (typeof value === "number") return JSON.stringify(value);
	if (typeof value === "boolean" || typeof value === "string") return JSON.stringify(value);
	if (Array.isArray(value)) return "[" + value.map(canonicalJson).join(",") + "]";
	if (typeof value === "object") {
		return "{" + Object.entries(value).filter(([, item]) => item !== undefined).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, item]) => JSON.stringify(key) + ":" + canonicalJson(item)).join(",") + "}";
	}
	return JSON.stringify(String(value));
}

function fail(message) {
	throw new Error(message);
}

function sha256(data) {
	return createHash("sha256").update(data).digest("hex");
}

function sha256File(path) {
	return sha256(readFileSync(path));
}

function readJson(path) {
	return JSON.parse(readFileSync(path, "utf8"));
}

function writeJson(path, value) {
	writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function sleep(ms) {
	return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function parseFlags(args) {
	const positional = [];
	const flags = {};
	for (let index = 0; index < args.length; index += 1) {
		const arg = args[index];
		if (!arg.startsWith("--")) {
			positional.push(arg);
			continue;
		}
		const name = arg.slice(2);
		const next = args[index + 1];
		if (next === undefined || next.startsWith("--")) flags[name] = true;
		else {
			flags[name] = next;
			index += 1;
		}
	}
	return { positional, flags };
}

function positiveNumber(value, name) {
	const number = Number(value);
	if (!Number.isFinite(number) || number <= 0) fail(`${name} 必须是正数`);
	return number;
}

function parseWindow(flags) {
	let durationMs = DEFAULT_DURATION_MS;
	if (flags.seconds !== undefined) durationMs = Math.round(positiveNumber(flags.seconds, "--seconds") * 1000);
	else if (flags.minutes !== undefined) durationMs = Math.round(positiveNumber(flags.minutes, "--minutes") * 60 * 1000);
	let sampleMs = DEFAULT_SAMPLE_MS;
	if (flags["sample-seconds"] !== undefined) sampleMs = Math.round(positiveNumber(flags["sample-seconds"], "--sample-seconds") * 1000);
	if (sampleMs < MIN_SAMPLE_MS || sampleMs > MAX_SAMPLE_MS) fail(`--sample-seconds 需在 ${MIN_SAMPLE_MS / 1000} 到 ${MAX_SAMPLE_MS / 1000} 之间`);
	if (durationMs < sampleMs) fail("观察时长不能短于采样间隔");
	return {
		durationMs,
		sampleMs,
		mode: flags["dry-run"] ? "dry-run" : durationMs < 24 * 60 * 60 * 1000 ? "dry-run" : "soak"
	};
}

function assertNewDir(dir) {
	const absolute = resolve(dir);
	if (existsSync(absolute)) fail(`观察目录必须尚不存在：${absolute}`);
	return absolute;
}

function assertCandidate(commit) {
	if (!/^[0-9a-f]{40}$/u.test(commit)) fail("候选必须是 40 位小写十六进制完整提交");
}

function assertTarball(path, expectedSha) {
	const absolute = resolve(path);
	if (!existsSync(absolute)) fail(`候选包不存在：${absolute}`);
	if (!/^[0-9a-f]{64}$/u.test(expectedSha)) fail("包 SHA256 必须是 64 位小写十六进制");
	const actual = sha256File(absolute);
	if (actual !== expectedSha) fail(`包 SHA256 不匹配：${actual} != ${expectedSha}`);
	return { absolute, actual };
}

function assertSafeTar(tarball) {
	const listing = spawnSync("tar", ["-tzf", tarball], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
	if (listing.status !== 0) fail(`无法读取候选包清单：${String(listing.stderr).trim()}`);
	const entries = listing.stdout.split("\n").filter(Boolean);
	if (entries.length === 0) fail("候选包为空");
	for (const entry of entries) {
		if (entry.startsWith("/") || entry.split("/").includes("..")) fail(`候选包含不安全路径：${entry}`);
	}
	return entries;
}

function extractPackage(tarball, destination) {
	mkdirSync(destination, { recursive: true });
	const result = spawnSync("tar", ["-xzf", tarball, "-C", destination], { encoding: "utf8" });
	if (result.status !== 0) fail(`解包失败：${String(result.stderr).trim()}`);
	const entry = join(destination, "package", "lib", "index.js");
	if (!existsSync(entry)) fail("候选包缺少 package/lib/index.js");
	return entry;
}

function createSyntheticWorkspace(dir) {
	const workspace = join(dir, FILES.workspace);
	mkdirSync(workspace, { recursive: true });
	const sample = join(workspace, "sample.txt");
	writeFileSync(sample, "R0 synthetic read-only sample\n");
	chmodSync(sample, 0o444);
	return { workspace, sample, sha256: sha256File(sample) };
}

function sourceState() {
	const head = spawnSync("git", ["rev-parse", "HEAD"], { cwd: ROOT, encoding: "utf8" }).stdout.trim();
	const status = spawnSync("git", ["status", "--porcelain"], { cwd: ROOT, encoding: "utf8" }).stdout.trim();
	return { head, dirty: status.length > 0 };
}

function appendSample(path, sample) {
	writeFileSync(path, `${JSON.stringify(sample)}\n`, { flag: "a" });
}

function readSamples(dir) {
	const path = join(dir, FILES.samples);
	if (!existsSync(path)) return [];
	return readFileSync(path, "utf8")
		.split("\n")
		.filter((line) => line.trim().length > 0)
		.map((line, index) => {
			try {
				return JSON.parse(line);
			} catch (error) {
				fail(`第 ${index + 1} 条采样不是合法 JSON：${error.message}`);
			}
		});
}

function readLedger(file) {
	const db = new DatabaseSync(file, { readOnly: true });
	try {
		const migration = db.prepare("SELECT COUNT(*) AS count, COALESCE(MAX(version), 0) AS version FROM schema_migrations").get();
		const event = db.prepare("SELECT COUNT(*) AS count, COALESCE(MAX(seq), 0) AS seq FROM events").get();
		return { schema_version: Number(migration.version), migrations: Number(migration.count), event_seq: Number(event.seq), event_count: Number(event.count) };
	} finally {
		db.close();
	}
}

function assertLedgerFile(dir) {
	const file = join(dir, FILES.ledger);
	if (!existsSync(file)) fail("缺少 ledger.db");
	return file;
}

/** 任务恢复与未完成任务是 R0 观察的必录项，不是可选项。 */
function readTaskSnapshot(file) {
	const db = new DatabaseSync(file, { readOnly: true });
	try {
		const hasTasks = db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = 'tasks'").get();
		if (!hasTasks) return { counts: {}, unfinished: [] };
		const counts = {};
		for (const row of db.prepare("SELECT lifecycle_state AS state, COUNT(*) AS count FROM tasks GROUP BY lifecycle_state").all()) {
			counts[row.state ?? "unknown"] = Number(row.count);
		}
		const placeholders = UNFINISHED_STATES.map(() => "?").join(",");
		const unfinished = db.prepare(`SELECT task_id, lifecycle_state, status FROM tasks WHERE lifecycle_state IN (${placeholders}) ORDER BY updated_at DESC LIMIT 20`).all(...UNFINISHED_STATES).map((row) => ({
			task_id: row.task_id,
			lifecycle_state: row.lifecycle_state,
			status: row.status
		}));
		return { counts, unfinished };
	} finally {
		db.close();
	}
}

function diskSnapshot(dir) {
	try {
		const info = statfsSync(dir);
		return {
			block_size: Number(info.bsize),
			blocks: Number(info.blocks),
			blocks_free: Number(info.bfree),
			blocks_available: Number(info.bavail)
		};
	} catch {
		return null;
	}
}

function resourceSnapshot(cpuStart) {
	const usage = process.cpuUsage(cpuStart);
	const memory = process.memoryUsage();
	return {
		pid: process.pid,
		rss_bytes: memory.rss,
		heap_used_bytes: memory.heapUsed,
		heap_total_bytes: memory.heapTotal,
		external_bytes: memory.external,
		cpu_user_ms: Math.round(usage.user / 1000),
		cpu_system_ms: Math.round(usage.system / 1000),
		uptime_s: Math.round(process.uptime())
	};
}

function observationSummary(dir) {
	const launch = readJson(join(dir, FILES.launch));
	const config = readJson(join(dir, FILES.config));
	const worker = readJson(join(dir, FILES.worker));
	const end = readJson(join(dir, FILES.end));
	const samples = readSamples(dir);
	return { launch, config, worker, end, samples };
}

function assertAlive(pid) {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		if (error.code === "ESRCH") return false;
		throw error;
	}
}

async function launch(dir, commit, tarball, expectedSha, window) {
	assertCandidate(commit);
	const target = assertNewDir(dir);
	const { absolute, actual } = assertTarball(tarball, expectedSha);
	const entries = assertSafeTar(absolute);
	if (!entries.includes("package/package.json")) fail("候选包缺少 package/package.json");
	mkdirSync(target, { recursive: true });
	const entry = extractPackage(absolute, target);
	const pkg = readJson(join(target, "package", "package.json"));
	const workspace = createSyntheticWorkspace(target);
	const startedAt = new Date().toISOString();
	const config = {
		schema_version: 1,
		candidate: commit,
		duration_ms: window.durationMs,
		sample_interval_ms: window.sampleMs,
		mode: window.mode,
		read_only: true,
		ledger: FILES.ledger,
		workspace: FILES.workspace
	};
	const launchRecord = {
		schema_version: 1,
		candidate: commit,
		tarball: absolute,
		tarball_sha256: actual,
		name: pkg.name,
		version: pkg.version,
		package_entry: entry,
		workspace: workspace.workspace,
		workspace_sha256: workspace.sha256,
		started_at: startedAt,
		mode: window.mode,
		duration_ms: window.durationMs,
		sample_interval_ms: window.sampleMs,
		source: sourceState(),
		node: process.version
	};
	writeJson(join(target, FILES.config), config);
	writeJson(join(target, FILES.launch), launchRecord);
	const child = spawn(process.execPath, ["--no-warnings", SCRIPT, "__worker", target], { detached: true, stdio: "ignore" });
	child.unref();
	const workerPath = join(target, FILES.worker);
	const deadline = Date.now() + START_TIMEOUT_MS;
	while (!existsSync(workerPath) && Date.now() < deadline) await sleep(100);
	if (!existsSync(workerPath)) {
		try {
			process.kill(child.pid, "SIGTERM");
		} catch {}
		fail("观察 worker 未在时限内启动，已保留目录并终止进程");
	}
	const bootDeadline = Date.now() + START_TIMEOUT_MS;
	while (Date.now() < bootDeadline) {
		const endPath = join(target, FILES.end);
		if (existsSync(endPath)) fail(`观察 worker 启动失败：${readJson(endPath).reason}`);
		if (readSamples(target).length > 0) break;
		await sleep(100);
	}
	if (readSamples(target).length === 0) {
		try {
			process.kill(child.pid, "SIGTERM");
		} catch {}
		fail("观察 worker 未产生首个采样，已保留目录并终止进程");
	}
	const worker = readJson(workerPath);
	console.log(`R0 ${window.mode} 已启动：${target}`);
	console.log(`候选 ${commit.slice(0, 12)}，包 ${pkg.name}@${pkg.version}`);
	console.log(`时长 ${window.durationMs} ms，采样间隔 ${window.sampleMs} ms，worker pid ${worker.pid}`);
	return { dir: target, worker };
}

function status(dir) {
	const target = resolve(dir);
	if (!existsSync(target)) fail(`观察目录不存在：${target}`);
	const launch = readJson(join(target, FILES.launch));
	const worker = readJson(join(target, FILES.worker));
	const samples = readSamples(target);
	const endPath = join(target, FILES.end);
	const end = existsSync(endPath) ? readJson(endPath) : null;
	const running = !end && assertAlive(worker.pid);
	const last = samples.at(-1) ?? null;
	console.log(`观察目录：${target}`);
	console.log(`模式：${launch.mode}，候选：${launch.candidate.slice(0, 12)}`);
	console.log(`进程：${running ? `运行中（pid ${worker.pid}）` : end ? `已结束（${end.status}）` : "未运行"}`);
	console.log(`采样：${samples.length} 条${last ? `，最后 ${last.at}` : ""}`);
	if (last) console.log(`账本：schema ${last.ledger.schema_version}，事件水位 ${last.ledger.event_seq}`);
	return { launch, worker, samples, end, running };
}

async function cancel(dir) {
	const target = resolve(dir);
	if (!existsSync(target)) fail(`观察目录不存在：${target}`);
	const workerPath = join(target, FILES.worker);
	if (!existsSync(workerPath)) fail("缺少 worker.json，无法取消");
	const worker = readJson(workerPath);
	if (assertAlive(worker.pid)) {
		process.kill(worker.pid, "SIGTERM");
		const deadline = Date.now() + 10_000;
		while (assertAlive(worker.pid) && Date.now() < deadline) await sleep(100);
	}
	const endPath = join(target, FILES.end);
	if (!existsSync(endPath)) {
		const samples = readSamples(target);
		writeJson(endPath, {
			status: "cancelled",
			reason: "cancel requested",
			started_at: worker.started_at,
			ended_at: new Date().toISOString(),
			samples: samples.length
		});
	}
	console.log(`已取消并保留现场：${target}`);
}

function verifyObservation(dir, { requireSoak = false } = {}) {
	const target = resolve(dir);
	for (const name of [FILES.launch, FILES.config, FILES.worker, FILES.samples, FILES.end, FILES.ledger, FILES.sample]) {
		if (!existsSync(join(target, name))) fail(`缺少 ${name}`);
	}
	const { launch, config, worker, end, samples } = observationSummary(target);
	if (end.status !== "completed" && end.status !== "cancelled") fail(`观察未正常结束：${end.status}`);
	if (samples.length === 0) fail("没有采样记录");
	const startedMs = Date.parse(worker.sampling_started_at ?? worker.started_at);
	const endedMs = Date.parse(end.ended_at);
	if (!Number.isFinite(startedMs) || !Number.isFinite(endedMs) || endedMs <= startedMs) fail("观察起止时间不合法");
	const interval = config.sample_interval_ms;
	const maxGap = Math.max(interval * 3, 5_000);
	const expected = Math.floor((endedMs - startedMs) / interval);
	const requiredSamples = end.status === "completed" ? Math.max(1, expected) : 1;
	if (samples.length < requiredSamples) fail(`采样不足：${samples.length} < ${requiredSamples}`);
	const workspaceSha = sha256File(join(target, FILES.sample));
	const sampleMode = statSync(join(target, FILES.sample)).mode;
	if (process.platform !== "win32" && (sampleMode & 0o222) !== 0) fail("合成工作区样本不是只读");
	let previousAt = 0;
	let previousMono = -1;
	let previousSeq = -1;
	let previousSchema = -1;
	samples.forEach((sample, index) => {
		if (sample.slot !== index) fail(`采样槽不连续：第 ${index + 1} 条为 ${sample.slot}`);
		const at = Date.parse(sample.at);
		if (!Number.isFinite(at)) fail(`采样时间不合法：${sample.at}`);
		if (at <= previousAt) fail(`采样时间未递增：${sample.at}`);
		if (!Number.isInteger(sample.monotonic_ms) || sample.monotonic_ms <= previousMono) fail(`单调计时未递增：${sample.monotonic_ms}`);
		if (at - previousAt > maxGap && index > 0) fail(`采样间隔超限：${at - previousAt} ms`);
		if (sample.workspace_sha256 !== workspaceSha) fail("合成工作区内容发生变化");
		if (sample.trial?.candidate !== launch.candidate) fail("采样与候选不一致");
		if (sample.trial?.circle !== "R0") fail(`采样离开 R0：${sample.trial?.circle}`);
		if (sample.ledger?.event_seq < previousSeq) fail("账本事件水位回退");
		if (sample.ledger?.schema_version < previousSchema) fail("schema 版本回退");
		previousAt = at;
		previousMono = sample.monotonic_ms;
		previousSeq = sample.ledger.event_seq;
		previousSchema = sample.ledger.schema_version;
	});
	const last = samples.at(-1);
	const ledger = readLedger(join(target, FILES.ledger));
	if (ledger.schema_version !== last.ledger.schema_version) fail("离线账本 schema 与采样不一致");
	if (ledger.event_seq < last.ledger.event_seq) fail("离线账本事件水位低于采样");
	if (end.samples !== undefined && end.samples !== samples.length) fail("end.json 采样数与文件不一致");
	const summary = {
		candidate: launch.candidate,
		version: launch.version,
		mode: launch.mode,
		status: end.status,
		samples: samples.length,
		expected_slots: expected,
		duration_ms: endedMs - startedMs,
		schema_version: ledger.schema_version,
		event_seq: ledger.event_seq,
		workspace_sha256: workspaceSha,
		exceptions: samples.reduce((total, sample) => total + Number(sample.exceptions?.count ?? 0), 0),
		unfinished_tasks: last.tasks?.unfinished ?? [],
		recovery: worker.recovery ?? null,
		last_resource: last.resource ?? null
	};
	if (requireSoak) {
		const result = soakChecks(target, summary);
		if (!result.ok) fail(`不满足 72 小时验收条件：${result.checks.filter((check) => !check.ok).map((check) => `${check.id} ${check.detail}`).join("; ")}`);
	}
	console.log("R0 观察核验通过");
	for (const [key, value] of Object.entries(summary)) {
		if (key === "unfinished_tasks" || key === "recovery" || key === "last_resource") continue;
		console.log(`${key}: ${value}`);
	}
	return summary;
}

/** 72 小时验收的硬条件：dry-run、取消、采样不足、异常、未记录恢复/资源/任务快照都不算通过。 */
function soakChecks(dir, summary) {
	const target = resolve(dir);
	const launch = readJson(join(target, FILES.launch));
	const config = readJson(join(target, FILES.config));
	const worker = readJson(join(target, FILES.worker));
	const end = readJson(join(target, FILES.end));
	const samples = readSamples(target);
	const checks = [];
	const add = (id, ok, detail) => checks.push({ id, ok: Boolean(ok), detail });
	add("candidate", /^[0-9a-f]{40}$/u.test(String(launch.candidate ?? "")), `候选 ${launch.candidate ?? "缺失"}`);
	add("soak_mode", launch.mode === "soak" && config.mode === "soak", `launch=${launch.mode} config=${config.mode}`);
	add("completed", end.status === "completed", `status=${end.status}`);
	add("duration", summary.duration_ms >= MIN_SOAK_MS && Number(config.duration_ms) >= MIN_SOAK_MS, `实际 ${summary.duration_ms} ms / 配置 ${config.duration_ms} ms`);
	add("samples", summary.samples >= summary.expected_slots && summary.expected_slots > 0, `${summary.samples} / ${summary.expected_slots}`);
	add("exceptions", summary.exceptions === 0, `${summary.exceptions} 次未处理异常`);
	add("recovery", worker.recovery?.attempted === true && worker.recovery?.ok === true, worker.recovery?.detail ?? "缺少任务恢复记录");
	add("task_snapshot", samples.every((sample) => sample.tasks && Array.isArray(sample.tasks.unfinished)) && end.tasks !== undefined, "未完成任务快照缺失");
	add("resource", samples.every((sample) => sample.resource && sample.disk), "资源占用快照缺失");
	add("ledger", summary.schema_version > 0 && summary.event_seq >= 0, `schema=${summary.schema_version} seq=${summary.event_seq}`);
	add("workspace", /^[0-9a-f]{64}$/u.test(String(summary.workspace_sha256 ?? "")), "工作区摘要缺失");
	return { ok: checks.every((check) => check.ok), checks };
}

/** 生成带 Ed25519 签名的验收报告；不满足 72 小时条件时签名但 accepted=false。 */
function signObservationReport(dir, { keyPath, outPath } = {}) {
	if (!keyPath) fail("生成验收报告需要 --key <Ed25519 私钥 PEM>");
	const privateKey = createPrivateKey(readFileSync(resolve(keyPath)));
	if (privateKey.asymmetricKeyType !== "ed25519") fail("签名私钥必须是 Ed25519");
	const target = resolve(dir);
	const checks = [];
	let summary = null;
	try {
		summary = verifyObservation(target);
		checks.push(...soakChecks(target, summary).checks);
	} catch (error) {
		checks.push({ id: "integrity", ok: false, detail: error?.message ?? String(error) });
	}
	const payload = {
		schema_version: 1,
		generated_at: new Date().toISOString(),
		observation: {
			dir: target,
			launch: readJson(join(target, FILES.launch)),
			end: readJson(join(target, FILES.end))
		},
		verification: {
			accepted: checks.length > 0 && checks.every((check) => check.ok),
			checks,
			summary
		},
		artifacts: {
			samples_sha256: sha256File(join(target, FILES.samples)),
			ledger_sha256: sha256File(join(target, FILES.ledger)),
			workspace_sha256: sha256File(join(target, FILES.sample))
		}
	};
	const publicKeyPem = createPublicKey(privateKey).export({ type: "spki", format: "pem" }).toString();
	const signature = sign(null, Buffer.from(canonicalJson(payload)), privateKey).toString("base64");
	const report = {
		...payload,
		signature: { algorithm: "ed25519", public_key_pem: publicKeyPem, value: signature }
	};
	const file = resolve(outPath ?? join(target, FILES.report));
	writeJson(file, report);
	console.log(`验收报告已写入：${file}`);
	console.log(`accepted: ${payload.verification.accepted}`);
	for (const check of checks) console.log(`${check.ok ? "PASS" : "FAIL"} ${check.id}: ${check.detail}`);
	return report;
}

/** 只校验签名与载荷一致性；是否通过验收由 payload.verification.accepted 决定。 */
function verifySignedReport(path) {
	const file = resolve(path);
	if (!existsSync(file)) fail(`报告不存在：${file}`);
	const report = readJson(file);
	const { signature, ...payload } = report;
	if (signature?.algorithm !== "ed25519" || typeof signature.value !== "string" || typeof signature.public_key_pem !== "string") fail("报告签名结构不完整");
	const ok = verify(null, Buffer.from(canonicalJson(payload)), createPublicKey(signature.public_key_pem), Buffer.from(signature.value, "base64"));
	if (!ok) fail("验收报告签名校验失败");
	return payload;
}

async function runWorker(dir) {
	const target = resolve(dir);
	const config = readJson(join(target, FILES.config));
	const launch = readJson(join(target, FILES.launch));
	const startedAt = new Date().toISOString();
	const monoStart = performance.now();
	writeJson(join(target, FILES.worker), {
		pid: process.pid,
		started_at: startedAt,
		node: process.version,
		package_entry: launch.package_entry
	});
	let stopping = false;
	const stop = () => {
		stopping = true;
	};
	process.on("SIGTERM", stop);
	process.on("SIGINT", stop);
	const exceptions = [];
	const recordException = (kind) => (error) => {
		exceptions.push({ kind, at: new Date().toISOString(), message: String(error?.message ?? error) });
	};
	process.on("uncaughtException", recordException("uncaught_exception"));
	process.on("unhandledRejection", recordException("unhandled_rejection"));
	let service = null;
	let samples = 0;
	let failure = null;
	const cpuStart = process.cpuUsage();
	try {
		const module = await import(pathToFileURL(join(target, "package", "lib", "index.js")).href);
		if (typeof module.openServiceOrchestra !== "function") fail("候选包未导出 openServiceOrchestra");
		service = await module.openServiceOrchestra({
			dbFile: join(target, FILES.ledger),
			secret: randomBytes(32).toString("hex"),
			phase: "S6",
			trial: {
				candidate: config.candidate,
				workspace: { id: "r0-synthetic", kind: "synthetic", root: join(target, FILES.workspace) },
				authorize: () => null,
				authenticateOperator: () => null
			}
		});
		let recovery;
		try {
			if (typeof service.orchestra.recoverTaskLifecycles !== "function") {
				recovery = { attempted: false, ok: false, detail: "候选包未提供任务恢复接口" };
			} else {
				const result = await service.orchestra.recoverTaskLifecycles();
				recovery = { attempted: true, ok: true, detail: "任务恢复已执行", result: result ?? null };
			}
		} catch (error) {
			recovery = { attempted: true, ok: false, detail: `任务恢复失败：${error?.message ?? error}` };
		}
		const state = service.orchestra.trial?.get();
		if (!state || state.circle !== "R0") fail("候选包未进入 R0 试用圈");
		const samplingStartedAt = new Date().toISOString();
		writeJson(join(target, FILES.worker), {
			pid: process.pid,
			started_at: startedAt,
			sampling_started_at: samplingStartedAt,
			node: process.version,
			package_entry: launch.package_entry,
			recovery
		});
		const workspacePath = join(target, FILES.sample);
		const endAt = Date.now() + config.duration_ms;
		let slot = 0;
		while (!stopping && Date.now() < endAt) {
			const ledger = readLedger(join(target, FILES.ledger));
			const trial = service.orchestra.trial.get();
			appendSample(join(target, FILES.samples), {
				slot,
				at: new Date().toISOString(),
				monotonic_ms: Math.round(performance.now() - monoStart),
				rss_bytes: process.memoryUsage().rss,
				resource: resourceSnapshot(cpuStart),
				disk: diskSnapshot(target),
				tasks: readTaskSnapshot(join(target, FILES.ledger)),
				exceptions: exceptions.length === 0 ? { count: 0 } : { count: exceptions.length, last_at: exceptions.at(-1).at, last_message: exceptions.at(-1).message },
				ledger,
				trial: { candidate: trial.candidate, circle: trial.circle, revision: trial.revision, halted: trial.halted },
				workspace_sha256: sha256File(workspacePath)
			});
			samples += 1;
			slot += 1;
			const sleepMs = Math.min(config.sample_interval_ms, Math.max(0, endAt - Date.now()));
			const sleepUntil = Date.now() + sleepMs;
			while (!stopping && Date.now() < sleepUntil) await sleep(Math.min(500, sleepUntil - Date.now()));
		}
		writeJson(join(target, FILES.end), {
			status: stopping ? "cancelled" : "completed",
			reason: stopping ? "signal" : null,
			started_at: samplingStartedAt,
			ended_at: new Date().toISOString(),
			samples,
			duration_ms: Date.now() - Date.parse(samplingStartedAt),
			recovery,
			exceptions: { count: exceptions.length, items: exceptions.slice(-20) },
			tasks: readTaskSnapshot(join(target, FILES.ledger)),
			resource: resourceSnapshot(cpuStart)
		});
	} catch (error) {
		failure = error;
		writeJson(join(target, FILES.end), {
			status: "failed",
			reason: error?.message ?? String(error),
			started_at: startedAt,
			ended_at: new Date().toISOString(),
			samples,
			duration_ms: Date.now() - Date.parse(startedAt)
		});
	} finally {
		if (service) {
			try {
				await service.release();
			} catch {}
		}
	}
	if (failure) {
		console.error(failure);
		process.exitCode = 1;
	}
}

function usage() {
	console.log(`用法：
  node --no-warnings scripts/r0-observe.mjs launch <新目录> <40位候选提交> <候选包.tgz> <包SHA256> [--seconds N | --minutes N] [--sample-seconds N] [--dry-run]
  node --no-warnings scripts/r0-observe.mjs status <观察目录>
  node --no-warnings scripts/r0-observe.mjs verify <观察目录> [--allow-dry-run]
  node --no-warnings scripts/r0-observe.mjs cancel <观察目录>
  node --no-warnings scripts/r0-observe.mjs report <观察目录> --key <ed25519 私钥.pem> [--out report.json]
  node --no-warnings scripts/r0-observe.mjs verify-report <report.json>`);
}

async function main(argv) {
	const command = argv[0];
	const { positional, flags } = parseFlags(argv.slice(1));
	if (command === "launch") {
		if (positional.length !== 4) {
			usage();
			fail("launch 需要新目录、完整候选提交、候选包与包 SHA256");
		}
		await launch(positional[0], positional[1], positional[2], positional[3], parseWindow(flags));
		return;
	}
	if (command === "status") {
		if (positional.length !== 1) fail("status 需要一个观察目录");
		status(positional[0]);
		return;
	}
	if (command === "verify") {
		if (positional.length !== 1) fail("verify 需要一个观察目录");
		verifyObservation(positional[0], { requireSoak: !flags["allow-dry-run"] });
		return;
	}
	if (command === "cancel") {
		if (positional.length !== 1) fail("cancel 需要一个观察目录");
		await cancel(positional[0]);
		return;
	}
	if (command === "report") {
		if (positional.length !== 1 || typeof flags.key !== "string") {
			usage();
			fail("report 需要观察目录与 --key <Ed25519 私钥 PEM>");
		}
		const report = signObservationReport(positional[0], { keyPath: flags.key, outPath: typeof flags.out === "string" ? flags.out : undefined });
		if (report.verification.accepted !== true) process.exitCode = 1;
		return;
	}
	if (command === "verify-report") {
		if (positional.length !== 1) fail("verify-report 需要报告文件");
		const payload = verifySignedReport(positional[0]);
		console.log(`签名有效；accepted: ${payload.verification?.accepted === true}`);
		for (const check of payload.verification?.checks ?? []) console.log(`${check.ok ? "PASS" : "FAIL"} ${check.id}: ${check.detail}`);
		if (payload.verification?.accepted !== true) process.exitCode = 1;
		return;
	}
	if (command === "__worker") {
		if (positional.length !== 1) fail("内部 worker 缺少观察目录");
		await runWorker(positional[0]);
		return;
	}
	usage();
	if (command) fail(`未知命令：${command}`);
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(SCRIPT)) {
	main(process.argv.slice(2)).catch((error) => {
		console.error(error?.message ?? error);
		process.exitCode = 1;
	});
}

export {
	assertCandidate,
	assertTarball,
	observationSummary,
	parseFlags,
	parseWindow,
	runWorker,
	sha256File,
	signObservationReport,
	status,
	verifyObservation,
	verifySignedReport
};
