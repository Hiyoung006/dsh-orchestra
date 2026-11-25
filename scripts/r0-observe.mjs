// R0 观察 harness：从候选包启动 S6 试用圈，按固定间隔采集真实时间、单调计时、
// 账本水位与合成工作区摘要；支持短时 dry-run、状态、取消与离线核验。
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
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
	sample: "workspace/sample.txt"
});

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
		mode: durationMs < 24 * 60 * 60 * 1000 ? "dry-run" : "soak"
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

function verifyObservation(dir) {
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
		workspace_sha256: workspaceSha
	};
	console.log("R0 观察核验通过");
	for (const [key, value] of Object.entries(summary)) console.log(`${key}: ${value}`);
	return summary;
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
	let service = null;
	let samples = 0;
	let failure = null;
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
		const state = service.orchestra.trial?.get();
		if (!state || state.circle !== "R0") fail("候选包未进入 R0 试用圈");
		const samplingStartedAt = new Date().toISOString();
		writeJson(join(target, FILES.worker), {
			pid: process.pid,
			started_at: startedAt,
			sampling_started_at: samplingStartedAt,
			node: process.version,
			package_entry: launch.package_entry
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
			duration_ms: Date.now() - Date.parse(samplingStartedAt)
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
  node --no-warnings scripts/r0-observe.mjs launch <新目录> <40位候选提交> <候选包.tgz> <包SHA256> [--seconds N | --minutes N] [--sample-seconds N]
  node --no-warnings scripts/r0-observe.mjs status <观察目录>
  node --no-warnings scripts/r0-observe.mjs verify <观察目录>
  node --no-warnings scripts/r0-observe.mjs cancel <观察目录>`);
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
		verifyObservation(positional[0]);
		return;
	}
	if (command === "cancel") {
		if (positional.length !== 1) fail("cancel 需要一个观察目录");
		await cancel(positional[0]);
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

export { assertCandidate, assertTarball, observationSummary, parseFlags, parseWindow, runWorker, sha256File, status, verifyObservation };
