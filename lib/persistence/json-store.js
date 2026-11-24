// 健壮的 JSON 持久化基础层。
// 设计约束：
// 1) 读取失败绝不返回空数据，绝不覆盖原文件；
// 2) 写入一律 tmp + fsync + rename，失败清理 tmp，原文件保持不变；
// 3) 损坏文件只复制到隔离目录，不移动、不删除原始文件；
// 4) 版本升级前先留副本，升级失败保留原始数据；
// 5) 跨进程写入用独占锁串行化。

import {
	closeSync,
	copyFileSync,
	existsSync,
	fsyncSync,
	lstatSync,
	mkdirSync,
	openSync,
	readFileSync,
	readdirSync,
	renameSync,
	statSync,
	unlinkSync,
	utimesSync,
	writeSync
} from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { basename, dirname, join } from "node:path";

/** 当前 orchestra.json 的逻辑格式版本。 */
export const JSON_STORE_SCHEMA_VERSION = 2;
/** 默认保留的滚动备份个数。 */
export const DEFAULT_BACKUP_KEEP = 10;
/** 导出文档类型。 */
export const EXPORT_KIND = "dsh-orchestra-export";

const RETRYABLE_CODES = new Set(["EBUSY", "EAGAIN", "EINTR", "EMFILE", "ENFILE", "SQLITE_BUSY", "SQLITE_LOCKED"]);
const BLOB_TAG = "__dsh_blob_base64__";
const BIGINT_TAG = "__dsh_bigint__";
const TABLE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

export class PersistenceError extends Error {
	constructor(code, message, details = {}) {
		super(message);
		this.name = "PersistenceError";
		this.code = code;
		this.details = details;
	}
}

export function sha256Hex(input) {
	return createHash("sha256").update(input).digest("hex");
}

/** 稳定序列化：对象键排序，保证同一份数据得到同一个校验和。 */
export function canonicalJson(value) {
	if (value === null) return "null";
	if (typeof value === "number") {
		if (!Number.isFinite(value)) throw new PersistenceError("STORE_SERIALIZE_FAILED", "不接受非有限数字");
		return JSON.stringify(value);
	}
	if (typeof value === "boolean" || typeof value === "string") return JSON.stringify(value);
	if (Array.isArray(value)) return "[" + value.map(canonicalJson).join(",") + "]";
	if (typeof value === "object") {
		return "{" + Object.entries(value)
			.filter(([, v]) => v !== undefined)
			.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
			.map(([k, v]) => JSON.stringify(k) + ":" + canonicalJson(v))
			.join(",") + "}";
	}
	throw new PersistenceError("STORE_SERIALIZE_FAILED", "不接受这个类型: " + typeof value);
}

export function sleepSync(ms) {
	if (ms <= 0) return;
	Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

export function isRetryableError(error) {
	const code = error?.code ?? "";
	if (RETRYABLE_CODES.has(code)) return true;
	return /SQLITE_BUSY|SQLITE_LOCKED|database is locked|resource temporarily unavailable/i.test(String(error?.message ?? ""));
}

/** 同步重试；只对明确的瞬时错误重试，权限/磁盘满等错误立即抛出。 */
export function withRetrySync(fn, options = {}) {
	const attempts = Math.max(1, options.attempts ?? 5);
	const baseDelayMs = options.baseDelayMs ?? 25;
	const maxDelayMs = options.maxDelayMs ?? 500;
	const retryable = options.retryable ?? isRetryableError;
	let last;
	for (let i = 0; i < attempts; i += 1) {
		try {
			return fn(i);
		} catch (error) {
			last = error;
			if (!retryable(error) || i === attempts - 1) throw error;
			sleepSync(Math.min(maxDelayMs, baseDelayMs * 2 ** i));
		}
	}
	throw last;
}

export function ensureDirSync(dir, mode = 0o700) {
	mkdirSync(dir, { recursive: true, mode });
}

export function fsyncDirSync(dir) {
	try {
		const fd = openSync(dir, "r");
		try {
			fsyncSync(fd);
		} finally {
			closeSync(fd);
		}
	} catch (error) {
		// Windows 不支持对目录 fsync；其余平台失败必须暴露。
		if (process.platform !== "win32") throw error;
	}
}

export function fsyncFileSync(file) {
	const fd = openSync(file, "r+");
	try {
		fsyncSync(fd);
	} finally {
		closeSync(fd);
	}
}

/**
 * 原子写文件：同目录唯一 tmp → 写入 → fsync → rename → fsync 目录。
 * 任何一步失败都只清理 tmp，目标文件保持原样。
 * writeImpl 仅用于测试注入（模拟磁盘满/半写中断）。
 */
export function writeFileAtomicSync(file, data, options = {}) {
	const dir = dirname(file);
	ensureDirSync(dir);
	const tmp = join(dir, `.${basename(file)}.${process.pid}.${Date.now()}.${randomUUID()}.tmp`);
	const mode = options.mode ?? 0o600;
	const buffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data), "utf8");
	let fd;
	try {
		fd = openSync(tmp, "wx", mode);
		if (options.writeImpl) options.writeImpl(fd, buffer);
		else writeSync(fd, buffer);
		if (options.fsync !== false) fsyncSync(fd);
		closeSync(fd);
		fd = undefined;
		renameSync(tmp, file);
		if (options.fsync !== false) fsyncDirSync(dir);
	} catch (error) {
		try {
			if (fd !== undefined) closeSync(fd);
		} catch {}
		try {
			if (existsSync(tmp)) unlinkSync(tmp);
		} catch {}
		throw error;
	}
}

export function writeJsonAtomicSync(file, document, options = {}) {
	return withRetrySync(
		() => writeFileAtomicSync(file, JSON.stringify(document, null, 2) + "\n", options),
		{ attempts: options.attempts ?? 3 }
	);
}

/** 读取 JSON；解析失败抛 STORE_CORRUPT，绝不返回空对象。 */
export function readJsonSync(file, options = {}) {
	let text;
	try {
		text = readFileSync(file, "utf8");
	} catch (error) {
		if (error?.code === "ENOENT" && options.allowMissing === true) return options.missingValue ?? null;
		throw new PersistenceError("STORE_READ_FAILED", `读取失败：${file}`, { file, cause: error?.message, errno: error?.code });
	}
	try {
		return JSON.parse(text);
	} catch (error) {
		throw new PersistenceError("STORE_CORRUPT", `JSON 解析失败：${file}`, { file, cause: error?.message });
	}
}

/** 默认不跟随符号链接，避免把写操作引到意外路径。 */
export function assertRegularFileSync(file, label = "文件", options = {}) {
	const info = options.allowSymlink ? statSync(file) : lstatSync(file);
	if (!options.allowSymlink && info.isSymbolicLink()) throw new PersistenceError("STORE_UNSAFE_PATH", `${label}是符号链接，拒绝操作：${file}`, { file });
	if (!info.isFile()) throw new PersistenceError("STORE_UNSAFE_PATH", `${label}不是普通文件：${file}`, { file });
	return info;
}

function stamp() {
	const now = new Date();
	const pad = (n, w = 2) => String(n).padStart(w, "0");
	return `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}T${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}${pad(now.getUTCMilliseconds(), 3)}-${process.pid}-${Math.random().toString(16).slice(2, 8)}`;
}

/**
 * 把损坏文件复制到独立隔离目录，并写一份原因说明。
 * 只复制，不移动、不删除；即使复制失败也不影响原文件。
 */
export function quarantineFileSync(file, backupRoot, reason) {
	if (!existsSync(file)) return null;
	const dir = join(backupRoot, "corrupt");
	ensureDirSync(dir);
	const dest = join(dir, `${stamp()}-${basename(file)}`);
	copyFileDurableSync(file, dest);
	writeFileAtomicSync(`${dest}.error.txt`, [
		`source: ${file}`,
		`quarantined_at: ${new Date().toISOString()}`,
		`reason: ${reason ?? "unknown"}`
	].join("\n") + "\n");
	return dest;
}

/** 大文件复制：同目录 tmp + fsync + rename；失败清理 tmp。 */
export function copyFileDurableSync(source, target, options = {}) {
	const dir = dirname(target);
	ensureDirSync(dir);
	return withRetrySync(() => {
		const tmp = join(dir, `.${basename(target)}.${process.pid}.${Date.now()}.${randomUUID()}.copy`);
		try {
			copyFileSync(source, tmp);
			fsyncFileSync(tmp);
			renameSync(tmp, target);
			fsyncDirSync(dir);
			return target;
		} catch (error) {
			try {
				if (existsSync(tmp)) unlinkSync(tmp);
			} catch {}
			throw error;
		}
	}, { attempts: options.attempts ?? 3 });
}

/** 只删除备份目录内的旧备份，永远不碰用户原文件。 */
export function rotateBackupsSync(backupDir, keep, pattern = /^orchestra-.*\.json$/) {
	const limit = Math.max(0, Number(keep) || 0);
	if (!existsSync(backupDir)) return [];
	const entries = readdirSync(backupDir)
		.filter((name) => pattern.test(name))
		.map((name) => ({ name, path: join(backupDir, name), mtimeMs: statSync(join(backupDir, name)).mtimeMs }))
		.sort((a, b) => b.mtimeMs - a.mtimeMs);
	const removed = [];
	for (const entry of entries.slice(limit)) {
		try {
			unlinkSync(entry.path);
			removed.push(entry.path);
		} catch {}
	}
	return removed;
}

/** 先把当前文件复制成滚动备份，再允许覆盖写。 */
export function backupJsonFileSync(file, backupDir, options = {}) {
	if (!existsSync(file)) return null;
	ensureDirSync(backupDir);
	const dest = join(backupDir, `orchestra-${stamp()}.json`);
	copyFileDurableSync(file, dest);
	rotateBackupsSync(backupDir, options.keep ?? DEFAULT_BACKUP_KEEP);
	return dest;
}

/**
 * 进程间独占锁：路径旁创建 *.lock（O_EXCL），带过期接管。
 * 锁内只做短操作；长操作请把 staleMs 调大。
 */
export function acquireFileLockSync(target, options = {}) {
	const lockPath = `${target}.lock`;
	const staleMs = options.staleMs ?? 60_000;
	const timeoutMs = options.timeoutMs ?? 10_000;
	const pollMs = options.pollMs ?? 25;
	const token = `${process.pid}:${randomUUID()}`;
	ensureDirSync(dirname(lockPath));
	const started = Date.now();
	for (;;) {
		try {
			const fd = openSync(lockPath, "wx", 0o600);
			writeSync(fd, JSON.stringify({ pid: process.pid, token, at: new Date().toISOString() }));
			fsyncSync(fd);
			closeSync(fd);
			let released = false;
			return {
				path: lockPath,
				release() {
					if (released) return;
					released = true;
					try {
						const holder = JSON.parse(readFileSync(lockPath, "utf8"));
						if (holder.token === token) unlinkSync(lockPath);
					} catch {}
				},
				touch() {
					try {
						const now = new Date();
						utimesSync(lockPath, now, now);
					} catch {}
				}
			};
		} catch (error) {
			if (error?.code !== "EEXIST") throw error;
			let age = Infinity;
			try {
				age = Date.now() - statSync(lockPath).mtimeMs;
			} catch {
				continue;
			}
			if (age > staleMs) {
				// rename 是原子的：并发接管时只有一个进程能成功，避免互相删掉新锁。
				const tomb = `${lockPath}.stale.${randomUUID()}`;
				try {
					renameSync(lockPath, tomb);
					try {
						unlinkSync(tomb);
					} catch {}
				} catch {}
				continue;
			}
			if (Date.now() - started > timeoutMs) {
				throw new PersistenceError("STORE_LOCKED", `另一个进程正在写入：${target}`, { target, lockPath, ageMs: age });
			}
			sleepSync(pollMs);
		}
	}
}

export function withFileLockSync(target, fn, options = {}) {
	const lock = acquireFileLockSync(target, options);
	try {
		return fn(lock);
	} finally {
		lock.release();
	}
}

function deriveColumns(tables) {
	const out = {};
	for (const [name, rows] of Object.entries(tables)) {
		const columns = new Set();
		for (const row of rows) {
			for (const column of Object.keys(row)) columns.add(column);
		}
		out[name] = [...columns];
	}
	return out;
}

export function documentChecksum(document) {
	return sha256Hex(canonicalJson({
		dbSchemaVersion: document?.dbSchemaVersion ?? null,
		tableColumns: document?.tableColumns ?? {},
		tables: document?.tables ?? {}
	}));
}

function validateTables(tables, label) {
	if (!tables || typeof tables !== "object" || Array.isArray(tables)) {
		throw new PersistenceError("STORE_CORRUPT", `${label} 的 tables 不是对象`);
	}
	for (const [name, rows] of Object.entries(tables)) {
		if (!TABLE_NAME.test(name)) throw new PersistenceError("STORE_CORRUPT", `非法表名：${name}`);
		if (!Array.isArray(rows)) throw new PersistenceError("STORE_CORRUPT", `${name} 不是行数组`);
		for (const row of rows) {
			if (!row || typeof row !== "object" || Array.isArray(row)) throw new PersistenceError("STORE_CORRUPT", `${name} 含非法行`);
			for (const column of Object.keys(row)) {
				if (!TABLE_NAME.test(column)) throw new PersistenceError("STORE_CORRUPT", `非法列名：${name}.${column}`);
			}
		}
	}
}

/**
 * JSON 逻辑格式迁移。只做纯函数变换，不写盘。
 * v1（历史）：{ schemaVersion: 1, data: { tables } } 或 { tables } 无版本
 * v2（当前）：{ schemaVersion: 2, kind, generatedAt, dbSchemaVersion, tableColumns, tables, checksum }
 */
const JSON_MIGRATIONS = {
	1(document) {
		const tables = document?.data?.tables ?? document?.tables;
		if (!tables || typeof tables !== "object" || Array.isArray(tables)) {
			throw new PersistenceError("STORE_CORRUPT", "v1 数据缺少 tables");
		}
		validateTables(tables, "v1");
		const tableColumns = document.tableColumns ?? deriveColumns(tables);
		const migrated = {
			schemaVersion: 2,
			kind: EXPORT_KIND,
			generatedAt: document.generatedAt ?? new Date().toISOString(),
			dbSchemaVersion: document.dbSchemaVersion ?? null,
			tableColumns,
			tables
		};
		migrated.checksum = documentChecksum(migrated);
		return migrated;
	}
};

export function migrateJsonDocument(input) {
	if (!input || typeof input !== "object" || Array.isArray(input)) {
		throw new PersistenceError("STORE_CORRUPT", "顶层不是 JSON 对象");
	}
	const from = Number.isInteger(input.schemaVersion) ? input.schemaVersion : 1;
	if (from > JSON_STORE_SCHEMA_VERSION) {
		throw new PersistenceError("STORE_SCHEMA_UNSUPPORTED", `数据版本 ${from} 高于当前支持的 ${JSON_STORE_SCHEMA_VERSION}`, {
			found: from,
			supported: JSON_STORE_SCHEMA_VERSION
		});
	}
	let version = Math.max(1, from);
	let document = structuredClone(input);
	const applied = [];
	while (version < JSON_STORE_SCHEMA_VERSION) {
		const step = JSON_MIGRATIONS[version];
		if (!step) throw new PersistenceError("STORE_SCHEMA_UNSUPPORTED", `缺少 v${version} → v${version + 1} 的迁移`, { version });
		document = step(document);
		version += 1;
		document.schemaVersion = version;
		applied.push(version);
	}
	if (document.kind !== EXPORT_KIND) {
		throw new PersistenceError("STORE_CORRUPT", `不是 ${EXPORT_KIND} 文档`, { kind: document.kind });
	}
	validateTables(document.tables, `v${version}`);
	document.tableColumns = document.tableColumns ?? deriveColumns(document.tables);
	if (document.checksum && document.checksum !== documentChecksum(document)) {
		throw new PersistenceError("STORE_CHECKSUM_MISMATCH", "内容校验和不一致，文件可能被截断或篡改", {
			expected: document.checksum,
			actual: documentChecksum(document)
		});
	}
	document.checksum = document.checksum ?? documentChecksum(document);
	return { document, from, to: version, applied };
}

/**
 * 读取并升级 JSON 文档：
 * - 解析失败：隔离副本 + 抛错，不写原文件；
 * - 版本升级：先滚动备份，再原子写入；写入失败原文件不变。
 */
export function readOrUpgradeJsonSync(file, options = {}) {
	if (!existsSync(file)) throw new PersistenceError("STORE_MISSING", `文件不存在：${file}`, { file });
	assertRegularFileSync(file);
	const backupDir = options.backupDir ?? join(dirname(file), "backups", "json");
	let parsed;
	try {
		parsed = readJsonSync(file);
	} catch (error) {
		if (error?.code === "STORE_CORRUPT") quarantineFileSync(file, backupDir, error.message);
		throw error;
	}
	const result = migrateJsonDocument(parsed);
	if (result.applied.length > 0) {
		backupJsonFileSync(file, backupDir, { keep: options.keep ?? DEFAULT_BACKUP_KEEP });
		try {
			writeJsonAtomicSync(file, result.document);
		} catch (error) {
			throw new PersistenceError("STORE_MIGRATION_WRITE_FAILED", "升级写入失败，原始文件保持不变", {
				file,
				backupDir,
				cause: error?.message,
				code: error?.code
			});
		}
	}
	return result;
}
