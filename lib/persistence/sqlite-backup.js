// SQLite 持久化可靠性增强：完整性校验、迁移前快照、滚动备份、
// 文件级恢复、逻辑 JSON 导出/导入。
// 原则：任何失败都保留原始文件；只在隔离目录里处理副本。

import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import {
	closeSync,
	existsSync,
	fsyncSync,
	openSync,
	readSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	unlinkSync,
	writeSync
} from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
	DEFAULT_BACKUP_KEEP,
	EXPORT_KIND,
	PersistenceError,
	assertRegularFileSync,
	backupJsonFileSync,
	canonicalJson,
	copyFileDurableSync,
	ensureDirSync,
	fsyncDirSync,
	fsyncFileSync,
	quarantineFileSync,
	readJsonSync,
	readOrUpgradeJsonSync,
	sha256Hex,
	withRetrySync,
	writeJsonAtomicSync
} from "./json-store.js";

export const SNAPSHOT_KIND = "dsh-orchestra-snapshot";
export const SNAPSHOT_SCHEMA_VERSION = 2;

/** 每个数据库独立的备份目录，避免不同工作区互相轮转掉备份。 */
export function backupDirectoryFor(dbFile) {
	return join(dirname(dbFile), "backups", basename(dbFile).replace(/\.sqlite$/u, ""));
}

const BLOB_TAG = "__dsh_blob_base64__";
const BIGINT_TAG = "__dsh_bigint__";
const SAFE_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const INTERNAL_TABLES = new Set(["schema_migrations", "service_meta"]);

function nowStamp() {
	const d = new Date();
	const pad = (n, w = 2) => String(n).padStart(w, "0");
	return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}${pad(d.getUTCMilliseconds(), 3)}-${process.pid}-${Math.random().toString(16).slice(2, 8)}`;
}

function sqlString(value) {
	return `'${String(value).replace(/'/g, "''")}'`;
}

function quoteIdent(name) {
	if (!SAFE_NAME.test(name)) throw new PersistenceError("STORE_CORRUPT", `非法标识符：${name}`);
	return `"${name.replace(/"/g, '""')}"`;
}

/** 大文件分块哈希，避免把整个库读进内存。 */
export function sha256FileSync(file) {
	const hash = createHash("sha256");
	const fd = openSync(file, "r");
	const buffer = Buffer.allocUnsafe(1024 * 1024);
	try {
		for (;;) {
			const read = readSync(fd, buffer, 0, buffer.length, null);
			if (read <= 0) break;
			hash.update(buffer.subarray(0, read));
		}
	} finally {
		closeSync(fd);
	}
	return hash.digest("hex");
}

function integrityRows(db, pragma) {
	return db.prepare(`PRAGMA ${pragma}`).all();
}

function assertIntegrityWith(db, pragma, file) {
	const rows = integrityRows(db, pragma);
	const first = rows[0] ? String(Object.values(rows[0])[0] ?? "") : "";
	if (rows.length === 1 && first.toLowerCase() === "ok") return true;
	throw new PersistenceError("DB_CORRUPT", `SQLite 完整性校验失败（${pragma}）：${file}`, {
		file,
		details: rows.slice(0, 20)
	});
}

/** 完整性检查；quick=true 用于启动，false 用于离线校验。允许数据库路径是符号链接。 */
export function assertDatabaseIntegritySync(file, options = {}) {
	if (!existsSync(file)) throw new PersistenceError("DB_MISSING", `数据库不存在：${file}`, { file });
	assertRegularFileSync(file, "数据库文件", { allowSymlink: true });
	let db;
	try {
		// 用读写连接打开，允许 SQLite 在存在热 WAL 时先做正常恢复；不执行任何业务写。
		db = new DatabaseSync(file);
		db.exec("PRAGMA busy_timeout=5000");
		return assertIntegrityWith(db, options.quick === true ? "quick_check" : "integrity_check", file);
	} catch (error) {
		if (error instanceof PersistenceError) throw error;
		throw new PersistenceError("DB_CORRUPT", `无法读取数据库：${file}`, { file, cause: error?.message, code: error?.code });
	} finally {
		try {
			db?.close();
		} catch {}
	}
}

function readSchemaVersion(db) {
	const has = db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='schema_migrations'").get();
	if (!has) return { exists: false, maxVersion: 0, checksums: new Map() };
	const rows = db.prepare("SELECT version, checksum FROM schema_migrations ORDER BY version").all();
	const checksums = new Map(rows.map((row) => [Number(row.version), String(row.checksum)]));
	const maxVersion = rows.length ? Number(rows[rows.length - 1].version) : 0;
	return { exists: true, maxVersion, checksums };
}

/**
 * 检查迁移状态，不写入。
 * steps: [{version, checksum}]
 */
export function inspectMigrationsSync(file, steps) {
	const latest = steps.length ? Number(steps[steps.length - 1].version) : 0;
	if (!existsSync(file)) {
		return { exists: false, maxVersion: 0, latest, pending: steps, checksumMismatch: [], newerThanCode: false, gaps: [] };
	}
	assertDatabaseIntegritySync(file, { quick: true });
	let db;
	try {
		db = new DatabaseSync(file);
		db.exec("PRAGMA busy_timeout=5000");
		const state = readSchemaVersion(db);
		const checksumMismatch = steps
			.filter((step) => state.checksums.has(step.version) && state.checksums.get(step.version) !== step.checksum)
			.map((step) => step.version);
		const pending = steps.filter((step) => !state.checksums.has(step.version));
		const newerThanCode = state.maxVersion > latest;
		const gaps = [];
		for (let version = 1; version <= state.maxVersion; version += 1) {
			if (!state.checksums.has(version)) gaps.push(version);
		}
		return { exists: true, maxVersion: state.maxVersion, latest, pending, checksumMismatch, newerThanCode, gaps };
	} finally {
		try {
			db?.close();
		} catch {}
	}
}

/** 用 VACUUM INTO 生成一致性快照，并写带 schemaVersion 的 manifest。 */
export function createDatabaseSnapshotSync(dbFile, backupDir, options = {}) {
	ensureDirSync(backupDir);
	const keep = Math.max(1, Number(options.keep ?? DEFAULT_BACKUP_KEEP));
	const base = `orchestra-${nowStamp()}`;
	const snapshotPath = join(backupDir, `${base}.sqlite`);
	const tmp = join(backupDir, `.${base}.sqlite.tmp`);
	if (existsSync(tmp)) unlinkSync(tmp);
	withRetrySync(() => {
		let db;
		try {
			db = new DatabaseSync(dbFile);
			db.exec("PRAGMA busy_timeout=5000");
			db.exec(`VACUUM INTO ${sqlString(tmp)}`);
		} finally {
			try {
				db?.close();
			} catch {}
		}
	}, { attempts: 3 });
	fsyncFileSync(tmp);
	renameSync(tmp, snapshotPath);
	fsyncDirSync(backupDir);
	const bytes = statSync(snapshotPath).size;
	const sha256 = sha256FileSync(snapshotPath);
	const manifest = {
		schemaVersion: SNAPSHOT_SCHEMA_VERSION,
		kind: SNAPSHOT_KIND,
		createdAt: new Date().toISOString(),
		dbSchemaVersion: options.dbSchemaVersion ?? null,
		label: options.label ?? null,
		source: { dbFile },
		snapshot: { file: `${base}.sqlite`, bytes, sha256 }
	};
	const manifestPath = join(backupDir, `${base}.json`);
	writeJsonAtomicSync(manifestPath, manifest);
	rotateSnapshotsSync(backupDir, keep);
	return { manifestPath, manifest, snapshotPath };
}

function rotateSnapshotsSync(backupDir, keep) {
	if (!existsSync(backupDir)) return [];
	const bases = readdirSync(backupDir)
		.filter((name) => /^orchestra-.*\.sqlite$/.test(name))
		.map((name) => name.slice(0, -".sqlite".length))
		.sort()
		.reverse();
	const removed = [];
	for (const base of bases.slice(keep)) {
		for (const suffix of [".sqlite", ".json"]) {
			const file = join(backupDir, base + suffix);
			try {
				unlinkSync(file);
				removed.push(file);
			} catch {}
		}
	}
	return removed;
}

/** manifest 版本迁移：v1（无版本）→ v2。 */
export function normalizeSnapshotManifest(input) {
	if (!input || typeof input !== "object" || Array.isArray(input)) {
		throw new PersistenceError("STORE_CORRUPT", "快照 manifest 不是对象");
	}
	const version = Number.isInteger(input.schemaVersion) ? input.schemaVersion : 1;
	if (version > SNAPSHOT_SCHEMA_VERSION) {
		throw new PersistenceError("STORE_SCHEMA_UNSUPPORTED", `快照版本 ${version} 高于当前支持的 ${SNAPSHOT_SCHEMA_VERSION}`, {
			found: version,
			supported: SNAPSHOT_SCHEMA_VERSION
		});
	}
	let manifest = structuredClone(input);
	if (version < 2) {
		manifest = {
			schemaVersion: 2,
			kind: SNAPSHOT_KIND,
			createdAt: input.createdAt ?? new Date().toISOString(),
			dbSchemaVersion: input.dbSchemaVersion ?? null,
			label: input.label ?? null,
			source: input.source ?? {},
			snapshot: input.snapshot ?? {}
		};
	}
	if (manifest.kind !== SNAPSHOT_KIND) throw new PersistenceError("STORE_CORRUPT", "不是快照 manifest");
	const file = manifest.snapshot?.file;
	if (typeof file !== "string" || file.includes("/") || file.includes("\\") || file.startsWith(".")) {
		throw new PersistenceError("STORE_CORRUPT", "快照文件名不合法", { file });
	}
	return manifest;
}

export function readSnapshotManifestSync(manifestPath, options = {}) {
	assertRegularFileSync(manifestPath);
	let parsed;
	try {
		parsed = readJsonSync(manifestPath);
	} catch (error) {
		if (error?.code === "STORE_CORRUPT") quarantineFileSync(manifestPath, options.backupDir ?? join(dirname(manifestPath), "corrupt"), error.message);
		throw error;
	}
	return normalizeSnapshotManifest(parsed);
}

function moveIfExists(source, targetDir, name) {
	if (!existsSync(source)) return null;
	ensureDirSync(targetDir);
	const dest = join(targetDir, name);
	renameSync(source, dest);
	return dest;
}

function restoreMarkerPath(dbFile) {
	return `${dbFile}.restore-in-progress.json`;
}

function writeRestoreMarkerSync(dbFile, info) {
	ensureDirSync(dirname(dbFile));
	writeJsonAtomicSync(restoreMarkerPath(dbFile), {
		schemaVersion: 1,
		startedAt: new Date().toISOString(),
		...info
	});
}

function clearRestoreMarkerSync(dbFile) {
	try {
		unlinkSync(restoreMarkerPath(dbFile));
	} catch {}
}

/**
 * 恢复操作会被中断（断电/强杀）。如果发现恢复标记：
 * - 当前库存在且完整：说明恢复已完成，清掉标记；
 * - 当前库缺失或损坏：拒绝启动，避免用空库掩盖数据。
 */
export function assertNoIncompleteRestoreSync(dbFile) {
	const marker = restoreMarkerPath(dbFile);
	if (!existsSync(marker)) return;
	let info = null;
	try {
		info = readJsonSync(marker);
	} catch {}
	if (existsSync(dbFile)) {
		try {
			assertDatabaseIntegritySync(dbFile, { quick: true });
			clearRestoreMarkerSync(dbFile);
			return;
		} catch {}
	}
	throw new PersistenceError("DB_RESTORE_INCOMPLETE", "检测到未完成的恢复操作；为避免覆盖数据，已拒绝启动", {
		dbFile,
		marker,
		info
	});
}

/**
 * 用快照文件恢复数据库。调用方必须先关闭数据库并持有维护锁。
 * 当前库（含 WAL/SHM）被移动到隔离目录，不删除；失败时回滚。
 */
export function restoreSnapshotFileSync(snapshotPath, dbFile, backupRoot, options = {}) {
	assertDatabaseIntegritySync(snapshotPath, { quick: false });
	const corruptDir = join(backupRoot, "corrupt", `${nowStamp()}-${options.reason ?? "restore"}`);
	ensureDirSync(corruptDir);
	const hadOriginal = existsSync(dbFile);
	writeRestoreMarkerSync(dbFile, {
		kind: "snapshot-restore",
		snapshotPath,
		corruptDir,
		hadOriginal
	});
	const moved = [];
	try {
		for (const suffix of ["", "-wal", "-shm"]) {
			const dest = moveIfExists(dbFile + suffix, corruptDir, `database${suffix}`);
			if (dest) moved.push({ from: dbFile + suffix, to: dest });
		}
		copyFileDurableSync(snapshotPath, dbFile);
		assertDatabaseIntegritySync(dbFile, { quick: false });
		clearRestoreMarkerSync(dbFile);
		return { quarantinedDir: corruptDir, moved };
	} catch (error) {
		try {
			if (existsSync(dbFile)) renameSync(dbFile, join(corruptDir, "restore-failed.sqlite"));
		} catch {}
		for (const entry of moved) {
			try {
				renameSync(entry.to, entry.from);
			} catch {}
		}
		// 旧库已搬回（或本来就没有旧库）时才允许下次启动继续，否则保留标记等待人工处理。
		if (existsSync(dbFile) || !hadOriginal) clearRestoreMarkerSync(dbFile);
		throw new PersistenceError("DB_RESTORE_FAILED", "恢复失败，原始文件已回滚", {
			dbFile,
			cause: error?.message,
			quarantinedDir: corruptDir
		});
	}
}

export function restoreSnapshotManifestSync(manifestPath, dbFile, backupRoot, options = {}) {
	const manifest = readSnapshotManifestSync(manifestPath, { backupDir: backupRoot });
	const snapshotPath = join(dirname(manifestPath), manifest.snapshot.file);
	if (!existsSync(snapshotPath)) {
		throw new PersistenceError("STORE_CORRUPT", `快照文件缺失：${snapshotPath}`, { snapshotPath });
	}
	const bytes = statSync(snapshotPath).size;
	if (manifest.snapshot.bytes != null && Number(manifest.snapshot.bytes) !== bytes) {
		throw new PersistenceError("STORE_CHECKSUM_MISMATCH", "快照大小与 manifest 不一致", {
			expected: manifest.snapshot.bytes,
			actual: bytes
		});
	}
	if (manifest.snapshot.sha256 && sha256FileSync(snapshotPath) !== manifest.snapshot.sha256) {
		throw new PersistenceError("STORE_CHECKSUM_MISMATCH", "快照校验和不一致", { snapshotPath });
	}
	return restoreSnapshotFileSync(snapshotPath, dbFile, backupRoot, { reason: options.reason ?? "manifest-restore" });
}

function encodeValue(value) {
	if (value instanceof Uint8Array) return { [BLOB_TAG]: Buffer.from(value).toString("base64") };
	if (typeof value === "bigint") return { [BIGINT_TAG]: value.toString() };
	return value;
}

function decodeValue(value) {
	if (value && typeof value === "object" && !Array.isArray(value)) {
		if (typeof value[BLOB_TAG] === "string") return Buffer.from(value[BLOB_TAG], "base64");
		if (typeof value[BIGINT_TAG] === "string") return BigInt(value[BIGINT_TAG]);
	}
	return value;
}

/**
 * 逻辑导出为 orchestra.json（schemaVersion=2）。
 * 同时把新导出复制到滚动目录，保留最近 N 份。
 */
export function exportDatabaseJsonSync(dbFile, outFile, options = {}) {
	if (!existsSync(dbFile)) throw new PersistenceError("DB_MISSING", `数据库不存在：${dbFile}`, { dbFile });
	assertDatabaseIntegritySync(dbFile, { quick: true });
	const backupRoot = options.backupRoot ?? backupDirectoryFor(dbFile);
	const versionsDir = join(backupRoot, "json");
	ensureDirSync(versionsDir);
	let db;
	const tables = {};
	const tableColumns = {};
	let dbSchemaVersion = null;
	try {
		db = new DatabaseSync(dbFile);
		db.exec("PRAGMA busy_timeout=5000");
		const state = readSchemaVersion(db);
		dbSchemaVersion = state.exists ? state.maxVersion : 0;
		const names = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all()
			.map((row) => String(row.name));
		for (const name of names) {
			const columns = db.prepare(`PRAGMA table_info(${quoteIdent(name)})`).all().map((row) => String(row.name));
			tableColumns[name] = columns;
			const rows = db.prepare(`SELECT * FROM ${quoteIdent(name)}`).all();
			tables[name] = rows.map((row) => {
				const encoded = {};
				for (const column of columns) encoded[column] = encodeValue(row[column]);
				return encoded;
			});
		}
	} finally {
		try {
			db?.close();
		} catch {}
	}
	const document = {
		schemaVersion: 2,
		kind: EXPORT_KIND,
		generatedAt: new Date().toISOString(),
		dbSchemaVersion,
		tableColumns,
		tables
	};
	document.checksum = sha256Hex(canonicalJson({
		dbSchemaVersion: document.dbSchemaVersion,
		tableColumns: document.tableColumns,
		tables: document.tables
	}));
	if (existsSync(outFile)) backupJsonFileSync(outFile, versionsDir, { keep: options.keep ?? DEFAULT_BACKUP_KEEP });
	writeJsonAtomicSync(outFile, document);
	const rolling = join(versionsDir, `orchestra-${nowStamp()}.json`);
	copyFileDurableSync(outFile, rolling);
	rotateJsonVersionsSync(versionsDir, Math.max(1, Number(options.keep ?? DEFAULT_BACKUP_KEEP)), outFile);
	return {
		outFile,
		rolling,
		tables: Object.keys(tables).length,
		rows: Object.values(tables).reduce((sum, rows) => sum + rows.length, 0),
		dbSchemaVersion
	};
}

function rotateJsonVersionsSync(dir, keep, exclude) {
	const entries = readdirSync(dir)
		.filter((name) => /^orchestra-.*\.json$/.test(name))
		.map((name) => ({ name, path: join(dir, name), mtimeMs: statSync(join(dir, name)).mtimeMs }))
		.filter((entry) => entry.path !== exclude)
		.sort((a, b) => b.mtimeMs - a.mtimeMs);
	for (const entry of entries.slice(keep)) {
		try {
			unlinkSync(entry.path);
		} catch {}
	}
}

/** 定位迁移目录（相对本模块）。 */
export function migrationsDirectory() {
	const here = fileURLToPath(new URL(".", import.meta.url));
	return join(here, "..", "infra", "sqlite", "migrations");
}

export function loadMigrationStepsSync(dir = migrationsDirectory()) {
	return readdirSync(dir)
		.filter((file) => file.endsWith(".sql"))
		.sort()
		.map((file) => {
			const match = /^(\d{3})_(.+)\.sql$/.exec(file);
			if (!match) throw new PersistenceError("SCHEMA_VERSION_UNSUPPORTED", "迁移文件名不合规", { file });
			const body = readFileSync(join(dir, file), "utf8");
			return {
				version: Number(match[1]),
				name: match[2] ?? file,
				checksum: sha256Hex(body),
				statements: body.split(/^;\s*$/m).map((sql) => sql.trim()).filter(Boolean)
			};
		});
}

const BOOTSTRAP_SQL = [
	"CREATE TABLE IF NOT EXISTS service_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)",
	"CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
];

function applySchemaSync(db, steps) {
	for (const sql of BOOTSTRAP_SQL) db.exec(sql);
	for (const step of steps) {
		db.exec("BEGIN IMMEDIATE");
		try {
			for (const sql of step.statements) db.exec(sql);
			db.prepare("INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?,?,?,?)")
				.run(step.version, step.name, step.checksum, new Date().toISOString());
			db.exec("COMMIT");
		} catch (error) {
			try {
				db.exec("ROLLBACK");
			} catch {}
			throw error;
		}
	}
}

function createDbFromSchemaSync(file, steps) {
	const db = new DatabaseSync(file);
	try {
		db.exec("PRAGMA journal_mode=WAL");
		db.exec("PRAGMA synchronous=FULL");
		db.exec("PRAGMA busy_timeout=5000");
		applySchemaSync(db, steps);
	} finally {
		try {
			db?.close();
		} catch {}
	}
}

function quoteColumns(columns) {
	return columns.map(quoteIdent).join(",");
}

/**
 * 从 orchestra.json 逻辑恢复。
 * 旧库移入隔离目录 → 建新库并应用当前 schema → 导入数据 → 校验 → 原子替换。
 * 任何失败都会删除新库并搬回旧库，原始数据不丢。
 */
export function importDatabaseJsonSync(docPath, dbFile, options = {}) {
	const backupRoot = options.backupRoot ?? backupDirectoryFor(dbFile);
	const upgraded = readOrUpgradeJsonSync(docPath, {
		backupDir: join(backupRoot, "json"),
		keep: options.keep ?? DEFAULT_BACKUP_KEEP
	});
	const document = upgraded.document;
	const steps = options.steps ?? loadMigrationStepsSync();
	const latest = steps.length ? Number(steps[steps.length - 1].version) : 0;
	if (document.dbSchemaVersion != null && Number(document.dbSchemaVersion) > latest) {
		throw new PersistenceError("STORE_SCHEMA_UNSUPPORTED", `导出数据的数据库版本 ${document.dbSchemaVersion} 高于当前代码的 ${latest}`, {
			found: document.dbSchemaVersion,
			supported: latest
		});
	}
	const tmp = `${dbFile}.restore-${nowStamp()}.tmp`;
	if (existsSync(tmp)) rmSync(tmp, { force: true });
	const corruptDir = join(backupRoot, "corrupt", `${nowStamp()}-logical-restore`);
	const hadOriginal = existsSync(dbFile);
	const moved = [];
	let restored = false;
	if (hadOriginal) {
		writeRestoreMarkerSync(dbFile, {
			kind: "logical-import",
			docPath,
			corruptDir,
			hadOriginal
		});
	}
	try {
		if (hadOriginal) {
			createDatabaseSnapshotSync(dbFile, join(backupRoot, "snapshots"), {
				keep: options.keep ?? DEFAULT_BACKUP_KEEP,
				dbSchemaVersion: document.dbSchemaVersion ?? undefined,
				label: "pre-logical-restore"
			});
			ensureDirSync(corruptDir);
			for (const suffix of ["", "-wal", "-shm"]) {
				const source = dbFile + suffix;
				if (!existsSync(source)) continue;
				const dest = join(corruptDir, `database${suffix}`);
				renameSync(source, dest);
				moved.push({ from: source, to: dest });
			}
		}
		createDbFromSchemaSync(tmp, steps);
		const db = new DatabaseSync(tmp);
		try {
			db.exec("PRAGMA busy_timeout=5000");
			db.exec("PRAGMA foreign_keys=OFF");
			db.exec("BEGIN IMMEDIATE");
			const existingTables = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => String(row.name)));
			for (const [name, rows] of Object.entries(document.tables)) {
				if (INTERNAL_TABLES.has(name)) continue;
				if (!existingTables.has(name)) {
					throw new PersistenceError("STORE_SCHEMA_MISMATCH", `当前数据库没有表 ${name}`, { table: name });
				}
				const actual = new Set(db.prepare(`PRAGMA table_info(${quoteIdent(name)})`).all().map((row) => String(row.name)));
				const declared = document.tableColumns?.[name] ?? (rows[0] ? Object.keys(rows[0]) : []);
				const columns = declared.filter((column) => actual.has(column));
				const missing = declared.filter((column) => !actual.has(column));
				if (missing.length) {
					throw new PersistenceError("STORE_SCHEMA_MISMATCH", `表 ${name} 缺少列 ${missing.join(",")}`, { table: name, missing });
				}
			}
			const triggers = db.prepare("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND sql IS NOT NULL").all();
			for (const trigger of triggers) db.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(String(trigger.name))}`);
			for (const [name, rows] of Object.entries(document.tables)) {
				if (INTERNAL_TABLES.has(name)) continue;
				const actual = new Set(db.prepare(`PRAGMA table_info(${quoteIdent(name)})`).all().map((row) => String(row.name)));
				const declared = document.tableColumns?.[name] ?? (rows[0] ? Object.keys(rows[0]) : []);
				const columns = declared.filter((column) => actual.has(column));
				db.exec(`DELETE FROM ${quoteIdent(name)}`);
				if (!columns.length || rows.length === 0) continue;
				const statement = db.prepare(`INSERT INTO ${quoteIdent(name)} (${quoteColumns(columns)}) VALUES (${columns.map(() => "?").join(",")})`);
				for (const row of rows) {
					statement.run(...columns.map((column) => {
						const value = decodeValue(row[column]);
						return value === undefined ? null : value;
					}));
				}
			}
			for (const trigger of triggers) db.exec(trigger.sql);
			const violations = db.prepare("PRAGMA foreign_key_check").all();
			if (violations.length) {
				throw new PersistenceError("STORE_FOREIGN_KEY_VIOLATION", "导入后外键校验失败", { violations: violations.slice(0, 20) });
			}
			db.exec("COMMIT");
			db.exec("PRAGMA foreign_keys=ON");
			db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
			assertIntegrityWith(db, "integrity_check", tmp);
		} catch (error) {
			try {
				db.exec("ROLLBACK");
			} catch {}
			throw error;
		} finally {
			try {
				db.close();
			} catch {}
		}
		for (const suffix of ["-wal", "-shm"]) {
			const leftover = tmp + suffix;
			if (existsSync(leftover)) {
				try {
					unlinkSync(leftover);
				} catch {}
			}
		}
		fsyncFileSync(tmp);
		fsyncDirSync(dirname(tmp));
		renameSync(tmp, dbFile);
		restored = true;
		clearRestoreMarkerSync(dbFile);
		return {
			dbFile,
			restoredFrom: docPath,
			quarantinedDir: moved.length ? corruptDir : null,
			tables: Object.keys(document.tables).length
		};
	} finally {
		if (!restored) {
			try {
				if (existsSync(tmp)) renameSync(tmp, join(corruptDir, "restore-failed.sqlite"));
			} catch {
				try {
					rmSync(tmp, { force: true });
				} catch {}
			}
			for (const entry of moved) {
				try {
					renameSync(entry.to, entry.from);
				} catch {}
			}
			if (existsSync(dbFile) || !hadOriginal) clearRestoreMarkerSync(dbFile);
		}
	}
}

/** 同时复制数据库与 WAL/SHM 到隔离目录。 */
export function quarantineDatabaseFilesSync(dbFile, backupRoot, reason) {
	const destDir = join(backupRoot, "corrupt", nowStamp());
	ensureDirSync(destDir);
	const copied = [];
	for (const suffix of ["", "-wal", "-shm"]) {
		const source = dbFile + suffix;
		if (!existsSync(source)) continue;
		const dest = join(destDir, `database${suffix}`);
		copyFileDurableSync(source, dest);
		copied.push(dest);
	}
	writeJsonAtomicSync(join(destDir, "reason.json"), {
		source: dbFile,
		reason: reason ?? "unknown",
		at: new Date().toISOString(),
		files: copied
	});
	return destDir;
}

/**
 * 启动前准备：完整性校验 + 迁移前快照。
 * 返回的 rollback 必须在数据库连接关闭后调用。
 */
export function prepareServiceStorageSync(options) {
	const { dbFile, steps } = options;
	assertNoIncompleteRestoreSync(dbFile);
	const backupRoot = options.backupRoot ?? backupDirectoryFor(dbFile);
	ensureDirSync(backupRoot);
	if (!existsSync(dbFile)) {
		return { backupRoot, snapshot: null, rollback() {} };
	}
	let state;
	try {
		// inspect 内部先做 quick_check；一次打开同时完成完整性与迁移状态检查。
		state = inspectMigrationsSync(dbFile, steps);
	} catch (error) {
		if (error?.code !== "DB_CORRUPT") throw error;
		const quarantined = quarantineDatabaseFilesSync(dbFile, backupRoot, error.message);
		throw new PersistenceError("DB_CORRUPT", "数据库损坏，已隔离副本；原始文件未被修改", {
			dbFile,
			quarantined,
			cause: error?.message
		});
	}
	if (state.checksumMismatch.length) {
		throw new PersistenceError("SCHEMA_VERSION_UNSUPPORTED", "已执行的迁移被修改过", { versions: state.checksumMismatch });
	}
	if (state.gaps.length) {
		throw new PersistenceError("SCHEMA_VERSION_UNSUPPORTED", "迁移记录不连续，拒绝在缺失版本的库上继续升级", { gaps: state.gaps });
	}
	if (state.newerThanCode) {
		throw new PersistenceError("SCHEMA_VERSION_UNSUPPORTED", `数据库版本 ${state.maxVersion} 高于当前代码支持的 ${state.latest}`, {
			found: state.maxVersion,
			supported: state.latest
		});
	}
	if (!state.pending.length) {
		return { backupRoot, snapshot: null, rollback() {} };
	}
	const snapshot = createDatabaseSnapshotSync(dbFile, join(backupRoot, "snapshots"), {
		keep: options.keep ?? DEFAULT_BACKUP_KEEP,
		dbSchemaVersion: state.maxVersion,
		label: "pre-migration"
	});
	return {
		backupRoot,
		snapshot,
		rollback(error) {
			if (!error) return null;
			return restoreSnapshotFileSync(snapshot.snapshotPath, dbFile, backupRoot, { reason: "migration-failed" });
		}
	};
}

function isProcessAlive(pid) {
	if (!Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return error?.code === "EPERM";
	}
}

/** 维护锁：CLI 使用，阻止 DSH 运行时同时打开库。 */
export function acquireMaintenanceLockSync(dbFile, options = {}) {
	const lockPath = `${dbFile}.maintenance`;
	ensureDirSync(dirname(dbFile));
	const staleMs = options.staleMs ?? 30 * 60_000;
	const runtimeLock = `${dbFile}.lock`;
	if (existsSync(runtimeLock)) {
		const age = Date.now() - statSync(runtimeLock).mtimeMs;
		if (age < (options.runtimeLockStaleMs ?? 10_000)) {
			throw new PersistenceError("STORE_LOCKED", "DSH 正在使用该数据库，请先停止 DSH 再执行维护操作", {
				lock: runtimeLock,
				ageMs: age
			});
		}
	}
	if (existsSync(lockPath)) {
		const age = Date.now() - statSync(lockPath).mtimeMs;
		let holder = null;
		try {
			holder = JSON.parse(readFileSync(lockPath, "utf8"));
		} catch {}
		const alive = isProcessAlive(holder?.pid);
		if (alive || (!holder && age < staleMs)) {
			throw new PersistenceError("STORE_LOCKED", "另一个维护操作正在进行", { lock: lockPath, ageMs: age, pid: holder?.pid });
		}
		try {
			unlinkSync(lockPath);
		} catch {}
	}
	const fd = openSync(lockPath, "wx", 0o600);
	try {
		writeSync(fd, JSON.stringify({ pid: process.pid, at: new Date().toISOString() }));
		fsyncSync(fd);
	} finally {
		closeSync(fd);
	}
	let released = false;
	return {
		path: lockPath,
		release() {
			if (released) return;
			released = true;
			try {
				unlinkSync(lockPath);
			} catch {}
		}
	};
}

export function assertNoMaintenanceLockSync(dbFile) {
	const lockPath = `${dbFile}.maintenance`;
	if (!existsSync(lockPath)) return;
	const age = Date.now() - statSync(lockPath).mtimeMs;
	let holder = null;
	try {
		holder = JSON.parse(readFileSync(lockPath, "utf8"));
	} catch {}
	if (isProcessAlive(holder?.pid)) {
		throw new PersistenceError("STORE_MAINTENANCE", "检测到维护操作正在进行，暂不启动服务", { lock: lockPath, ageMs: age, pid: holder.pid });
	}
	// 进程已退出或文件已成为陈旧残留：清掉，避免阻塞 30 分钟。
	if (holder?.pid || age > 10 * 60_000) {
		try {
			unlinkSync(lockPath);
		} catch {}
		return;
	}
	throw new PersistenceError("STORE_MAINTENANCE", "检测到无法识别的维护锁，暂不启动服务", { lock: lockPath, ageMs: age });
}
