// 运行时接入层：启动守卫、事务开始重试。
// 这里不引入 DSH 依赖，便于在 CLI / 单测中复用。

import { sleepSync } from "./json-store.js";
import {
	assertNoMaintenanceLockSync,
	prepareServiceStorageSync
} from "./sqlite-backup.js";

export {
	assertNoMaintenanceLockSync,
	prepareServiceStorageSync
};

export function isSqliteBusyError(error) {
	return /SQLITE_BUSY|SQLITE_LOCKED|database is locked|database table is locked/i.test(String(error?.message ?? ""));
}

/**
 * BEGIN IMMEDIATE 遇到 BUSY/LOCKED 时做有界重试。
 * 只重试“开始事务”这一步，绝不重放已经执行了一部分的业务函数。
 */
export function beginImmediateWithRetry(raw, options = {}) {
	const attempts = Math.max(1, options.attempts ?? 3);
	for (let i = 0; ; i += 1) {
		try {
			raw.exec("BEGIN IMMEDIATE");
			return;
		} catch (error) {
			if (!isSqliteBusyError(error) || i >= attempts - 1) throw error;
			sleepSync(25 * 2 ** i);
		}
	}
}
