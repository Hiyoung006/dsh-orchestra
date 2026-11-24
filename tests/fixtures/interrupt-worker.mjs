import { writeFileSync } from "node:fs";
import { acquireFileLockSync } from "../../lib/persistence/json-store.js";

const file = process.argv[2];
// 持有锁后写入半成品临时文件，随后直接退出，模拟进程被强杀。
acquireFileLockSync(file, { timeoutMs: 2000, staleMs: 60_000 });
writeFileSync(file + ".partial.tmp", '{"counter":');
process.exit(9);
