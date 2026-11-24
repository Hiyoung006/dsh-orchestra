import { acquireFileLockSync, readJsonSync, writeJsonAtomicSync } from "../../lib/persistence/json-store.js";

const [file, id] = process.argv.slice(2);
const lock = acquireFileLockSync(file, { timeoutMs: 5000, staleMs: 20_000 });
try {
	const document = readJsonSync(file);
	document.counter += 1;
	document.writers.push(id);
	await new Promise((resolve) => setTimeout(resolve, 40));
	writeJsonAtomicSync(file, document);
} finally {
	lock.release();
}
