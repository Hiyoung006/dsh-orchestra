// 语法与公开接口面检查。lib 是本仓库的唯一源码，因此不运行 TS 编译器。
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const checkRoots = ["lib", "scripts", "tests"];
const failures = [];
let checked = 0;

function walk(dir) {
	for (const name of readdirSync(dir)) {
		const path = join(dir, name);
		const info = statSync(path);
		if (info.isDirectory()) {
			if (name === "node_modules" || name === ".git") continue;
			walk(path);
			continue;
		}
		if (!/\.(?:m?js)$/u.test(name)) continue;
		checked += 1;
		try {
			execFileSync(process.execPath, ["--check", path], { stdio: "pipe" });
		} catch (error) {
			failures.push(`${relative(ROOT, path)}: ${String(error.stderr || error.message).trim()}`);
		}
	}
}

for (const root of checkRoots) walk(join(ROOT, root));

const pkg = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8"));
const required = [
	pkg.main,
	pkg.types,
	pkg.exports?.["."]?.default,
	pkg.exports?.["."]?.types,
	pkg.exports?.["./client"],
	...(pkg.files ?? [])
];
for (const entry of required) {
	if (typeof entry !== "string") continue;
	if (!existsSync(join(ROOT, entry))) failures.push(`package.json 引用了不存在的文件：${entry}`);
}
const declarations = readFileSync(join(ROOT, pkg.types), "utf8");
if (!/\bexport\b/u.test(declarations)) failures.push(`${pkg.types} 没有导出声明`);

if (failures.length > 0) {
	console.error(failures.join("\n"));
	console.error(`\n类型/语法检查失败：${failures.length} 项，已检查 ${checked} 个 JS 文件。`);
	process.exit(1);
}
console.log(`类型/语法检查通过：${checked} 个 JS 文件，公开导出与声明文件完整。`);
