// 发布包完整性校验：SHA256、文件清单、禁止文件和 build-info 关联。
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const DIST = join(ROOT, "dist");
const infoPath = join(DIST, "build-info.json");
if (!existsSync(infoPath)) throw new Error("缺少 dist/build-info.json，请先运行 pnpm build");
const info = JSON.parse(readFileSync(infoPath, "utf8"));
const tarballPath = join(DIST, info.tarball);
if (!existsSync(tarballPath)) throw new Error(`缺少发布包 ${info.tarball}`);

const digest = createHash("sha256").update(readFileSync(tarballPath)).digest("hex");
if (digest !== info.tarball_sha256) throw new Error(`SHA256 不匹配：${digest} != ${info.tarball_sha256}`);

const sums = readFileSync(join(DIST, "SHA256SUMS"), "utf8").trim();
const expected = `${info.tarball_sha256}  ${info.tarball}`;
if (sums !== expected) throw new Error("SHA256SUMS 与 build-info 不一致");

const list = spawnSync("tar", ["-tzf", tarballPath], { encoding: "utf8" });
if (list.status !== 0) throw new Error("无法读取发布包文件清单");
const files = list.stdout.split("\n").filter(Boolean);
const required = ["package/package.json", "package/lib/index.js", "package/lib/client.js", "package/lib/index.d.ts", "package/cordis.patch.yml", "package/README.md", "package/LICENSE", "package/CHANGELOG.md"];
for (const entry of required) if (!files.includes(entry)) throw new Error(`发布包缺少 ${entry}`);
const forbidden = [/node_modules\//u, /\.sqlite/u, /\.env/u, /^package\/tests\//u, /^package\/src\//u, /^package\/\.github\//u, /\.log$/u];
for (const entry of files) if (forbidden.some((pattern) => pattern.test(entry))) throw new Error(`发布包含有禁止文件：${entry}`);

if (!existsSync(join(DIST, info.sbom))) throw new Error("缺少 SBOM 文件");
if (!/^[a-f0-9]{40}$/u.test(info.commit)) throw new Error("build-info 缺少合法 Git commit");
if (!Number.isInteger(info.sbom_components) || info.sbom_components < 1) throw new Error("SBOM 组件数为空");
if (info.reproducibility?.packs !== 2 || info.reproducibility?.identical !== true) throw new Error("缺少双次打包可复现证据");

console.log(`包完整性校验通过：${info.tarball}`);
console.log(`SHA256：${info.tarball_sha256}`);
console.log(`Commit：${info.commit_short}，文件数：${files.length}，SBOM 组件：${info.sbom_components}`);
