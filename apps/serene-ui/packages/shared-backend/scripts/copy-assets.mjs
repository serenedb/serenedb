import { fileURLToPath } from "url";
import path from "path";
import fs from "fs/promises";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const root = path.resolve(__dirname, "..");

async function copyDir(src, dest) {
    try {
        await fs.mkdir(dest, { recursive: true });
        const entries = await fs.readdir(src, { withFileTypes: true });
        for (const entry of entries) {
            const srcPath = path.join(src, entry.name);
            const destPath = path.join(dest, entry.name);
            if (entry.isDirectory()) {
                await copyDir(srcPath, destPath);
            } else {
                await fs.copyFile(srcPath, destPath);
            }
        }
    } catch (e) {
        if (e && e.code === "ENOENT") return; // source missing is ok
        throw e;
    }
}

async function main() {
    const migrationsSrc = path.join(root, "src", "migrations");
    const migrationsDest = path.join(root, "dist", "migrations");
    await copyDir(migrationsSrc, migrationsDest);
}

main().catch((err) => {
    console.error("[copy-assets] Failed:", err);
    process.exit(1);
});
