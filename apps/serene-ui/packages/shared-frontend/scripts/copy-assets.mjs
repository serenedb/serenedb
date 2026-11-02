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

async function copyFile(src, dest) {
    try {
        await fs.mkdir(path.dirname(dest), { recursive: true });
        await fs.copyFile(src, dest);
    } catch (e) {
        if (e && e.code === "ENOENT") return;
        throw e;
    }
}

async function main() {
    const assetsSrc = path.join(root, "src", "shared", "assets");
    const assetsDest = path.join(root, "dist", "shared", "assets");
    await copyDir(assetsSrc, assetsDest);
    console.log(
        `[copy-assets] Copied assets to ${path.relative(root, assetsDest)}`,
    );

    const stylesSrc = path.join(root, "src", "styles.css");
    const stylesDest = path.join(root, "dist", "styles.css");
    await copyFile(stylesSrc, stylesDest);
}

main().catch((err) => {
    console.error("[copy-assets] Failed:", err);
    process.exit(1);
});
