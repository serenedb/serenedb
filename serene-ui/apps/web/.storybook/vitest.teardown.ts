import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function cleanupScreenshotDirs() {
    const srcDir = path.join(__dirname, "../src");

    async function removeScreenshotDirs(dir: string) {
        try {
            const entries = await fs.readdir(dir, { withFileTypes: true });

            for (const entry of entries) {
                const fullPath = path.join(dir, entry.name);

                if (entry.isDirectory()) {
                    if (entry.name === "__screenshots__") {
                        await fs.rm(fullPath, { recursive: true, force: true });
                        console.log(`✓ Cleaned up: ${fullPath}`);
                    } else {
                        await removeScreenshotDirs(fullPath);
                    }
                }
            }
        } catch (error) {
            // Ignore errors if directory doesn't exist
        }
    }

    await removeScreenshotDirs(srcDir);
    console.log("✓ Screenshot cleanup complete");
}

cleanupScreenshotDirs().catch(console.error);
