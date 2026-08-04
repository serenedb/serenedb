import { readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import picomatch from "picomatch";
import type { FetchContext, FetchResult, SourceFile } from "./sources.types";

const ALWAYS_SKIP = new Set([".git", "node_modules", ".docusaurus", "dist", "build"]);
const MAX_FILE_BYTES = 5 * 1024 * 1024;

/** Extensions the parsing service can turn into sections. */
const PARSEABLE = new Set([".md", ".mdx", ".html", ".htm", ".rst", ".txt", ".ipynb", ".pdf"]);

export async function fetchFolder(root: string, ctx: FetchContext): Promise<FetchResult> {
    const files = await collectFiles(root, root, ctx);
    return { files, ref: `fs:${Date.now()}` };
}

export async function collectFiles(
    root: string,
    dir: string,
    ctx: FetchContext,
): Promise<SourceFile[]> {
    const isExcluded = ctx.exclude.length
        ? picomatch(ctx.exclude, { dot: true })
        : () => false;
    const wanted = new Set(ctx.extensions.map((e) => e.toLowerCase()));
    const out: SourceFile[] = [];

    const walk = async (d: string): Promise<void> => {
        let entries;
        try {
            entries = await readdir(d, { withFileTypes: true });
        } catch {
            return;
        }
        for (const entry of entries) {
            if (entry.name.startsWith(".") && entry.isDirectory()) continue;
            if (ALWAYS_SKIP.has(entry.name)) continue;
            const abs = path.join(d, entry.name);
            const rel = path.relative(root, abs).split(path.sep).join("/");
            if (isExcluded(rel)) continue;
            if (entry.isDirectory()) {
                await walk(abs);
            } else if (entry.isFile()) {
                const ext = path.extname(entry.name).toLowerCase();
                if (!wanted.has(ext)) continue;
                const st = await stat(abs);
                if (st.size > MAX_FILE_BYTES) continue; // skip >5MB blobs
                const encoding = ext === ".pdf" ? "base64" : "utf8";
                const content = await readFile(abs, encoding);
                out.push({ path: rel, content, encoding, extension: ext });
                ctx.onProgress?.(out.length);
            }
        }
    };
    await walk(dir);
    return out;
}

/**
 * Collect from a list of entries — subdirectories are walked with the usual
 * filters, explicitly named files are read directly (they bypass the
 * extensions toggle but must be a type the parser understands). All paths
 * stay relative to `root`, so entries from different directories can't
 * collide. Entries that don't exist or escape `root` are skipped, matching
 * the walker's silent handling of unreadable directories.
 */
export async function collectEntries(
    root: string,
    entries: string[],
    ctx: FetchContext,
): Promise<SourceFile[]> {
    const rootAbs = path.resolve(root);
    const out: SourceFile[] = [];
    const seen = new Set<string>();
    const push = (f: SourceFile) => {
        if (!seen.has(f.path)) {
            seen.add(f.path);
            out.push(f);
        }
    };

    for (const entry of entries) {
        const abs = path.resolve(rootAbs, entry);
        if (abs !== rootAbs && !abs.startsWith(rootAbs + path.sep)) continue; // never escape the checkout
        let st;
        try {
            st = await stat(abs);
        } catch {
            continue;
        }
        if (st.isDirectory()) {
            const base = out.length;
            const sub = await collectFiles(rootAbs, abs, {
                ...ctx,
                onProgress: (n, detail) => ctx.onProgress?.(base + n, detail),
            });
            sub.forEach(push);
        } else if (st.isFile() && st.size <= MAX_FILE_BYTES) {
            const ext = path.extname(abs).toLowerCase();
            if (!PARSEABLE.has(ext)) continue;
            const rel = path.relative(rootAbs, abs).split(path.sep).join("/");
            const encoding = ext === ".pdf" ? "base64" : ("utf8" as const);
            push({ path: rel, content: await readFile(abs, encoding), encoding, extension: ext });
            ctx.onProgress?.(out.length);
        }
    }
    return out;
}
