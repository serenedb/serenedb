import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { existsSync } from "node:fs";
import { mkdir, rm, stat } from "node:fs/promises";
import path from "node:path";
import type { GitSource } from "@serenedb/docs-search-core";
import { collectEntries, collectFiles } from "./folder";
import type { FetchContext, FetchResult } from "./sources.types";

const exec = promisify(execFile);
const GIT_TIMEOUT = 10 * 60 * 1000;

async function git(args: string[], cwd?: string): Promise<string> {
    const { stdout } = await exec("git", args, {
        cwd,
        timeout: GIT_TIMEOUT,
        maxBuffer: 64 * 1024 * 1024,
        env: { ...process.env, GIT_TERMINAL_PROMPT: "0" },
    });
    return stdout.trim();
}

function cloneDir(ctx: FetchContext): string {
    return path.join(ctx.workDir, "repo");
}

/** "docs, guides/faq.md" | ["docs", …] -> clean list of repo-relative entries. */
export function subdirEntries(subdir: string | string[] | undefined): string[] {
    const raw = Array.isArray(subdir) ? subdir : (subdir ?? "").split(",");
    return raw.map((s) => s.trim().replace(/^\/+|\/+$/g, "")).filter(Boolean);
}

/** Current commit sha of the remote branch — cheap check for commit-watch sync. */
export async function remoteHead(source: GitSource): Promise<string | null> {
    const ref = source.branch || "HEAD";
    const out = await git(["ls-remote", source.url, ref]);
    const line = out.split("\n").find(Boolean);
    return line ? line.split(/\s+/)[0] : null;
}

export async function fetchGit(source: GitSource, ctx: FetchContext): Promise<FetchResult> {
    const dir = cloneDir(ctx);
    const branch = source.branch || undefined;

    if (existsSync(path.join(dir, ".git"))) {
        try {
            await git(["fetch", "--depth", "1", "origin", ...(branch ? [branch] : [])], dir);
            await git(
                ["reset", "--hard", branch ? `origin/${branch}` : "FETCH_HEAD"],
                dir,
            );
        } catch {
            // corrupted/diverged clone — start over
            await rm(dir, { recursive: true, force: true });
        }
    }
    if (!existsSync(path.join(dir, ".git"))) {
        await mkdir(path.dirname(dir), { recursive: true });
        const args = ["clone", "--depth", "1", "--single-branch"];
        if (branch) args.push("--branch", branch);
        args.push(source.url, dir);
        await git(args);
    }
    if (source.commit) {
        // pinned commit: shallow clones may not have it — fetch it explicitly
        try {
            await git(["checkout", "--detach", source.commit], dir);
        } catch {
            await git(["fetch", "--depth", "1", "origin", source.commit], dir);
            await git(["checkout", "--detach", source.commit], dir);
        }
    }
    const sha = await git(["rev-parse", "HEAD"], dir);
    const entries = subdirEntries(source.subdir);
    let files;
    if (entries.length === 0) {
        files = await collectFiles(dir, dir, ctx);
    } else if (entries.length === 1 && (await isDirectory(path.join(dir, entries[0])))) {
        // single subdirectory keeps the historical behavior: file paths (and
        // thus section ids / urls) are relative to the subdir, not the repo
        const root = path.join(dir, entries[0]);
        files = await collectFiles(root, root, ctx);
    } else {
        files = await collectEntries(dir, entries, ctx);
    }
    return { files, ref: sha };
}

async function isDirectory(p: string): Promise<boolean> {
    try {
        return (await stat(p)).isDirectory();
    } catch {
        return false;
    }
}
