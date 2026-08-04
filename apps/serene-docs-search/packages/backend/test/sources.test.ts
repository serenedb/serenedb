import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { collectEntries, collectFiles } from "../src/services/sources/folder";
import { fetchGit, subdirEntries } from "../src/services/sources/git";
import type { FetchContext } from "../src/services/sources/sources.types";

describe("subdirEntries", () => {
    it("splits a comma-separated string and trims slashes/whitespace", () => {
        expect(subdirEntries("docs, guides/faq.md")).toEqual(["docs", "guides/faq.md"]);
        expect(subdirEntries(" /docs/ ,, ")).toEqual(["docs"]);
    });

    it("accepts an array form and handles empty input", () => {
        expect(subdirEntries(["docs/", " sdk "])).toEqual(["docs", "sdk"]);
        expect(subdirEntries(undefined)).toEqual([]);
        expect(subdirEntries("")).toEqual([]);
    });
});

describe("collectEntries", () => {
    let repo: string;
    const ctx = (over: Partial<FetchContext> = {}): FetchContext => ({
        workDir: repo,
        extensions: [".md"],
        exclude: [],
        ...over,
    });
    const paths = (files: Array<{ path: string }>) => files.map((f) => f.path).sort();

    beforeAll(async () => {
        repo = await mkdtemp(path.join(os.tmpdir(), "sds-sources-"));
        const write = async (rel: string, content = `# ${rel}\n`) => {
            await mkdir(path.dirname(path.join(repo, rel)), { recursive: true });
            await writeFile(path.join(repo, rel), content);
        };
        await write("README.md");
        await write("docs/intro.md");
        await write("docs/api/ref.md");
        await write("docs/skip.txt");
        await write("guides/faq.md");
        await write("notes.txt", "plain notes\n");
        await write("logo.svg", "<svg/>");
        await write("node_modules/pkg/x.md");
        // a sibling outside the checkout — the escape guard must never reach it
        await writeFile(path.join(repo, "..", "sds-outside.md"), "# outside\n");
    });

    afterAll(async () => {
        await rm(repo, { recursive: true, force: true });
        await rm(path.join(repo, "..", "sds-outside.md"), { force: true });
    });

    it("walks several directories, keeping paths relative to the repo root", async () => {
        const files = await collectEntries(repo, ["docs", "guides"], ctx());
        expect(paths(files)).toEqual(["docs/api/ref.md", "docs/intro.md", "guides/faq.md"]);
    });

    it("directory walks respect the extensions toggle and exclude patterns", async () => {
        const files = await collectEntries(repo, ["docs"], ctx({ exclude: ["**/api/**"] }));
        expect(paths(files)).toEqual(["docs/intro.md"]); // skip.txt filtered, api/ excluded
    });

    it("explicitly named files bypass the extensions toggle but must be parseable", async () => {
        const files = await collectEntries(repo, ["notes.txt", "logo.svg"], ctx());
        expect(paths(files)).toEqual(["notes.txt"]);
        expect(files[0].content).toBe("plain notes\n");
        expect(files[0].extension).toBe(".txt");
    });

    it("dedupes overlapping entries", async () => {
        const files = await collectEntries(repo, ["docs", "docs/intro.md", "docs"], ctx());
        expect(paths(files)).toEqual(["docs/api/ref.md", "docs/intro.md"]);
    });

    it("skips missing entries and anything escaping the checkout", async () => {
        const files = await collectEntries(
            repo,
            ["no-such-dir", "../sds-outside.md", "../../etc"],
            ctx(),
        );
        expect(files).toEqual([]);
    });

    it("single-directory legacy mode stays subdir-relative (collectFiles contract)", async () => {
        const root = path.join(repo, "docs");
        const files = await collectFiles(root, root, ctx());
        expect(paths(files)).toEqual(["api/ref.md", "intro.md"]);
    });
});

describe("fetchGit subdir handling (local repo)", () => {
    let upstream: string;
    let base: string;
    const paths = (files: Array<{ path: string }>) => files.map((f) => f.path).sort();
    const fetchCtx = async (): Promise<FetchContext> => ({
        workDir: await mkdtemp(path.join(base, "work-")),
        extensions: [".md"],
        exclude: [],
    });

    beforeAll(async () => {
        base = await mkdtemp(path.join(os.tmpdir(), "sds-git-"));
        upstream = path.join(base, "upstream");
        for (const rel of ["docs/intro.md", "guides/faq.md", "internal/secret.md"]) {
            await mkdir(path.dirname(path.join(upstream, rel)), { recursive: true });
            await writeFile(path.join(upstream, rel), `# ${rel}\n`);
        }
        const git = (...args: string[]) =>
            execFileSync("git", args, { cwd: upstream, stdio: "pipe" });
        git("init", "-q", "-b", "main");
        git("-c", "user.email=t@t", "-c", "user.name=t", "add", ".");
        git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "init");
    });

    afterAll(async () => {
        await rm(base, { recursive: true, force: true });
    });

    it("comma-separated subdirs and files index repo-relative paths", async () => {
        const res = await fetchGit(
            { type: "git", url: upstream, branch: "main", subdir: "docs, guides/faq.md" },
            await fetchCtx(),
        );
        expect(paths(res.files)).toEqual(["docs/intro.md", "guides/faq.md"]);
        expect(res.ref).toMatch(/^[0-9a-f]{40}$/);
    });

    it("a single subdirectory keeps the historical subdir-relative paths", async () => {
        const res = await fetchGit(
            { type: "git", url: upstream, branch: "main", subdir: "docs" },
            await fetchCtx(),
        );
        expect(paths(res.files)).toEqual(["intro.md"]);
    });
});
