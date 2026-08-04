import { describe, expect, it } from "vitest";
import {
    BUNDLED_OLLAMA_URL,
    DEFAULT_BACKEND_PORT,
    defaultConfig,
    defaultProvider,
    describeSource,
    generateToken,
    parseInterval,
} from "../src/defaults";

describe("parseInterval", () => {
    it("parses the units the wizard offers", () => {
        expect(parseInterval("15m")).toBe(15 * 60_000);
        expect(parseInterval("1h")).toBe(3_600_000);
        expect(parseInterval("24h")).toBe(24 * 3_600_000);
        expect(parseInterval("90s")).toBe(90_000);
        expect(parseInterval("2d")).toBe(2 * 86_400_000);
    });

    it("is whitespace- and case-tolerant", () => {
        expect(parseInterval(" 1H ")).toBe(3_600_000);
        expect(parseInterval("10 m")).toBe(600_000);
    });

    it("returns null for junk", () => {
        expect(parseInterval(undefined)).toBeNull();
        expect(parseInterval("")).toBeNull();
        expect(parseInterval("soon")).toBeNull();
        expect(parseInterval("15")).toBeNull();
        expect(parseInterval("h1")).toBeNull();
    });
});

describe("generateToken", () => {
    it("emits sk-local-<24 hex> and does not repeat", () => {
        const a = generateToken();
        const b = generateToken();
        expect(a).toMatch(/^sk-local-[0-9a-f]{24}$/);
        expect(a).not.toBe(b);
    });
});

describe("defaultProvider", () => {
    it("picks role-appropriate models per kind", () => {
        expect(defaultProvider("openai", "answers")).toMatchObject({ model: "gpt-4o-mini" });
        expect(defaultProvider("openai", "embeddings")).toMatchObject({
            model: "text-embedding-3-small",
            baseUrl: "https://api.openai.com/v1",
        });
        expect(defaultProvider("ollama", "embeddings")).toMatchObject({
            model: "nomic-embed-text",
            baseUrl: BUNDLED_OLLAMA_URL,
        });
    });
});

describe("defaultConfig", () => {
    it("watches commits for git sources and polls for everything else", () => {
        expect(defaultConfig({ type: "git", url: "https://x/y", branch: "main" }).sync.mode).toBe(
            "commits",
        );
        expect(defaultConfig({ type: "folder", path: "./docs" }).sync.mode).toBe("poll");
    });

    it("ships sane content defaults", () => {
        const c = defaultConfig({ type: "folder", path: "./docs" });
        expect(c.content.extensions).toEqual([".md", ".mdx"]);
        expect(c.content.markdown?.mode).toBe("split");
        expect(c.content.urlMapping?.stripExtensions).toBe(true);
        expect(c.server?.port).toBe(DEFAULT_BACKEND_PORT);
    });
});

describe("describeSource", () => {
    it("summarizes each source type", () => {
        expect(
            describeSource({ type: "git", url: "https://github.com/acme/docs.git", branch: "main" }),
        ).toBe("github.com/acme/docs @ main");
        expect(
            describeSource({ type: "git", url: "https://x/y", branch: "main", commit: "abc123" }),
        ).toBe("x/y @ abc123");
        expect(describeSource({ type: "folder", path: "/data/docs" })).toBe("/data/docs (mounted)");
        expect(describeSource({ type: "site", url: "https://docs.acme.dev" })).toBe(
            "https://docs.acme.dev · depth 2",
        );
        expect(describeSource({ type: "bucket", uri: "s3://acme/docs" })).toBe("s3://acme/docs");
    });
});
