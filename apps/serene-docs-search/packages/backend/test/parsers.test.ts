import { describe, expect, it } from "vitest";
import { ParsingService } from "../src/services/parsing";
import { parseHtml } from "../src/services/parsing/html";
import { parseMarkdown } from "../src/services/parsing/markdown";
import { parseNotebook, parseRst } from "../src/services/parsing/misc";
import { slugify } from "../src/services/parsing/section";
import { generateCompose, defaultConfig } from "@serenedb/docs-search-core";
import { chatCompletionsUrl, resolveProvider } from "../src/utils/providers";
import { parseQuery } from "../src/utils/query";
import { makeSnippet } from "../src/utils/snippet";
import { normalizeConfig } from "../src/config";
import { vocabFrequencies } from "../src/repositories/vocab";
import { RankingService } from "../src/services/ranking";
import { correctSpelling, type VocabLookup } from "../src/services/spelling";
import { mapUrl } from "../src/utils/urlmap";
import type { ContentConfig, SearchResultItem } from "@serenedb/docs-search-core";

const MD = `---
title: Read replicas
sidebar_position: 3
---

import Tabs from '@theme/Tabs';

Intro paragraph about replication.

## Add a replica {#add-replica}

Run \`ALTER CLUSTER\` to attach a node.

\`\`\`sql
ALTER CLUSTER docs ADD NODE 'node-3:5433' ROLE replica;
\`\`\`

### Verify status

Check **replication status** with [this function](/api).

## Add a replica

Duplicate heading gets a numbered slug.
`;

describe("parseMarkdown", () => {
    const res = parseMarkdown(MD, { mode: "split" });

    it("takes the title from frontmatter", () => {
        expect(res.docTitle).toBe("Read replicas");
    });

    it("splits by h1–h3 and keeps preamble as a level-0 section", () => {
        expect(res.sections.map((s) => s.level)).toEqual([0, 2, 3, 2]);
        expect(res.sections[0].content).toContain("Intro paragraph");
        expect(res.sections[0].content).not.toContain("import Tabs");
    });

    it("honours explicit {#id} anchors", () => {
        expect(res.sections[1].anchor).toBe("add-replica");
    });

    it("dedupes generated slugs", () => {
        expect(res.sections[3].anchor).toBe("add-a-replica");
        expect(res.sections[2].anchor).toBe("verify-status");
    });

    it("keeps code block content but strips fence markers", () => {
        expect(res.sections[1].content).toContain("ALTER CLUSTER docs");
        expect(res.sections[1].content).not.toContain("```");
    });

    it("strips inline markdown", () => {
        expect(res.sections[2].content).toContain("replication status");
        expect(res.sections[2].content).toContain("this function");
        expect(res.sections[2].content).not.toContain("**");
    });

    it("whole mode yields a single section", () => {
        const whole = parseMarkdown(MD, { mode: "whole" });
        expect(whole.sections).toHaveLength(1);
        expect(whole.sections[0].title).toBe("Read replicas");
    });

    it("ignores headings inside code fences", () => {
        const tricky = "# Top\n\n```sh\n# not a heading\necho hi\n```\n\ntext";
        const r = parseMarkdown(tricky, { mode: "split" });
        expect(r.sections).toHaveLength(1);
        expect(r.sections[0].title).toBe("Top");
    });

    it("splits h4 API-reference headings by default (function lookups)", () => {
        const api =
            "## TSQUERY Constructors\n\nintro\n\n" +
            "#### `ts_starts_with(prefix)` {#ts_starts_with}\n\nPrefix matcher.\n\n" +
            "#### `plainto_tsquery(text)` {#plainto_tsquery}\n\nAnalyzed constructor.\n";
        const r = parseMarkdown(api, { mode: "split" });
        expect(r.sections.map((s) => s.title)).toEqual([
            "TSQUERY Constructors",
            "ts_starts_with(prefix)",
            "plainto_tsquery(text)",
        ]);
        expect(r.sections[1].anchor).toBe("ts_starts_with");
        expect(r.sections[1].level).toBe(4);
        expect(r.sections[2].content).toContain("Analyzed constructor");
    });

    it("collects fenced code into the code field", () => {
        const md =
            "## Usage\n\nCall it like this:\n\n```sql\nSELECT * FROM t WHERE body @@ plainto_tsquery('quick brown');\n```\n\nprose after\n";
        const r = parseMarkdown(md, { mode: "split" });
        expect(r.sections).toHaveLength(1);
        expect(r.sections[0].code).toContain("body @@ plainto_tsquery('quick brown')");
        expect(r.sections[0].code).not.toContain("```");
        expect(r.sections[0].content).toContain("prose after");
    });

    it("honours a shallower depth override", () => {
        const api = "## Group\n\n#### `fn(x)` {#fn}\n\nBody.\n";
        const r = parseMarkdown(api, { mode: "split", depth: 3 });
        expect(r.sections).toHaveLength(1);
        expect(r.sections[0].title).toBe("Group");
        expect(r.sections[0].content).toContain("Body.");
    });
});

describe("slugify", () => {
    it("matches github/docusaurus slugs", () => {
        expect(slugify("Add a replica")).toBe("add-a-replica");
        expect(slugify("What's `ai_embed()`?")).toBe("whats-ai_embed");
        expect(slugify("Hybrid search: lexical & semantic")).toBe("hybrid-search-lexical--semantic");
    });
});

describe("parseHtml", () => {
    const HTML = `<html><head><title>Page</title></head><body>
      <nav><a href="/">skip me</a></nav>
      <article>
        <h1 id="intro">Intro</h1>
        <p>First paragraph.</p>
        <h2>Usage</h2>
        <p>Second paragraph.</p>
        <pre><code>npm install serene</code></pre>
        <ul><li>item one</li><li>item two</li></ul>
      </article>
      <footer><p>footer junk</p></footer>
    </body></html>`;

    it("scopes to selectors and splits on headings", () => {
        const res = parseHtml(HTML, { selectors: "article", tags: ["h1", "h2", "h3", "p", "li", "pre"] });
        expect(res.sections.map((s) => s.title)).toEqual(["Intro", "Usage"]);
        expect(res.sections[0].anchor).toBe("intro");
        expect(res.sections[1].content).toContain("npm install serene");
        expect(res.sections[1].content).toContain("item one");
        const all = res.sections.map((s) => s.content).join(" ");
        expect(all).not.toContain("skip me");
        expect(all).not.toContain("footer junk");
    });

    it("any h1–h6 named in tags opens a section (API references live at h4)", () => {
        const html = `<article>
          <h2 id="ctors">Constructors</h2><p>intro</p>
          <h4 id="ts_phrase">ts_phrase(text)</h4><p>Phrase matcher.</p>
          <h4 id="ts_tokenize">ts_tokenize(text)</h4><p>Tokenizer.</p>
        </article>`;
        const res = parseHtml(html, { selectors: "article", tags: ["h2", "h4", "p"] });
        expect(res.sections.map((s) => [s.title, s.level])).toEqual([
            ["Constructors", 2],
            ["ts_phrase(text)", 4],
            ["ts_tokenize(text)", 4],
        ]);
        expect(res.sections[1].anchor).toBe("ts_phrase");
    });

    it("renders tables row by row instead of gluing cell texts", () => {
        const html = `<article><h1>Compat</h1><table>
          <tr><th>Feature</th><th>Support State</th><th>Details</th></tr>
          <tr><td>CREATE TABLE</td><td>Yes</td><td></td></tr>
          <tr><td>Foreign Keys</td><td>Yes</td><td>Enforced; over-eager on some <code>UPDATE</code>s</td></tr>
        </table></article>`;
        const res = parseHtml(html, { tags: ["h1", "table", "code"] });
        const s = res.sections[0];
        expect(s.content).toContain("Feature · Support State · Details");
        expect(s.content).toContain("CREATE TABLE · Yes");
        expect(s.content).toContain("Foreign Keys · Yes · Enforced; over-eager on some UPDATEs");
        expect(s.content).not.toContain("FeatureSupport");
        // inline code inside a cell feeds the code field, not duplicate content
        expect(s.code).toContain("UPDATE");
        expect(s.content.match(/UPDATE/g)).toHaveLength(1);
    });

    it("excludeSelectors drops matched elements before extraction", () => {
        const html = `<article><h1>X</h1>
          <p>Real prose.</p>
          <pre class="language-sql"><code>SELECT 1;</code></pre>
          <pre class="language-plaintext"><code>1 quick brown</code></pre>
          <p class="badge">v2.1 badge junk</p>
        </article>`;
        const res = parseHtml(html, {
            tags: ["h1", "p", "pre"],
            excludeSelectors: "pre.language-plaintext, .badge",
        });
        const s = res.sections[0];
        expect(s.content).toContain("Real prose.");
        expect(s.content).toContain("SELECT 1;");
        expect(s.content).not.toContain("quick brown");
        expect(s.content).not.toContain("badge junk");
        expect(s.code).toBe("SELECT 1;");
    });

    it("rejects an invalid excludeSelectors with a message naming the knob", () => {
        expect(() =>
            parseHtml("<p>x</p>", { excludeSelectors: "p:::nope" }),
        ).toThrowError(/excludeSelectors/);
    });

    it("drops a near-empty heading-less preamble instead of emitting a stub section", () => {
        const html = `<article><span class="badge"><p>v2.1</p></span>
          <header><h1>Parquet Export</h1></header><p>To export data, use COPY.</p></article>`;
        const res = parseHtml(html, { selectors: "article", tags: ["h1", "p"] });
        expect(res.sections).toHaveLength(1);
        expect(res.sections[0].title).toBe("Parquet Export");
        // a real preamble (>= 30 chars) still becomes a level-0 section
        const withIntro = parseHtml(
            `<article><p>A long introduction paragraph before any heading at all.</p><h1>T</h1><p>Body text here.</p></article>`,
            { selectors: "article", tags: ["h1", "p"] },
        );
        expect(withIntro.sections.map((s) => s.level)).toEqual([0, 1]);
    });

    it("keeps the h1 inside an article <header> and gives it no junk anchor", () => {
        const html = `<div id="__layout"><article><header><h1>TSQUERY</h1></header>
          <p>Intro.</p><h2 id="producing">Producing</h2><p>Body.</p></article></div>`;
        const res = parseHtml(html, { selectors: "article", tags: ["h1", "h2", "p"] });
        expect(res.sections.map((s) => [s.title, s.anchor])).toEqual([
            ["TSQUERY", undefined],
            ["Producing", "producing"],
        ]);
    });

    it("preserves line breaks in highlighter markup (prism token-line spans)", () => {
        const html = `<article><h1>X</h1><pre><code>` +
            `<span class="token-line"><span class="token">statement ok</span></span>` +
            `<span class="token-line"><span class="token">CREATE TABLE t (id INT)</span></span>` +
            `</code></pre></article>`;
        const res = parseHtml(html, { tags: ["h1", "pre"] });
        expect(res.sections[0].code).toBe("statement ok\nCREATE TABLE t (id INT)");
        expect(res.sections[0].content).not.toContain("okCREATE");
    });

    it("feeds inline <code> to the code field without duplicating content", () => {
        const html = `<article><h1>X</h1>
          <p>Call <code>ts_starts_with(prefix)</code> for prefixes.</p>
          <code>bare_snippet()</code>
        </article>`;
        const res = parseHtml(html, { tags: ["h1", "p", "code"] });
        const s = res.sections[0];
        // inline code text appears once in content (via the <p>), and in code
        expect(s.content.match(/ts_starts_with/g)).toHaveLength(1);
        expect(s.code).toContain("ts_starts_with(prefix)");
        // a bare <code> outside any counted container still lands in content
        expect(s.content).toContain("bare_snippet()");
        expect(s.code).toContain("bare_snippet()");
    });
});

describe("parseRst / parseNotebook", () => {
    it("splits rst on underlined headers", () => {
        const rst = "Title\n=====\n\nBody text.\n\nSection\n-------\n\nMore text.\n";
        const res = parseRst(rst);
        expect(res.map((s) => s.title)).toEqual(["Title", "Section"]);
        expect(res[1].level).toBe(2);
    });

    it("extracts notebook markdown headings and code cells", () => {
        const nb = JSON.stringify({
            cells: [
                { cell_type: "markdown", source: ["# Analysis\n", "Some prose"] },
                { cell_type: "code", source: "print('hi')" },
            ],
        });
        const res = parseNotebook(nb);
        expect(res).toHaveLength(1);
        expect(res[0].title).toBe("Analysis");
        expect(res[0].content).toContain("print('hi')");
    });
});

describe("mapUrl", () => {
    it("strips prefix and extension, resolves index files", () => {
        const m = { baseUrl: "/docs", stripPrefix: "docs/", stripExtensions: true };
        expect(mapUrl("docs/replication/read-replicas.md", m)).toBe("/docs/replication/read-replicas");
        expect(mapUrl("docs/replication/index.md", m)).toBe("/docs/replication");
        expect(mapUrl("docs/README.md", m)).toBe("/docs");
    });

    it("keeps absolute base urls intact", () => {
        expect(mapUrl("guide.md", { baseUrl: "https://docs.acme.dev" })).toBe(
            "https://docs.acme.dev/guide",
        );
    });

    it("maps one corpus to multiple canonical sites with ordered rules", () => {
        const mapping = {
            stripExtensions: true,
            indexFiles: ["index"],
            rules: [
                {
                    match: "docs/**",
                    baseUrl: "https://docs.acme.dev",
                    stripPrefix: "docs/",
                },
                {
                    match: "blog/**",
                    baseUrl: "https://blog.acme.dev",
                    stripPrefix: "blog/",
                },
            ],
        };
        expect(mapUrl("docs/installation/index.html", mapping)).toBe(
            "https://docs.acme.dev/installation",
        );
        expect(mapUrl("blog/release-notes/index.html", mapping)).toBe(
            "https://blog.acme.dev/release-notes",
        );
        expect(
            mapUrl("sql/select/index.html", {
                ...mapping,
                rules: [
                    mapping.rules[1],
                    { match: "**", baseUrl: "https://docs.acme.dev" },
                ],
            }),
        ).toBe("https://docs.acme.dev/sql/select");
    });
});

describe("parseFile", () => {
    const content: ContentConfig = {
        extensions: [".md"],
        markdown: { mode: "split" },
        urlMapping: { baseUrl: "/docs", stripPrefix: "docs/" },
    };

    it("produces sections with urls, crumbs and groups", async () => {
        const sections = await ParsingService.parseFile(
            { path: "docs/replication/read-replicas.md", content: MD, extension: ".md" },
            content,
        );
        expect(sections[0].url).toBe("/docs/replication/read-replicas");
        expect(sections[1].url).toBe("/docs/replication/read-replicas#add-replica");
        expect(sections[1].crumb).toBe("Replication › Read replicas");
        expect(sections[1].group).toBe("Replication");
        expect(sections[0].hash).toHaveLength(64);
    });

    it("changes snapshot hashes when URL mapping changes", async () => {
        const file = {
            path: "docs/replication/read-replicas.md",
            content: MD,
            extension: ".md",
        };
        const relative = await ParsingService.parseFile(file, content);
        const absolute = await ParsingService.parseFile(file, {
            ...content,
            urlMapping: {
                ...content.urlMapping,
                baseUrl: "https://docs.acme.dev",
            },
        });

        expect(absolute[0].url).toBe("https://docs.acme.dev/replication/read-replicas");
        expect(absolute[0].hash).not.toBe(relative[0].hash);
    });
});

describe("vocabFrequencies", () => {
    const section = (title: string, content: string) => ({
        id: title,
        path: "x.md",
        url: "/x",
        title,
        crumb: "",
        group: "",
        kind: "text" as const,
        level: 2,
        content,
        code: "",
        hash: "h",
    });

    it("counts surface forms with the query tokenization ('_' splits too)", () => {
        const freqs = vocabFrequencies([
            section("Vacuum", "Run VACUUM to refresh. ts_starts_with helps."),
            section("Refresh", "vacuum again"),
        ]);
        expect(freqs.get("vacuum")).toBe(3);
        expect(freqs.get("refresh")).toBe(2);
        // code identifiers contribute their parts
        expect(freqs.get("starts")).toBe(1);
        expect(freqs.get("with")).toBe(1);
    });

    it("drops single characters and oversized blobs", () => {
        const freqs = vocabFrequencies([
            section("T", `a b ${"x".repeat(40)} ok`),
        ]);
        expect(freqs.has("a")).toBe(false);
        expect(freqs.has("x".repeat(40))).toBe(false);
        expect(freqs.get("ok")).toBe(1);
    });
});

describe("correctSpelling", () => {
    const fakeVocab = (opts: {
        known?: string[];
        prefixes?: string[];
        suggestions?: Record<string, string>;
        fail?: { code?: string };
    }) => {
        const suggestCalls: string[] = [];
        const lookup: VocabLookup = {
            existing: async (terms) => {
                if (opts.fail) throw Object.assign(new Error("boom"), opts.fail);
                return new Set(terms.filter((t) => opts.known?.includes(t)));
            },
            hasPrefix: async (p) => opts.prefixes?.some((w) => w.startsWith(p)) ?? false,
            suggest: async (term, maxDist) => {
                suggestCalls.push(`${term}:${maxDist}`);
                const s = opts.suggestions?.[term];
                return s ? { term: s, sim: 0.8, freq: 10 } : null;
            },
        };
        return { lookup, suggestCalls };
    };

    it("replaces a misspelled word, leaving known words alone", async () => {
        const { lookup } = fakeVocab({ known: ["refresh"], suggestions: { vacum: "vacuum" } });
        expect(await correctSpelling(lookup, "vacum refresh")).toBe("vacuum refresh");
    });

    it("returns undefined when every word is in the vocabulary", async () => {
        const { lookup, suggestCalls } = fakeVocab({ known: ["vacuum", "refresh"] });
        expect(await correctSpelling(lookup, "vacuum refresh")).toBeUndefined();
        expect(suggestCalls).toEqual([]);
    });

    it("skips short and numeric tokens entirely", async () => {
        const { lookup, suggestCalls } = fakeVocab({ suggestions: {} });
        expect(await correctSpelling(lookup, "ts 12345 abc")).toBeUndefined();
        expect(suggestCalls).toEqual([]);
    });

    it("scales the edit budget with token length and dedupes repeats", async () => {
        const { lookup, suggestCalls } = fakeVocab({
            suggestions: { vacum: "vacuum", levenstein: "levenshtein" },
        });
        const corrected = await correctSpelling(lookup, "vacum vacum levenstein!");
        expect(corrected).toBe("vacuum vacuum levenshtein!");
        expect(suggestCalls.sort()).toEqual(["levenstein:2", "vacum:1"]);
    });

    it("preserves quotes and never resurrects negated words", async () => {
        const { lookup, suggestCalls } = fakeVocab({
            known: ["read"],
            suggestions: { replika: "replica", deprekated: "deprecated" },
        });
        const corrected = await correctSpelling(lookup, '"read replika" -deprekated');
        expect(corrected).toBe('"read replica" -deprekated');
        expect(suggestCalls).toEqual(["replika:1"]);
    });

    it("leaves a trailing token alone while it is a live prefix (as-you-type)", async () => {
        const opts = { prefixes: ["stemming"], suggestions: { stemmin: "stemming" } };
        expect(await correctSpelling(fakeVocab(opts).lookup, "stemmin")).toBeUndefined();
        // a trailing space means the word is finished — correct it
        expect(await correctSpelling(fakeVocab(opts).lookup, "stemmin ")).toBe("stemming ");
        // non-trailing occurrences are complete words — correct them too
        const { lookup } = fakeVocab({ ...opts, known: ["rules"] });
        expect(await correctSpelling(lookup, "stemmin rules")).toBe("stemming rules");
    });

    it("returns undefined when nothing within distance exists", async () => {
        const { lookup } = fakeVocab({ known: ["docs"], suggestions: {} });
        expect(await correctSpelling(lookup, "blockchain docs")).toBeUndefined();
    });

    it("degrades silently while the vocab table is missing", async () => {
        const { lookup } = fakeVocab({ fail: { code: "42P01" } });
        expect(await correctSpelling(lookup, "vacum refresh")).toBeUndefined();
    });
});

describe("config normalization + compose", () => {
    it("lifts the legacy flat ai shape into answers/embeddings providers", () => {
        const legacy = {
            ...defaultConfig({ type: "folder", path: "/data/docs" }),
            ai: {
                enabled: true,
                baseUrl: "http://host:11434",
                apiKey: "k",
                model: "qwen2.5:0.5b",
                embeddingsModel: "all-minilm",
            } as never,
        };
        const n = normalizeConfig(legacy);
        expect(n.ai?.answers).toEqual({
            kind: "openai", baseUrl: "http://host:11434", apiKey: "k", model: "qwen2.5:0.5b",
        });
        expect(n.ai?.embeddings?.model).toBe("all-minilm");
        // already-normalized configs pass through untouched
        expect(normalizeConfig(n)).toBe(n);
    });

    it("bundles an ollama service only when a provider points at it", () => {
        const config = defaultConfig({ type: "git", url: "https://x/y", branch: "main" });
        config.search.type = "hybrid";
        config.ai = {
            enabled: true,
            answers: { kind: "openai", baseUrl: "https://api.openai.com/v1", apiKey: "${OPENAI_API_KEY}", model: "gpt-4o-mini" },
            embeddings: { kind: "ollama", baseUrl: "http://ollama:11434", model: "nomic-embed-text" },
        };
        const yml = generateCompose({ config, token: "sk-t" });
        expect(yml).toContain("ollama/ollama");
        expect(yml).toContain("ollama-data:/root/.ollama");
        expect(yml).toContain("depends_on: [serenedb, ollama]");
        expect(yml).toContain("OPENAI_API_KEY: ${OPENAI_API_KEY}");

        config.ai.embeddings = { kind: "ollama", baseUrl: "http://host.docker.internal:11434", model: "all-minilm" };
        expect(generateCompose({ config, token: "sk-t" })).not.toContain("ollama/ollama");
    });

    it("adds the MCP service only when mcp.enabled is set", () => {
        const config = defaultConfig({ type: "git", url: "https://x/y", branch: "main" });
        expect(generateCompose({ config, token: "sk-t" })).not.toContain("docs-search-mcp");

        config.mcp = { enabled: true };
        const yml = generateCompose({ config, token: "sk-t" });
        expect(yml).toContain("docs-search-mcp:");
        expect(yml).toContain("serenedb/docs-search-mcp:latest");
        expect(yml).toContain('"--backend", "http://search-backend:7700"');
        expect(yml).toContain('"--token", "sk-t"');
        expect(yml).toContain("depends_on: [search-backend]");
    });
});

describe("provider url handling", () => {
    it("bare hosts (ollama/vllm) get the /v1 segment", () => {
        expect(resolveProvider("http://localhost:11434")).toEqual({
            baseUrl: "http://localhost:11434",
            embeddingsPath: "/v1/embeddings",
        });
        expect(chatCompletionsUrl("http://localhost:11434")).toBe(
            "http://localhost:11434/v1/chat/completions",
        );
    });

    it("path-carrying bases (gemini/openrouter) are used as the root", () => {
        expect(resolveProvider("https://generativelanguage.googleapis.com/v1beta/openai/")).toEqual({
            baseUrl: "https://generativelanguage.googleapis.com/v1beta/openai",
            embeddingsPath: "/embeddings",
        });
        expect(chatCompletionsUrl("https://generativelanguage.googleapis.com/v1beta/openai")).toBe(
            "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
        );
        expect(chatCompletionsUrl("https://openrouter.ai/api/v1")).toBe(
            "https://openrouter.ai/api/v1/chat/completions",
        );
    });

    it("openai default needs no base_url on the secret", () => {
        expect(resolveProvider("https://api.openai.com/v1")).toBeNull();
        expect(chatCompletionsUrl(undefined)).toBe("https://api.openai.com/v1/chat/completions");
    });
});

describe("rerankByTitle", () => {
    const hit = (id: string, title: string): SearchResultItem => ({
        id,
        url: `/${id}`,
        path: id,
        title,
        crumb: "",
        group: "",
        kind: "heading",
    });

    it("puts the exact-title match above a superset title", () => {
        const fused = [
            hit("a", "Date Part Extraction Functions"),
            hit("b", "Date Part Functions"),
            hit("c", "Date Functions"),
        ];
        const ranked = RankingService.rerankByTitle("date part functions", fused);
        expect(ranked.map((r) => r.id)).toEqual(["b", "a", "c"]);
    });

    it("treats the trailing token as a prefix while the user is still typing", () => {
        const fused = [
            hit("a", "Date Part Extraction Functions"),
            hit("b", "Date Part Functions"),
        ];
        expect(RankingService.rerankByTitle("date part functio", fused).map((r) => r.id)).toEqual(["b", "a"]);
        expect(RankingService.rerankByTitle("date part f", fused).map((r) => r.id)).toEqual(["b", "a"]);
        // a complete mismatch in the middle must not get the prefix leniency
        expect(RankingService.rerankByTitle("date extraction functio", fused)[0].id).toBe("a");
    });

    it("is plural-insensitive; the literal surface form wins the tie", () => {
        const fused = [hit("x", "Vector search"), hit("y", "Hybrid search"), hit("z", "Vector Searches")];
        const ranked = RankingService.rerankByTitle("vector searches", fused);
        // both x and z are tier 2 (plural fold); z matches the typed string
        // literally, so the raw-prefix tie-break puts it first
        expect(ranked.map((r) => r.id)).toEqual(["z", "x", "y"]);
    });

    it("leaves order alone when nothing matches by title", () => {
        const fused = [hit("a", "Alpha"), hit("b", "Beta")];
        expect(RankingService.rerankByTitle("quorum", fused).map((r) => r.id)).toEqual(["a", "b"]);
    });

    it("sinks recurring boilerplate titles below distinct ones", () => {
        const fused = [
            hit("s1", "See also"),
            hit("hs", "Hybrid Search"),
            hit("ch", "Choosing a search type"),
            hit("s2", "See also"),
            hit("s3", "See also"),
        ];
        const ranked = RankingService.rerankByTitle("hybrid search", fused).map((r) => r.id);
        expect(ranked[0]).toBe("hs"); // exact title
        expect(ranked[1]).toBe("ch"); // distinct beats boilerplate
        expect(ranked.slice(2)).toEqual(["s1", "s2", "s3"]);
    });
});

describe("rerankByTitle raw-prefix tie-break", () => {
    const hit = (id: string, title: string): SearchResultItem => ({
        id, url: `/docs/${id}`, path: id, title, crumb: "", group: "", kind: "text",
    });

    it("prefers the title that literally continues the typed identifier", () => {
        // both cover tokens [ts, co] — column starts with "co" — but only
        // ts_compound continues the raw "ts_co"
        const fused = [
            hit("offsets", "ts_offsets(column [, limit])"),
            hit("compound", "ts_compound(must, must_not, should[, min_should_match])"),
        ];
        const ranked = RankingService.rerankByTitle("ts_co", fused);
        expect(ranked.map((r) => r.id)).toEqual(["compound", "offsets"]);
    });

    it("never outranks an exact-title match", () => {
        const fused = [
            hit("longer", "vacuum full guide"),
            hit("exact", "vacuum"),
        ];
        expect(RankingService.rerankByTitle("vacuum", fused)[0].id).toBe("exact");
    });
});

describe("capPerPage", () => {
    const hit = (id: string, url: string): SearchResultItem => ({
        id, url, path: url, title: id, crumb: "", group: "", kind: "text",
    });

    it("keeps at most N sections per page, preserving order", () => {
        const results = [
            hit("w1", "/docs/window#rank"), hit("w2", "/docs/window#lag"),
            hit("w3", "/docs/window#lead"), hit("w4", "/docs/window#cume"),
            hit("o1", "/docs/orderby"), hit("w5", "/docs/window#nth"),
        ];
        const capped = RankingService.capPerPage(results, 3);
        expect(capped.map((r) => r.id)).toEqual(["w1", "w2", "w3", "o1"]);
    });

    it("treats anchors of one page as one group and pages separately", () => {
        const results = [hit("a", "/docs/x#1"), hit("b", "/docs/y#1"), hit("c", "/docs/x#2")];
        expect(RankingService.capPerPage(results, 1).map((r) => r.id)).toEqual(["a", "b"]);
    });
});

describe("pinMatches", () => {
    it("matches exact, wildcard-suffix and contains patterns", () => {
        expect(RankingService.pinMatches("install", "install")).toBe(true);
        expect(RankingService.pinMatches("install*", "installation guide")).toBe(true);
        expect(RankingService.pinMatches("*replica*", "add a read replica now")).toBe(true);
        expect(RankingService.pinMatches("install", "installation")).toBe(false);
        expect(RankingService.pinMatches("docker", "install docker")).toBe(false);
    });
});

describe("parseQuery", () => {
    it("extracts quoted phrases and negated words", () => {
        const p = parseQuery('vacuum "read replica" -deprecated setup');
        expect(p.phrases).toEqual(["read replica"]);
        expect(p.negatives).toEqual(["deprecated"]);
        expect(p.tokens).toEqual(["vacuum", "setup", "read", "replica"]);
    });

    it("splits code identifiers on underscores like the analyzer does", () => {
        expect(parseQuery("ts_starts_with").tokens).toEqual(["ts", "starts", "with"]);
    });

    it("does not treat a hyphenated compound as negation mid-word", () => {
        const p = parseQuery("full-text search");
        expect(p.negatives).toEqual([]);
        expect(p.tokens).toEqual(["full", "text", "search"]);
    });
});

describe("makeSnippet", () => {
    it("highlights code identifiers by their underscore parts", () => {
        const snip = makeSnippet("Use ts_starts_with for prefix search", ["starts"]);
        expect(snip).toContain("<mark>ts_starts_with</mark>");
    });

    it("marks inflections both ways and centres the window on the hit", () => {
        const text =
            "Some unrelated preamble sits here first. " +
            "The easiest way to run SereneDB in a container is docker. " +
            "More trailing text follows the interesting part of the sentence.";
        const snip = makeSnippet(text, ["running"]);
        expect(snip).toContain("<mark>run</mark>");
        const dbSnip = makeSnippet("All the databases you own", ["database"]);
        expect(dbSnip).toContain("<mark>databases</mark>");
    });

    it("falls back to leading text without marks when nothing matches", () => {
        const snip = makeSnippet("Plain text about nothing in particular", ["quorum"]);
        expect(snip).toBe("Plain text about nothing in particular");
    });

    it("returns undefined for empty content", () => {
        expect(makeSnippet("   ", ["x"])).toBeUndefined();
    });
});
