import { describe, expect, it } from "vitest";
import type { ContentConfig, SearchResultItem } from "@serenedb/docs-search-core";
import { catalogEntries } from "../src/repositories/objects";
import { inlinkCounts } from "../src/repositories/pages";
import { buildLexicalWhere } from "../src/repositories/search/search.repository";
import type { Section } from "../src/repositories/sections";
import { ParsingService } from "../src/services/parsing";
import { parseHtml } from "../src/services/parsing/html";
import { lookupKey, objectFromTitle, objectsFromTable } from "../src/services/parsing/objects";
import { RankingService } from "../src/services/ranking";
import { parseQuery } from "../src/utils/query";

describe("parseQuery terms", () => {
    it("drops question words from the terms but keeps them in the phrase", () => {
        const p = parseQuery("how do I create an inverted index?");
        expect(p.terms).toEqual(["create", "inverted", "index"]);
        expect(p.sequence.join(" ")).toBe("how do i create an inverted index");
        expect(p.trailing).toBeUndefined();
    });

    it("keeps keyword-only queries searchable", () => {
        expect(parseQuery("is not null").terms).toEqual(["null"]);
        expect(parseQuery("on conflict").terms).toEqual(["conflict"]);
        expect(parseQuery("the").terms).toEqual(["the"]);
    });

    it("marks the word being typed, never a negated or finished one", () => {
        expect(parseQuery("hybrid sea").trailing).toBe("sea");
        expect(parseQuery("group b").trailing).toBe("b");
        expect(parseQuery("group b").terms).toEqual(["group"]);
        expect(parseQuery("hybrid search ").trailing).toBeUndefined();
        expect(parseQuery("vacuum -refresh").trailing).toBeUndefined();
        expect(parseQuery('"reciprocal rank"').trailing).toBeUndefined();
    });

    it("keeps the written word order around quoted phrases", () => {
        const p = parseQuery('"vacuum refresh" syntax');
        expect(p.sequence).toEqual(["vacuum", "refresh", "syntax"]);
        expect(p.phrases).toEqual(["vacuum refresh"]);
        expect(parseQuery('"reciprocal rank" fus').trailing).toBe("fus");
        expect(parseQuery("vacuum -refresh analyze").sequence).toEqual(["vacuum", "analyze"]);
    });

    it("keeps a long pasted sentence for the phrase, caps the terms", () => {
        const words = Array.from({ length: 40 }, (_, i) => `word${i}`).join(" ");
        const p = parseQuery(words);
        expect(p.sequence).toHaveLength(32);
        expect(p.terms).toHaveLength(12);
    });
});

describe("object catalog extraction", () => {
    it("reads signature, statement and identifier headings", () => {
        expect(objectFromTitle("date_trunc(part, date)")).toMatchObject({
            kind: "function",
            source: "heading",
            names: ["date_trunc"],
        });
        // a qualified name keeps its bare part only when that is code-like
        expect(objectFromTitle("sdb_docs.search(query[, max_hits])")?.names).toEqual(["sdb_docs.search"]);
        expect(objectFromTitle("pg_catalog.pg_class")?.names).toEqual(["pg_catalog.pg_class", "pg_class"]);
        expect(objectFromTitle("apply_index() / aapply_index()")?.names).toEqual(["apply_index", "aapply_index"]);
        expect(objectFromTitle("lower(string) -> VARCHAR")?.names).toEqual(["lower"]);
        expect(objectFromTitle("CREATE INDEX")).toMatchObject({ kind: "statement", names: ["create index"] });
        expect(objectFromTitle("SET / RESET")?.names).toEqual(["set / reset", "set", "reset"]);
        expect(objectFromTitle("split_text")?.names).toEqual(["split_text"]);
        // a dot command keeps its bare name only when that can't be a word
        expect(objectFromTitle(".timer")).toMatchObject({ kind: "command", names: [".timer"] });
        expect(objectFromTitle(".auto_format")?.names).toEqual([".auto_format", "auto_format"]);
    });

    it("ignores prose headings", () => {
        for (const t of [
            "Vector search",
            "Examples",
            "Prepared Statements",
            "How it works (overview)",
            "Hybrid (reciprocal rank fusion)",
            "Range (radius) search",
            "Parallelism (Multi-Core Processing)",
            "Integer Division Operator (//)",
            "Two (or More) Dots",
            "ts_highlight(text) in practice",
        ]) {
            expect(objectFromTitle(t, 2), t).toBeNull();
        }
    });

    it("takes a lone upper-case keyword only as a page title", () => {
        expect(objectFromTitle("VACUUM", 1)?.names).toEqual(["vacuum"]);
        expect(objectFromTitle("JSON", 4)).toBeNull();
        expect(objectFromTitle("GROUP BY", 2)?.names).toEqual(["group by"]);
    });

    it("reads reference tables keyed by their first column", () => {
        const types = objectsFromTable(
            ["Name", "Aliases", "Description"],
            [["BIGINT", "INT8, LONG, `INT64`", "Signed eight-byte integer"]],
        );
        expect(types).toEqual([
            { kind: "type", source: "table", signature: "BIGINT", names: ["bigint", "int8", "long", "int64"] },
        ]);
        const fns = objectsFromTable(
            ["Function", "Description"],
            [
                ["`abs(x)`", "Absolute value."],
                ["a sentence about something", "not a name"],
            ],
        );
        expect(fns.map((o) => o.names)).toEqual([["abs"]]);
        const cmds = objectsFromTable(["Command", "Arguments", "Description"], [[".indexes", "", "List indexes."]]);
        expect(cmds[0]).toMatchObject({ kind: "command", names: [".indexes"] });
        expect(objectsFromTable(["Feature", "Support"], [["vacuum", "yes"]])).toEqual([]);
        // argument tables: plain words are parameters, not documented objects
        const params = objectsFromTable(
            ["Name", "Description"],
            [["table", "The table."], ["column", "A column."], ["optimize_top_k", "WAND pruning."]],
        );
        expect(params.map((o) => o.names[0])).toEqual(["optimize_top_k"]);
        expect(objectsFromTable(["Setting", "Default"], [["threads", "8"]])[0]?.names).toEqual(["threads"]);
        // labels and option keywords are not objects
        expect(objectsFromTable(["Setting", "Flag"], [["Host", "-h"], ["Database", "-d"]])).toEqual([]);
        expect(objectsFromTable(["Option", "Type"], [["CASE", "TEXT"], ["GROUP", "INT"]])).toEqual([]);
        expect(objectsFromTable(["Name", "Description"], [["FORMAT", "File format."]])).toEqual([]);
    });

    it("reduces a pasted call to its name", () => {
        expect(lookupKey("date_trunc('day', ts)")).toBe("date_trunc");
        expect(lookupKey("  BIGINT ")).toBe("bigint");
        expect(lookupKey("Calendar date (year)")).toBe("calendar date (year)");
        expect(lookupKey("date_trunc('day'")).toBe("date_trunc");
        expect(lookupKey("count(*);")).toBe("count");
        expect(lookupKey("LIST(INTEGER): live documents per term")).not.toBe("list");
    });
});

const MD_CONTENT: ContentConfig = {
    extensions: [".md"],
    urlMapping: { baseUrl: "/docs", stripExtensions: true, indexFiles: ["index"] },
};

const MD = `# Text Functions

Intro with a [link to dates](../functions/date.md#date_trunc) and [the overview](./index.md).

## Text Functions and Operators

| Function | Description |
|:--|:--|
| \`ascii(string)\` | Unicode code point of the first character. |
| \`concat(value, ...)\` | Concatenates. |

### ascii(string)

Returns the code point.

## Examples

Example text.
`;

describe("parseFile: trail, nested body, objects, links", () => {
    const parse = () =>
        ParsingService.parseFile(
            { path: "sql/functions/text.md", extension: ".md", content: MD } as never,
            MD_CONTENT,
        );

    it("gives the page section the whole page and each section its heading trail", async () => {
        const [root, ops, ascii, examples] = await parse();
        expect(root.body).toContain("Returns the code point.");
        expect(root.body).toContain("Example text.");
        expect(ops.body).toContain("Returns the code point.");
        expect(ops.body).not.toContain("Example text.");
        expect(ascii.body).toBe("Returns the code point.");
        expect(root.trail).toBe("");
        expect(ascii.trail).toBe("Text Functions › Text Functions and Operators");
        expect(examples.trail).toBe("Text Functions");
        // content stays the section's own text
        expect(root.content).not.toContain("Returns the code point.");
    });

    it("collects heading and table objects", async () => {
        const [, ops, ascii] = await parse();
        expect(ascii.objects).toEqual([
            expect.objectContaining({ source: "heading", names: ["ascii"] }),
        ]);
        expect(ops.objects.map((o) => o.names[0])).toEqual(["ascii", "concat"]);
    });

    it("resolves the page's outbound links to page URLs", async () => {
        const [root, ops] = await parse();
        expect(root.links).toEqual(["/docs/sql/functions/date", "/docs/sql/functions"]);
        expect(ops.links).toBeUndefined();
    });
});

describe("parseHtml anchors and links", () => {
    it("ignores a layout container's id for id-less headings", () => {
        const html = `<main id="__docusaurus_skipToContent_fallback"><article>
            <h1>Page</h1><p>intro text long enough to keep as a section</p>
            <h2>No id here</h2><p>body</p>
            <div id="wrapped"><h3>Wrapped</h3></div><p>more <a href="/docs/other">x</a></p>
        </article></main>`;
        const res = parseHtml(html, { selectors: "article" });
        expect(res.sections.map((s) => s.anchor)).toEqual([undefined, "no-id-here", "wrapped"]);
        expect(res.links).toEqual(["/docs/other"]);
    });

    it("takes links from the indexed text, not from tag chips or a TOC", () => {
        const html = `<article><header><h1>Post</h1>
            <div class="tags"><a href="/blog/tags/search">search</a></div></header>
            <aside><a href="#intro">Intro</a></aside>
            <p>See <a href="/docs/sql/indexes">indexes</a>.</p></article>`;
        expect(parseHtml(html, { selectors: "article" }).links).toEqual(["/docs/sql/indexes"]);
    });
});

const section = (over: Partial<Section>): Section => ({
    id: "x",
    path: "p.md",
    url: "/docs/p",
    title: "",
    crumb: "",
    group: "",
    kind: "text",
    level: 2,
    content: "",
    code: "",
    trail: "",
    body: "",
    objects: [],
    hash: "h",
    ...over,
});

describe("catalogEntries", () => {
    it("prefers a heading over a table row of the same page", () => {
        const entries = catalogEntries([
            section({
                id: "root",
                level: 1,
                objects: [{ kind: "function", source: "table", signature: "date_part(part, date)", names: ["date_part"] }],
            }),
            section({
                id: "fn",
                level: 3,
                title: "date_part(part, date)",
                objects: [{ kind: "function", source: "heading", signature: "date_part(part, date)", names: ["date_part"] }],
            }),
        ]);
        expect(entries.map((e) => e.sectionId)).toEqual(["fn"]);
    });

    it("points a table-only object at the page's section named after it", () => {
        const entries = catalogEntries([
            section({
                id: "list",
                title: "Functions for Reading JSON as a Table",
                objects: [{ kind: "function", source: "table", signature: "read_json(filename)", names: ["read_json"] }],
            }),
            section({ id: "own", title: "The read_json Function", level: 3 }),
        ]);
        expect(entries).toEqual([expect.objectContaining({ name: "read_json", sectionId: "own", level: 3 })]);
    });
});

describe("catalogEntries: pages about an object", () => {
    it("adds the page titled with a name that tables elsewhere list", () => {
        const entries = catalogEntries([
            section({
                id: "nav",
                path: "nested.md",
                level: 1,
                objects: [{ kind: "type", source: "table", signature: "UNION", names: ["union"] }],
            }),
            section({ id: "page", path: "union.md", title: "Union", level: 1 }),
        ]);
        expect(entries.map((e) => [e.sectionId, e.source])).toEqual([
            ["nav", "table"],
            ["page", "heading"],
        ]);
    });
});

describe("inlinkCounts", () => {
    it("counts distinct linking pages, never self-links", () => {
        const counts = inlinkCounts([
            section({ url: "/docs/a", links: ["/docs/b", "/docs/c"] }),
            section({ url: "/docs/a#x", links: ["/docs/b"] }),
            section({ url: "/docs/b", links: ["/docs/b", "/docs/c"] }),
        ]);
        expect(counts.get("/docs/b")).toBe(1);
        expect(counts.get("/docs/c")).toBe(2);
    });
});

describe("buildLexicalWhere", () => {
    const ctx = { stopwords: ["the", "a", "an", "this", "these", "those"], exactnessEnabled: true } as never;
    const where = (q: string, pass: "strict" | "fuzzy" | "relaxed" = "strict") =>
        buildLexicalWhere(ctx, parseQuery(q), q, pass) ?? "";

    it("filters unscored, per term across fields, and scores the phrase", () => {
        const w = where("how do I highlight matches");
        const [filter] = w.split("::score(NULL)");
        expect(filter).toContain("plainto_tsquery('highlight')");
        expect(filter).not.toContain("plainto_tsquery('how')");
        // predicates are joined with SQL AND, never the tsquery operator
        expect(filter).not.toMatch(/\) && \(/);
        expect(filter).toContain("trail @@");
        // the phrase goes to the analyzer as written, punctuation and all
        expect(w).toContain("phraseto_tsquery('how do I highlight matches')");
    });

    it("never builds a phrase out of index stopwords only", () => {
        expect(where('"the" ranking')).not.toContain("phraseto_tsquery('the')");
        expect(where("the ")).toBe("");
    });

    it("matches an index stopword being typed as a prefix only", () => {
        const w = where("an");
        expect(w).toContain("ts_starts_with('an')");
        expect(w).not.toContain("plainto_tsquery('an')");
        expect(where("create an")).toContain("title @@ (ts_starts_with('an') ^ 1)");
    });

    it("leaves symbol-only queries to the code path", () => {
        expect(where("@@")).toBe("");
    });

    it("pads a short operator so it still has trigrams", () => {
        expect(where("x @@ y")).toContain("ts_ngram('x @@ y', 0.45)");
    });

    it("adds typo alternatives only to the passes that allow them", () => {
        expect(where("trasactions")).not.toContain("ts_levenshtein");
        expect(where("trasactions", "fuzzy")).toContain("ts_levenshtein('trasactions', 2, true)");
    });
});

describe("rerankByTitle: boilerplate", () => {
    const hit = (id: string, title: string, url: string): SearchResultItem => ({
        id,
        url,
        path: id,
        title,
        crumb: "",
        group: "",
        kind: "heading",
    });

    it("keeps a heading shared by two pages where relevance put it", () => {
        const results = [
            hit("syntax", "Syntax", "/docs/aggregates#syntax"),
            hit("other", "Aggregates", "/docs/select#aggregates"),
            hit("syntax2", "Syntax", "/docs/select#syntax"),
        ];
        expect(RankingService.rerankByTitle("aggregates combine rows", results).map((r) => r.id)).toEqual([
            "syntax",
            "other",
            "syntax2",
        ]);
    });
});

describe("rerankByTitle: equal titles", () => {
    const hit = (id: string, title: string, url: string, level: number): SearchResultItem => ({
        id,
        url,
        path: id,
        title,
        crumb: "",
        group: "",
        kind: "heading",
        level,
    });

    it("puts the page about it first, then the more linked-to page", () => {
        const results = [
            hit("faq", "Hybrid search", "/docs/clients/lc/faq#hybrid-search", 2),
            hit("lc", "Hybrid Search", "/docs/clients/lc/hybrid-search", 1),
            hit("ref", "Hybrid Search", "/docs/sql/indexes/inverted/hybrid-search", 1),
        ];
        const inlinks = new Map([
            ["/docs/clients/lc/hybrid-search", 2],
            ["/docs/sql/indexes/inverted/hybrid-search", 12],
            ["/docs/clients/lc/faq", 30],
        ]);
        expect(RankingService.rerankByTitle("hybrid search", results, inlinks).map((r) => r.id)).toEqual([
            "ref",
            "lc",
            "faq",
        ]);
        // without the link graph the page-level rule alone still applies
        expect(RankingService.rerankByTitle("hybrid search", results).map((r) => r.id)).toEqual([
            "lc",
            "ref",
            "faq",
        ]);
    });
});
