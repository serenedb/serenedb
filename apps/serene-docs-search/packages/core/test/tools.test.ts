import { describe, expect, it } from "vitest";
import {
    DOCS_AGENT_TOOLS,
    formatDocsHit,
    formatDocsHits,
    formatDocsSection,
    type DocsHit,
} from "../src/tools";

const hit = (over: Partial<DocsHit> = {}): DocsHit => ({
    title: "Hybrid Search",
    crumb: "Sql › Indexes",
    url: "/docs/sql/indexes/inverted/hybrid-search",
    snippet: "combine lexical and vector",
    ...over,
});

describe("DOCS_AGENT_TOOLS", () => {
    it("exposes search_docs and read_section as OpenAI function tools", () => {
        expect(DOCS_AGENT_TOOLS.map((t) => t.function.name)).toEqual([
            "search_docs",
            "read_section",
        ]);
        expect(DOCS_AGENT_TOOLS[0].function.parameters.required).toEqual(["query"]);
        expect(DOCS_AGENT_TOOLS[1].function.parameters.required).toEqual(["n"]);
    });
});

describe("formatDocsHit / formatDocsHits", () => {
    it("renders a citation-numbered hit with title, crumb, url and snippet", () => {
        expect(formatDocsHit(3, hit())).toBe(
            "[3] Hybrid Search — Sql › Indexes\n" +
                "URL: /docs/sql/indexes/inverted/hybrid-search\n" +
                "combine lexical and vector",
        );
    });

    it("omits the crumb dash and snippet line when empty", () => {
        expect(formatDocsHit(1, hit({ crumb: "", snippet: "" }))).toBe(
            "[1] Hybrid Search\nURL: /docs/sql/indexes/inverted/hybrid-search",
        );
    });

    it("numbers a run from the given start and separates with a blank line", () => {
        const out = formatDocsHits([hit({ title: "A" }), hit({ title: "B" })], 2);
        expect(out).toBe(
            [
                "[2] A — Sql › Indexes\nURL: /docs/sql/indexes/inverted/hybrid-search\ncombine lexical and vector",
                "[3] B — Sql › Indexes\nURL: /docs/sql/indexes/inverted/hybrid-search\ncombine lexical and vector",
            ].join("\n\n"),
        );
    });

    it("says so when there are no hits", () => {
        expect(formatDocsHits([])).toBe("No results.");
    });
});

describe("formatDocsSection", () => {
    it("puts the body under a label line", () => {
        expect(formatDocsSection("[3] Hybrid Search", "Combine signals.")).toBe(
            "[3] Hybrid Search\nCombine signals.",
        );
    });
});
