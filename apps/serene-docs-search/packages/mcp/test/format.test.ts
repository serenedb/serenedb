import { describe, expect, it } from "vitest";
import type { SearchResultItem } from "@serenedb/docs-search-core";
import { absolutize, formatHits, formatSection } from "../src/format";

const hit = (over: Partial<SearchResultItem> = {}): SearchResultItem => ({
    id: "a",
    url: "/docs/sql/indexes/inverted/hybrid-search",
    path: "hybrid-search.html",
    title: "Hybrid Search",
    crumb: "Sql › Indexes › Inverted",
    group: "Sql",
    kind: "heading",
    snippet: "Combine a <mark>lexical</mark> signal with a vector signal",
    ...over,
});

describe("absolutize", () => {
    it("prefixes relative urls with the site origin and leaves absolute ones alone", () => {
        expect(absolutize("/docs/x", "https://serenedb.com/")).toBe("https://serenedb.com/docs/x");
        expect(absolutize("https://elsewhere.dev/y", "https://serenedb.com")).toBe(
            "https://elsewhere.dev/y",
        );
        expect(absolutize("/docs/x")).toBe("/docs/x");
    });
});

describe("formatHits", () => {
    it("renders numbered hits with clean snippets and absolutized urls", () => {
        const out = formatHits([hit()], "https://serenedb.com");
        expect(out).toContain("[1] Hybrid Search — Sql › Indexes › Inverted");
        expect(out).toContain("URL: https://serenedb.com/docs/sql/indexes/inverted/hybrid-search");
        expect(out).toContain("Combine a lexical signal");
        expect(out).not.toContain("<mark>");
    });

    it("says so when there is nothing", () => {
        expect(formatHits([])).toBe("No results.");
    });
});

describe("formatSection", () => {
    it("renders title, url and full content", () => {
        const out = formatSection(
            {
                id: "a",
                url: "/docs/quick-start",
                title: "Quick Start",
                crumb: "Docs",
                content: "Install SereneDB with docker.",
            },
            "https://serenedb.com",
        );
        expect(out).toContain("Quick Start — Docs");
        expect(out).toContain("url: https://serenedb.com/docs/quick-start");
        expect(out).toContain("Install SereneDB with docker.");
    });
});
