// @vitest-environment node

import { describe, expect, it } from "vitest";
import type {
    SearchResultItem,
    SearchSectionConfig,
} from "@serenedb/docs-search-core";
import { groupResultsBySections } from "../src/lib/sections";

const hit = (
    id: string,
    url: string,
    path: string,
    group = "Legacy",
): SearchResultItem => ({
    id,
    url,
    path,
    title: id,
    crumb: "",
    group,
    kind: "text",
});

const SECTIONS: SearchSectionConfig[] = [
    {
        id: "installation",
        label: "Installation",
        match: {
            urls: ["https://docs.example.com/installation/**"],
            paths: ["docs/installation/**", "/installation/**"],
        },
    },
    {
        id: "sql",
        label: "SQL",
        match: { paths: ["docs/sql/**", "/sql/**"] },
    },
    {
        id: "docs",
        label: "Docs",
        match: {
            urls: ["https://docs.example.com/**"],
            paths: ["docs/**"],
        },
    },
    {
        id: "blog",
        label: "Blog",
        match: { urls: ["https://blog.example.com/**"], paths: ["blog/**"] },
    },
];

const RESULTS = [
    hit("blog-1", "https://blog.example.com/release", "blog/release/index.html"),
    hit("install-1", "https://docs.example.com/installation/docker", "docs/installation/docker.html"),
    hit("docs-1", "https://docs.example.com/quick-start", "docs/quick-start.html"),
    hit("install-2", "https://docs.example.com/installation/source", "docs/installation/source.html"),
    hit("sql-1", "https://docs.example.com/sql/select", "docs/sql/select.html"),
];

describe("groupResultsBySections", () => {
    it("prioritizes the current nested section and preserves ranking inside it", () => {
        const grouped = groupResultsBySections(
            RESULTS,
            SECTIONS,
            "https://docs.example.com/installation/overview",
        );

        expect(grouped.activeSectionId).toBe("installation");
        expect(grouped.groups.map((group) => group.label)).toEqual([
            "Installation",
            "SQL",
            "Docs",
            "Blog",
        ]);
        expect(grouped.results.map((item) => item.id)).toEqual([
            "install-1",
            "install-2",
            "sql-1",
            "docs-1",
            "blog-1",
        ]);
    });

    it("moves Blog first from a blog location without changing its own ranking", () => {
        const results = [
            ...RESULTS,
            hit("blog-2", "https://blog.example.com/benchmarks", "blog/benchmarks/index.html"),
        ];
        const grouped = groupResultsBySections(
            results,
            SECTIONS,
            "https://blog.example.com/ann-search",
        );

        expect(grouped.activeSectionId).toBe("blog");
        expect(grouped.groups[0].label).toBe("Blog");
        expect(grouped.groups[0].items.map((item) => item.id)).toEqual(["blog-1", "blog-2"]);
    });

    it("uses source paths when final URLs are relative", () => {
        const grouped = groupResultsBySections(
            [hit("sql", "/sql/select", "docs/sql/select.mdx")],
            SECTIONS,
            "http://localhost:3001/docs/sql/overview",
        );
        expect(grouped.groups[0]).toMatchObject({ id: "sql", label: "SQL" });
    });

    it("does not classify a relative Blog hit as Docs from the current docs origin", () => {
        const grouped = groupResultsBySections(
            [
                hit(
                    "blog",
                    "/blog/search-benchmark-game-overview",
                    "blog/search-benchmark-game-overview.html",
                ),
                hit("docs", "/sql/select", "sql/select.html"),
            ],
            [
                {
                    id: "blog",
                    label: "Blog",
                    match: {
                        urls: ["https://blog.example.com/**"],
                        paths: ["blog/**", "/blog/**"],
                    },
                },
                {
                    id: "docs",
                    label: "Docs",
                    match: {
                        urls: ["https://docs.example.com/**"],
                        paths: ["**", "/**"],
                    },
                },
            ],
            "https://docs.example.com/quick-start",
        );

        expect(grouped.groups.map((group) => group.id)).toEqual(["docs", "blog"]);
        expect(grouped.groups[1].items[0].id).toBe("blog");
    });

    it("hides section headings when every visible result belongs to one section", () => {
        const grouped = groupResultsBySections(
            [
                hit("docs-1", "/quick-start", "quick-start.html"),
                hit("docs-2", "/sql/select", "sql/select.html"),
            ],
            [
                {
                    id: "docs",
                    label: "Docs",
                    match: { paths: ["**"] },
                },
            ],
            "https://docs.example.com/quick-start",
        );

        expect(grouped.groups).toHaveLength(1);
        expect(grouped.sectioned).toBe(false);
        expect(grouped.results.map((item) => item.id)).toEqual(["docs-1", "docs-2"]);
    });

    it("omits empty sections, keeps unmatched results, and handles one section", () => {
        const grouped = groupResultsBySections(
            [
                hit("guide", "/guide", "guide.md"),
                hit("other", "/outside", "outside.md"),
            ],
            [{ id: "guide", label: "Guide", match: { paths: ["guide.md"] } }],
            "https://example.com/guide",
        );
        expect(grouped.groups.map((group) => group.label)).toEqual(["Guide", "Other results"]);
        expect(grouped.sectioned).toBe(true);
        expect(grouped.results.map((item) => item.id)).toEqual(["guide", "other"]);
    });

    it("preserves legacy order and grouping when sections are absent", () => {
        const grouped = groupResultsBySections(
            [
                hit("a", "/a", "a.md", "SQL"),
                hit("b", "/b", "b.md", "Docs"),
                hit("c", "/c", "c.md", "SQL"),
            ],
            undefined,
            "https://docs.example.com/",
        );
        expect(grouped.sectioned).toBe(false);
        expect(grouped.results.map((item) => item.id)).toEqual(["a", "b", "c"]);
        expect(grouped.groups.map((group) => group.label)).toEqual(["SQL", "Docs"]);
    });
});
