import {
    cleanup,
    fireEvent,
    render,
    screen,
    waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SereneDocsSearch } from "../src/SereneDocsSearch";

const response = (body: unknown) =>
    new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });

describe("sectioned search UI", () => {
    beforeEach(() => {
        localStorage.clear();
        vi.stubGlobal(
            "fetch",
            vi.fn(async (input: RequestInfo | URL) => {
                if (String(input).endsWith("/v1/health")) {
                    return response({
                        ok: true,
                        version: "0.1.0",
                        serenedb: { connected: true },
                        index: {
                            ready: true,
                            building: false,
                            sections: 3,
                            documents: 3,
                        },
                        features: { ai: false, hybrid: false },
                        searchType: "fulltext",
                    });
                }
                return response({
                    query: "search",
                    mode: "fulltext",
                    results: [
                        {
                            id: "docs",
                            url: "https://docs.example.com/search",
                            path: "docs/search.md",
                            title: "Docs search",
                            crumb: "Docs",
                            group: "Legacy",
                            kind: "text",
                        },
                        {
                            id: "blog",
                            url: "https://blog.example.com/search-story",
                            path: "blog/search-story.md",
                            title: "Blog search story",
                            crumb: "Blog",
                            group: "Legacy",
                            kind: "text",
                        },
                    ],
                    total: 2,
                    tookMs: 3,
                });
            }),
        );
        Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
            configurable: true,
            value: vi.fn(),
        });
    });

    afterEach(() => {
        cleanup();
        vi.unstubAllGlobals();
    });

    it("renders visible group headings with the current Blog section first", async () => {
        render(
            <SereneDocsSearch
                backendUrl="http://api"
                open
                onOpenChange={() => {}}
                trigger={false}
                debounceMs={0}
                contextUrl="https://blog.example.com/current"
                sections={[
                    {
                        id: "docs",
                        label: "Docs",
                        match: { urls: ["https://docs.example.com/**"] },
                    },
                    {
                        id: "blog",
                        label: "Blog",
                        match: { urls: ["https://blog.example.com/**"] },
                    },
                ]}
            />,
        );

        const input = await screen.findByPlaceholderText(
            "Search docs or ask a question…",
        );
        fireEvent.change(input, { target: { value: "search" } });

        await waitFor(() => {
            expect(screen.getAllByRole("heading", { level: 2 })).toHaveLength(
                2,
            );
        });
        expect(
            screen
                .getAllByRole("heading", { level: 2 })
                .map((heading) => heading.textContent),
        ).toEqual(["Blog", "Docs"]);
        expect(
            screen.getByRole("button", { name: /Blog search story/i }),
        ).toBeTruthy();
        expect(
            screen.getByRole("button", { name: /Docs search/i }),
        ).toBeTruthy();
    });

    it("does not render a redundant heading for a single populated section", async () => {
        vi.stubGlobal(
            "fetch",
            vi.fn(async (input: RequestInfo | URL) => {
                if (String(input).endsWith("/v1/health")) {
                    return response({
                        ok: true,
                        version: "0.9.0",
                        serenedb: { connected: true },
                        index: {
                            ready: true,
                            building: false,
                            sections: 2,
                            documents: 2,
                        },
                        features: { ai: false, hybrid: false },
                        searchType: "fulltext",
                    });
                }
                return response({
                    query: "sql",
                    mode: "fulltext",
                    results: [
                        {
                            id: "one",
                            url: "/sql/select",
                            path: "sql/select.html",
                            title: "SELECT",
                            crumb: "SQL",
                            group: "SQL",
                            kind: "text",
                        },
                    ],
                    total: 1,
                    tookMs: 2,
                });
            }),
        );

        render(
            <SereneDocsSearch
                backendUrl="http://api"
                open
                onOpenChange={() => {}}
                trigger={false}
                debounceMs={0}
                sections={[
                    {
                        id: "docs",
                        label: "Docs",
                        match: { paths: ["**"] },
                    },
                ]}
            />,
        );

        const input = await screen.findByPlaceholderText(
            "Search docs or ask a question…",
        );
        fireEvent.change(input, { target: { value: "sql" } });
        await waitFor(() => {
            expect(
                screen.getByRole("button", { name: /SELECT/i }),
            ).toBeTruthy();
        });
        expect(screen.queryByRole("heading", { level: 2 })).toBeNull();
    });
});
