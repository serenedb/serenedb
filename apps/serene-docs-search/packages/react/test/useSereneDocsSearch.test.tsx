import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SearchResultItem } from "@serenedb/docs-search-core";
import { useSereneDocsSearch } from "../src/hooks/useSereneDocsSearch";
import { SearchStorage } from "../src/lib/storage";

const HEALTH = {
    ok: true,
    version: "0.1.0",
    serenedb: { connected: true, version: "serenedb 1.2" },
    index: { ready: true, building: false, sections: 2006, documents: 300 },
    features: { ai: true, hybrid: true },
    searchType: "fulltext" as "fulltext" | "hybrid",
};

const hit = (id: string, over: Partial<SearchResultItem> = {}): SearchResultItem => ({
    id,
    url: `/docs/${id}`,
    path: `${id}.md`,
    title: id,
    crumb: "",
    group: "Docs",
    kind: "text",
    ...over,
});

const json = (body: unknown) =>
    new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });

const sse = (frames: string[]) =>
    new Response(
        new ReadableStream<Uint8Array>({
            start(c) {
                const enc = new TextEncoder();
                for (const f of frames) c.enqueue(enc.encode(f));
                c.close();
            },
        }),
        { status: 200 },
    );

interface BackendOptions {
    searchType?: "fulltext" | "hybrid";
    failHealth?: boolean;
    /** Per-mode results for POST /v1/search. */
    results?: (body: { q: string; mode?: string }) => SearchResultItem[];
    /** Await this before answering a search — lets tests hold a response. */
    beforeSearch?: (body: { q: string; mode?: string }) => Promise<void>;
    /** Await this before answering an ask — lets tests hold the stream. */
    beforeAsk?: () => Promise<void>;
    askFrames?: string[];
}

/** Stubs global fetch with a fake backend; returns the recorded calls. */
function mockBackend(opts: BackendOptions = {}) {
    const calls: Array<{ url: string; body?: Record<string, unknown> }> = [];
    vi.stubGlobal(
        "fetch",
        vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
            const url = String(input);
            const body = init?.body ? (JSON.parse(init.body as string) as Record<string, unknown>) : undefined;
            calls.push({ url, body });
            if (url.endsWith("/v1/health")) {
                if (opts.failHealth) throw new TypeError("fetch failed");
                return json({ ...HEALTH, searchType: opts.searchType ?? "fulltext" });
            }
            if (url.endsWith("/v1/search")) {
                const q = body as { q: string; mode?: string };
                await opts.beforeSearch?.(q);
                return json({
                    query: q.q,
                    mode: q.mode ?? "fulltext",
                    results: opts.results?.(q) ?? [],
                    total: 0,
                    tookMs: 5,
                });
            }
            if (url.endsWith("/v1/ask")) {
                await opts.beforeAsk?.();
                return sse(opts.askFrames ?? []);
            }
            return json({ ok: true });
        }),
    );
    return calls;
}

beforeEach(() => localStorage.clear());
afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
});

describe("useSereneDocsSearch — connection", () => {
    it("stays unconfigured without a backend and ignores queries", () => {
        const calls = mockBackend();
        const { result } = renderHook(() => useSereneDocsSearch());
        expect(result.current.status).toBe("unconfigured");
        expect(result.current.client).toBeNull();
        act(() => result.current.setQuery("vacuum"));
        expect(result.current.phase).toBe("idle");
        expect(calls).toHaveLength(0);
    });

    it("checks health when the modal opens and goes online", async () => {
        const calls = mockBackend();
        const { result } = renderHook(() => useSereneDocsSearch({ backendUrl: "http://api:7700" }));
        expect(result.current.status).toBe("connecting");

        act(() => result.current.setOpen(true));
        await waitFor(() => expect(result.current.status).toBe("online"));
        expect(result.current.health?.index.sections).toBe(2006);
        expect(result.current.aiEnabled).toBe(true);
        expect(calls[0].url).toBe("http://api:7700/v1/health");
    });

    it("reports offline when the backend is unreachable", async () => {
        mockBackend({ failHealth: true });
        const { result } = renderHook(() => useSereneDocsSearch({ backendUrl: "http://down" }));
        act(() => result.current.setOpen(true));
        await waitFor(() => expect(result.current.status).toBe("offline"));
        expect(result.current.health).toBeNull();
    });

    it("restores a saved connection and forgets it on disconnect", () => {
        mockBackend();
        new SearchStorage("serene-docs-search").saveConnection({ backendUrl: "http://saved:7700" });
        const { result } = renderHook(() => useSereneDocsSearch());
        expect(result.current.backendUrl).toBe("http://saved:7700");
        expect(result.current.status).toBe("connecting");

        act(() => result.current.disconnect());
        expect(result.current.backendUrl).toBeNull();
        expect(localStorage.getItem("serene-docs-search:connection")).toBeNull();
    });
});

describe("useSereneDocsSearch — searching", () => {
    it("debounces, keeps only the latest query and lands on done", async () => {
        const calls = mockBackend({
            results: (q) => (q.q === "vacuum" ? [hit("vacuum"), hit("vacuum-refresh")] : []),
        });
        const { result } = renderHook(() =>
            useSereneDocsSearch({ backendUrl: "http://api", debounceMs: 30 }),
        );
        act(() => result.current.setOpen(true));
        await waitFor(() => expect(result.current.status).toBe("online"));

        act(() => {
            result.current.setQuery("vac");
            result.current.setQuery("vacuum"); // same tick — first timer is cancelled
        });
        expect(result.current.phase).toBe("searching");

        await waitFor(() => expect(result.current.phase).toBe("done"));
        expect(result.current.results.map((r) => r.id)).toEqual(["vacuum", "vacuum-refresh"]);
        expect(result.current.total).toBe(2);
        expect(result.current.tookMs).toBe(5);
        const searches = calls.filter((c) => c.url.endsWith("/v1/search"));
        expect(searches.map((c) => c.body?.q)).toEqual(["vacuum"]);
    });

    it("merges the semantic pass on hybrid installs", async () => {
        // hold the hybrid response until the test has inspected pass 1
        let releaseHybrid!: () => void;
        const hybridGate = new Promise<void>((r) => (releaseHybrid = r));
        mockBackend({
            searchType: "hybrid",
            beforeSearch: (q) => (q.mode === "hybrid" ? hybridGate : Promise.resolve()),
            results: (q) =>
                q.mode === "hybrid"
                    ? [hit("a"), hit("b", { aiSuggested: true })]
                    : [hit("a")],
        });
        const { result } = renderHook(() =>
            useSereneDocsSearch({ backendUrl: "http://api", debounceMs: 1, semanticDebounceMs: 5 }),
        );
        act(() => result.current.setOpen(true));
        await waitFor(() => expect(result.current.status).toBe("online"));

        act(() => result.current.setQuery("hybrid search"));

        // pass 1: instant fulltext, semantic still pending
        await waitFor(() => expect(result.current.results).toHaveLength(1));
        expect(result.current.semanticPending).toBe(true);
        expect(result.current.semantic).toBe(false);
        releaseHybrid();

        // pass 2: hybrid results merge in
        await waitFor(() => expect(result.current.semantic).toBe(true));
        expect(result.current.results.map((r) => r.id)).toEqual(["a", "b"]);
        expect(result.current.semanticPending).toBe(false);
        expect(result.current.semanticTookMs).toBe(5);
        expect(result.current.phase).toBe("done");
    });

    it("groups results by their group label while keeping the flat order", async () => {
        mockBackend({
            results: () => [
                hit("a", { group: "SQL" }),
                hit("b", { group: "Cookbook" }),
                hit("c", { group: "SQL" }),
            ],
        });
        const { result } = renderHook(() =>
            useSereneDocsSearch({ backendUrl: "http://api", debounceMs: 1 }),
        );
        act(() => result.current.setOpen(true));
        await waitFor(() => expect(result.current.status).toBe("online"));
        act(() => result.current.setQuery("q"));
        await waitFor(() => expect(result.current.phase).toBe("done"));

        expect(result.current.results.map((r) => r.id)).toEqual(["a", "b", "c"]);
        expect(result.current.groups).toEqual([
            { label: "SQL", items: [expect.objectContaining({ id: "a" }), expect.objectContaining({ id: "c" })] },
            { label: "Cookbook", items: [expect.objectContaining({ id: "b" })] },
        ]);
    });

    it("reports no-results and resets to idle when the query is cleared", async () => {
        mockBackend({ results: () => [] });
        const { result } = renderHook(() =>
            useSereneDocsSearch({ backendUrl: "http://api", debounceMs: 1 }),
        );
        act(() => result.current.setOpen(true));
        await waitFor(() => expect(result.current.status).toBe("online"));

        act(() => result.current.setQuery("quorum"));
        await waitFor(() => expect(result.current.phase).toBe("no-results"));

        act(() => result.current.setQuery(""));
        expect(result.current.phase).toBe("idle");
        expect(result.current.results).toEqual([]);
    });
});

describe("useSereneDocsSearch — selection & navigation", () => {
    async function searchTwoHits() {
        const calls = mockBackend({ results: () => [hit("first"), hit("second")] });
        const navigate = vi.fn();
        const onSelect = vi.fn();
        const rendered = renderHook(() =>
            useSereneDocsSearch({
                backendUrl: "http://api",
                debounceMs: 1,
                navigate,
                transformUrl: (u) => `/base${u}`,
            }),
        );
        act(() => rendered.result.current.setOpen(true));
        await waitFor(() => expect(rendered.result.current.status).toBe("online"));
        act(() => rendered.result.current.setQuery("docs"));
        await waitFor(() => expect(rendered.result.current.phase).toBe("done"));
        return { ...rendered, calls, navigate, onSelect };
    }

    it("wraps arrow-key selection around the list", async () => {
        const { result } = await searchTwoHits();
        expect(result.current.selectedIndex).toBe(0);
        act(() => result.current.moveSelection(1));
        expect(result.current.selectedIndex).toBe(1);
        act(() => result.current.moveSelection(1));
        expect(result.current.selectedIndex).toBe(0);
        act(() => result.current.moveSelection(-1));
        expect(result.current.selectedIndex).toBe(1);
    });

    it("select() saves the query, fires analytics, navigates transformed url and closes", async () => {
        const { result, calls, navigate } = await searchTwoHits();
        act(() => result.current.select(result.current.results[1]));

        expect(navigate).toHaveBeenCalledWith("/base/docs/second");
        expect(result.current.open).toBe(false);
        expect(new SearchStorage("serene-docs-search").getRecent()).toEqual(["docs"]);
        await waitFor(() => {
            const urls = calls.map((c) => c.url);
            expect(urls).toContain("http://api/v1/analytics/click");
            expect(urls).toContain("http://api/v1/analytics/query");
        });
        const click = calls.find((c) => c.url.endsWith("/v1/analytics/click"));
        expect(click?.body).toMatchObject({ id: "second", url: "/docs/second" });
    });

    it("Escape closes the modal via onKeyDown", async () => {
        const { result } = await searchTwoHits();
        act(() => result.current.onKeyDown(new KeyboardEvent("keydown", { key: "Escape" })));
        expect(result.current.open).toBe(false);
    });
});

describe("useSereneDocsSearch — hotkey", () => {
    it("toggles the modal on mod+k and exposes a label", () => {
        mockBackend();
        const { result } = renderHook(() => useSereneDocsSearch({ backendUrl: "http://api" }));
        expect(result.current.hotkeyLabel).toBe("Ctrl+K"); // jsdom is not a mac

        act(() => {
            window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", ctrlKey: true }));
        });
        expect(result.current.open).toBe(true);
        act(() => {
            window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", metaKey: true }));
        });
        expect(result.current.open).toBe(false);
        // without the modifier nothing happens
        act(() => {
            window.dispatchEvent(new KeyboardEvent("keydown", { key: "k" }));
        });
        expect(result.current.open).toBe(false);
    });
});

describe("useSereneDocsSearch — ask ai", () => {
    it("streams sources, deltas and the final model into askState", async () => {
        mockBackend({
            askFrames: [
                'data: {"type":"sources","sources":[{"n":1,"id":"a","url":"/docs/a#x","path":"a.md","title":"A"}]}\n\n',
                'data: {"type":"delta","text":"Vacuum "}\n\n',
                'data: {"type":"delta","text":"rebuilds."}\n\n',
                'data: {"type":"done","model":"m1"}\n\n',
            ],
        });
        const { result } = renderHook(() => useSereneDocsSearch({ backendUrl: "http://api" }));

        act(() => result.current.ask("what does vacuum do?"));
        expect(result.current.askState.phase).toBe("thinking");

        await waitFor(() => expect(result.current.askState.phase).toBe("done"));
        expect(result.current.askState.answer).toBe("Vacuum rebuilds.");
        expect(result.current.askState.sources).toHaveLength(1);
        expect(result.current.askState.model).toBe("m1");
        expect(result.current.conversation).toHaveLength(1);

        act(() => result.current.resetAsk());
        expect(result.current.askState.phase).toBe("idle");
        expect(result.current.conversation).toHaveLength(0);
    });

    it("keeps a conversation: follow-ups send prior exchanges as history", async () => {
        const calls = mockBackend({
            askFrames: ['data: {"type":"delta","text":"Answer."}\n\ndata: {"type":"done"}\n\n'],
        });
        const { result } = renderHook(() => useSereneDocsSearch({ backendUrl: "http://api" }));

        act(() => result.current.ask("first question"));
        await waitFor(() => expect(result.current.askState.phase).toBe("done"));

        act(() => result.current.ask("follow-up question"));
        await waitFor(() => expect(result.current.conversation).toHaveLength(2));
        await waitFor(() => expect(result.current.askState.phase).toBe("done"));

        const asks = calls.filter((c) => c.url.endsWith("/v1/ask"));
        expect(asks).toHaveLength(2);
        expect(asks[0].body?.history).toBeUndefined();
        expect(asks[1].body?.history).toEqual([
            { role: "user", content: "first question" },
            { role: "assistant", content: "Answer." },
        ]);
        expect(result.current.conversation.map((t) => t.question)).toEqual([
            "first question",
            "follow-up question",
        ]);
    });

    it("ignores a new question while an answer is still running", async () => {
        let release!: () => void;
        const gate = new Promise<void>((r) => (release = r));
        const calls = mockBackend({
            beforeAsk: () => gate,
            askFrames: ['data: {"type":"delta","text":"A."}\n\ndata: {"type":"done"}\n\n'],
        });
        const { result } = renderHook(() => useSereneDocsSearch({ backendUrl: "http://api" }));

        act(() => result.current.ask("first"));
        expect(result.current.askState.phase).toBe("thinking");

        act(() => result.current.ask("second — must be ignored"));
        expect(result.current.conversation).toHaveLength(1);
        expect(result.current.askState.question).toBe("first");

        release();
        await waitFor(() => expect(result.current.askState.phase).toBe("done"));
        expect(result.current.conversation).toHaveLength(1);
        expect(calls.filter((c) => c.url.endsWith("/v1/ask"))).toHaveLength(1);

        // once done, the next question goes through again
        act(() => result.current.ask("third"));
        await waitFor(() => expect(result.current.conversation).toHaveLength(2));
    });

    it("regenerate() re-asks the last question without duplicating the turn", async () => {
        const calls = mockBackend({
            askFrames: ['data: {"type":"delta","text":"A."}\n\ndata: {"type":"done"}\n\n'],
        });
        const { result } = renderHook(() => useSereneDocsSearch({ backendUrl: "http://api" }));

        act(() => result.current.ask("q1"));
        await waitFor(() => expect(result.current.askState.phase).toBe("done"));

        act(() => result.current.regenerate());
        await waitFor(() => expect(result.current.askState.phase).toBe("done"));

        expect(result.current.conversation).toHaveLength(1);
        const asks = calls.filter((c) => c.url.endsWith("/v1/ask"));
        expect(asks).toHaveLength(2);
        expect(asks[1].body?.history).toBeUndefined(); // replaced turn is not history
    });
});
