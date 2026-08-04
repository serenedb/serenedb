import { describe, expect, it, vi } from "vitest";
import { SereneSearchClient, SereneSearchError, consumeSse } from "../src/client";

type Call = { url: string; init?: RequestInit };

/** fetch stub answering with the given response (or per-call responses). */
function fakeFetch(...responses: Response[]) {
    const calls: Call[] = [];
    let i = 0;
    const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        calls.push({ url: String(input), init });
        return responses[Math.min(i++, responses.length - 1)];
    });
    return { fn: fn as unknown as typeof fetch, calls };
}

const json = (body: unknown, status = 200, statusText = "OK") =>
    new Response(JSON.stringify(body), {
        status,
        statusText,
        headers: { "Content-Type": "application/json" },
    });

async function expectError(p: Promise<unknown>): Promise<SereneSearchError> {
    try {
        await p;
    } catch (e) {
        return e as SereneSearchError;
    }
    throw new Error("expected the promise to reject");
}

const sse = (chunks: string[]) =>
    new Response(
        new ReadableStream<Uint8Array>({
            start(c) {
                const enc = new TextEncoder();
                for (const chunk of chunks) c.enqueue(enc.encode(chunk));
                c.close();
            },
        }),
        { status: 200 },
    );

describe("SereneSearchClient requests", () => {
    it("strips trailing slashes from the backend url", async () => {
        const { fn, calls } = fakeFetch(json({ ok: true }));
        const client = new SereneSearchClient({ backendUrl: "http://api:7700//", fetch: fn });
        await client.health();
        expect(calls[0].url).toBe("http://api:7700/v1/health");
    });

    it("posts search queries as json and returns the parsed response", async () => {
        const payload = { query: "vacuum", mode: "fulltext", results: [], total: 0, tookMs: 3 };
        const { fn, calls } = fakeFetch(json(payload));
        const client = new SereneSearchClient({ backendUrl: "http://api", fetch: fn });

        const res = await client.search("vacuum", { mode: "fulltext", limit: 5 });

        expect(res).toEqual(payload);
        expect(calls[0].url).toBe("http://api/v1/search");
        expect(calls[0].init?.method).toBe("POST");
        expect(JSON.parse(calls[0].init?.body as string)).toEqual({
            q: "vacuum",
            mode: "fulltext",
            limit: 5,
        });
        expect((calls[0].init?.headers as Record<string, string>)["Content-Type"]).toBe(
            "application/json",
        );
    });

    it("sends the bearer token on admin calls, without Content-Type on GETs", async () => {
        const { fn, calls } = fakeFetch(json({ configured: true }));
        const client = new SereneSearchClient({
            backendUrl: "http://api",
            token: "sk-local-abc",
            fetch: fn,
        });

        await client.getConfig();

        const headers = calls[0].init?.headers as Record<string, string>;
        expect(headers.Authorization).toBe("Bearer sk-local-abc");
        expect(headers["Content-Type"]).toBeUndefined();
    });

    it("surfaces the backend's error message with the http status", async () => {
        const { fn } = fakeFetch(json({ message: "index is not configured" }, 409, "Conflict"));
        const client = new SereneSearchClient({ backendUrl: "http://api", fetch: fn });

        const err = await expectError(client.sync());
        expect(err).toBeInstanceOf(SereneSearchError);
        expect(err.message).toBe("index is not configured");
        expect(err.status).toBe(409);
    });

    it("falls back to status text when the error body is not json", async () => {
        const { fn } = fakeFetch(new Response("oops", { status: 502, statusText: "Bad Gateway" }));
        const client = new SereneSearchClient({ backendUrl: "http://api", fetch: fn });

        const err = await expectError(client.health());
        expect(err.message).toBe("502 Bad Gateway");
        expect(err.status).toBe(502);
    });

    it("analytics beacons are fire-and-forget and swallow network failures", async () => {
        const calls: Call[] = [];
        const failing = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
            calls.push({ url: String(input), init });
            throw new Error("offline");
        }) as unknown as typeof fetch;
        const client = new SereneSearchClient({ backendUrl: "http://api", fetch: failing });

        client.reportQuery("vacuum", 3);
        client.reportClick("id1", "/docs/vacuum", "Vacuum");
        await new Promise((r) => setTimeout(r, 0)); // no unhandled rejection

        expect(calls.map((c) => c.url)).toEqual([
            "http://api/v1/analytics/query",
            "http://api/v1/analytics/click",
        ]);
        expect(calls[0].init?.keepalive).toBe(true);
        expect(JSON.parse(calls[0].init?.body as string)).toEqual({ q: "vacuum", hits: 3 });
    });
});

describe("SereneSearchClient.ask (SSE)", () => {
    it("parses each data frame into an event, skipping malformed ones", async () => {
        const { fn } = fakeFetch(
            sse([
                'data: {"type":"sources","sources":[{"n":1,"id":"a","url":"/x","path":"x.md","title":"X"}]}\n\n',
                'data: {"type":"del', // frame split across network chunks
                'ta","text":"Hel"}\n\ndata: {"type":"delta","text":"lo"}\n\n',
                "data: not-json\n\n",
                ': comment\nevent: end\ndata: {"type":"done","model":"m1"}\n\n',
            ]),
        );
        const client = new SereneSearchClient({ backendUrl: "http://api", fetch: fn });

        const events: unknown[] = [];
        await client.ask("what is vacuum?", (ev) => events.push(ev));

        expect(events).toEqual([
            { type: "sources", sources: [{ n: 1, id: "a", url: "/x", path: "x.md", title: "X" }] },
            { type: "delta", text: "Hel" },
            { type: "delta", text: "lo" },
            { type: "done", model: "m1" },
        ]);
    });

    it("throws SereneSearchError on a non-ok response", async () => {
        const { fn } = fakeFetch(json({ error: "rate limited" }, 429, "Too Many Requests"));
        const client = new SereneSearchClient({ backendUrl: "http://api", fetch: fn });

        const err = await expectError(client.ask("q", () => {}));
        expect(err.message).toBe("rate limited");
        expect(err.status).toBe(429);
    });
});

describe("progressStream", () => {
    it("delivers parsed frames and stops when unsubscribed", async () => {
        let seenSignal: AbortSignal | undefined;
        const fn = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
            seenSignal = init?.signal ?? undefined;
            return sse([
                'data: {"state":"running","steps":{}}\n\n',
                'data: {"state":"done","sections":42,"steps":{}}\n\n',
            ]);
        }) as unknown as typeof fetch;
        const client = new SereneSearchClient({ backendUrl: "http://api", fetch: fn });

        const frames: Array<{ state: string }> = [];
        const unsub = client.progressStream((p) => frames.push(p));
        await vi.waitFor(() => expect(frames).toHaveLength(2));
        expect(frames[1]).toMatchObject({ state: "done", sections: 42 });

        expect(seenSignal?.aborted).toBe(false);
        unsub();
        expect(seenSignal?.aborted).toBe(true);
    });
});

describe("consumeSse", () => {
    it("joins multi-line data fields and ignores non-data lines", async () => {
        const stream = sse(["retry: 100\ndata: line1\ndata: line2\n\n", "data:tight\n\n"]).body!;
        const out: string[] = [];
        await consumeSse(stream, (d) => out.push(d));
        expect(out).toEqual(["line1\nline2", "tight"]);
    });
});
