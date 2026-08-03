import type {
    AskEvent,
    AskMessage,
    ConfigStatusResponse,
    HealthResponse,
    SearchResponse,
    SectionResponse,
    SereneSearchConfig,
    SyncProgress,
} from "./types";

export interface ClientOptions {
    backendUrl: string;
    /** Admin token — only needed for config/sync endpoints. */
    token?: string;
    fetch?: typeof fetch;
}

export class SereneSearchError extends Error {
    status: number;
    constructor(message: string, status: number) {
        super(message);
        this.name = "SereneSearchError";
        this.status = status;
    }
}

/**
 * Thin fetch client for the SereneDocsSearch backend. Framework-agnostic:
 * used by the React hook, the embed bundle and usable server-side.
 */
export class SereneSearchClient {
    private base: string;
    private token?: string;
    private fetchFn: typeof fetch;

    constructor(opts: ClientOptions) {
        this.base = opts.backendUrl.replace(/\/+$/, "");
        this.token = opts.token;
        this.fetchFn = opts.fetch ?? ((...a) => fetch(...a));
    }

    private headers(json = true): Record<string, string> {
        const h: Record<string, string> = {};
        if (json) h["Content-Type"] = "application/json";
        if (this.token) h["Authorization"] = `Bearer ${this.token}`;
        return h;
    }

    private async request<T>(path: string, init?: RequestInit): Promise<T> {
        const res = await this.fetchFn(this.base + path, init);
        if (!res.ok) {
            let message = `${res.status} ${res.statusText}`;
            try {
                const body = (await res.json()) as { error?: string; message?: string };
                if (body.message || body.error) message = body.message ?? body.error!;
            } catch {
                /* non-JSON error body */
            }
            throw new SereneSearchError(message, res.status);
        }
        return (await res.json()) as T;
    }

    health(signal?: AbortSignal): Promise<HealthResponse> {
        return this.request<HealthResponse>("/v1/health", { signal });
    }

    search(
        q: string,
        opts?: { mode?: "fulltext" | "hybrid"; limit?: number; signal?: AbortSignal },
    ): Promise<SearchResponse> {
        return this.request<SearchResponse>("/v1/search", {
            method: "POST",
            headers: this.headers(),
            body: JSON.stringify({ q, mode: opts?.mode, limit: opts?.limit }),
            signal: opts?.signal,
        });
    }

    /**
     * Streams an AI answer over SSE. Calls onEvent for each event;
     * resolves when the stream ends. Abort via signal. Pass prior exchanges
     * as `history` (oldest first) to continue a conversation.
     */
    async ask(
        q: string,
        onEvent: (ev: AskEvent) => void,
        signal?: AbortSignal,
        history?: AskMessage[],
    ): Promise<void> {
        const res = await this.fetchFn(this.base + "/v1/ask", {
            method: "POST",
            headers: this.headers(),
            body: JSON.stringify({ q, history: history?.length ? history : undefined }),
            signal,
        });
        if (!res.ok || !res.body) {
            let message = `${res.status} ${res.statusText}`;
            try {
                const body = (await res.json()) as { error?: string; message?: string };
                if (body.message || body.error) message = body.message ?? body.error!;
            } catch {
                /* ignore */
            }
            throw new SereneSearchError(message, res.status);
        }
        await consumeSse(res.body, (data) => {
            try {
                onEvent(JSON.parse(data) as AskEvent);
            } catch {
                /* skip malformed frame */
            }
        });
    }

    /** Full text of one indexed section by its exact url; null when unknown. */
    async section(url: string, signal?: AbortSignal): Promise<SectionResponse | null> {
        const res = await this.fetchFn(
            this.base + "/v1/section?url=" + encodeURIComponent(url),
            { signal },
        );
        if (res.status === 404) return null;
        if (!res.ok) throw new SereneSearchError(`${res.status} ${res.statusText}`, res.status);
        return (await res.json()) as SectionResponse;
    }

    /** Kick off a sync. Requires the admin token if the backend has one set. */
    sync(): Promise<{ started: boolean }> {
        return this.request("/v1/sync", { method: "POST", headers: this.headers() });
    }

    progress(signal?: AbortSignal): Promise<SyncProgress> {
        return this.request<SyncProgress>("/v1/sync/progress", { signal });
    }

    /** Subscribe to live sync progress (SSE). Returns an unsubscribe fn. */
    progressStream(onProgress: (p: SyncProgress) => void): () => void {
        const ctrl = new AbortController();
        void (async () => {
            try {
                const res = await this.fetchFn(this.base + "/v1/sync/progress?stream=1", {
                    headers: { Accept: "text/event-stream" },
                    signal: ctrl.signal,
                });
                if (!res.ok || !res.body) return;
                await consumeSse(res.body, (data) => {
                    try {
                        onProgress(JSON.parse(data) as SyncProgress);
                    } catch {
                        /* skip malformed frame */
                    }
                });
            } catch {
                /* aborted or network error — caller falls back to polling */
            }
        })();
        return () => ctrl.abort();
    }

    /** Fire-and-forget analytics beacons (settled queries / result clicks). */
    reportQuery(q: string, hits: number): void {
        void this.fetchFn(this.base + "/v1/analytics/query", {
            method: "POST",
            headers: this.headers(),
            body: JSON.stringify({ q, hits }),
            keepalive: true,
        }).catch(() => {});
    }

    reportClick(id: string, url: string, title: string): void {
        void this.fetchFn(this.base + "/v1/analytics/click", {
            method: "POST",
            headers: this.headers(),
            body: JSON.stringify({ id, url, title }),
            keepalive: true,
        }).catch(() => {});
    }

    getConfig(signal?: AbortSignal): Promise<ConfigStatusResponse> {
        return this.request<ConfigStatusResponse>("/v1/config", {
            headers: this.headers(false),
            signal,
        });
    }

    putConfig(config: SereneSearchConfig): Promise<{ saved: boolean }> {
        return this.request("/v1/config", {
            method: "PUT",
            headers: this.headers(),
            body: JSON.stringify(config),
        });
    }
}

/** Minimal SSE reader: emits each `data:` payload. */
export async function consumeSse(
    body: ReadableStream<Uint8Array>,
    onData: (data: string) => void,
): Promise<void> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx: number;
        while ((idx = buf.indexOf("\n\n")) >= 0) {
            const frame = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            const data = frame
                .split("\n")
                .filter((l) => l.startsWith("data:"))
                .map((l) => l.slice(5).trimStart())
                .join("\n");
            if (data) onData(data);
        }
    }
}
