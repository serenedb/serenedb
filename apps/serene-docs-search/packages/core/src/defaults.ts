import type { AiProvider, SereneSearchConfig, Source } from "./types";

export const DEFAULT_BACKEND_PORT = 7700;
export const DEFAULT_SERENEDB_PORT = 7890;
export const DEFAULT_TABLE = "serene_docs_sections";
export const CONFIG_FILENAME = "serene-search.config.json";

export const DEFAULT_EXTENSIONS = [".md", ".mdx"];
export const KNOWN_EXTENSIONS = [".md", ".mdx", ".html", ".rst", ".txt", ".ipynb", ".pdf"];
export const DEFAULT_HTML_TAGS = ["h1", "h2", "h3", "h4", "p", "li", "pre", "code", "table"];
export const DEFAULT_EXCLUDE = ["**/node_modules/**"];
export const DEFAULT_SYSTEM_PROMPT =
    "Answer from the indexed docs only. Cite sources. If unsure, say so.";

/** The Ollama container the generated docker-compose can bundle. */
export const BUNDLED_OLLAMA_URL = "http://ollama:11434";

export function defaultProvider(
    kind: AiProvider["kind"],
    role: "answers" | "embeddings",
): AiProvider {
    if (kind === "ollama") {
        return {
            kind,
            baseUrl: BUNDLED_OLLAMA_URL,
            model: role === "answers" ? "llama3.2" : "nomic-embed-text",
        };
    }
    return {
        kind,
        baseUrl: "https://api.openai.com/v1",
        model: role === "answers" ? "gpt-4o-mini" : "text-embedding-3-small",
    };
}

export function defaultConfig(source: Source): SereneSearchConfig {
    return {
        version: 1,
        source,
        content: {
            extensions: [...DEFAULT_EXTENSIONS],
            exclude: [...DEFAULT_EXCLUDE],
            markdown: { mode: "split" },
            html: { selectors: "article, main", tags: [...DEFAULT_HTML_TAGS] },
            urlMapping: { stripExtensions: true, indexFiles: ["index", "README"] },
        },
        search: { type: "hybrid" },
        ai: { enabled: false },
        sync: { mode: source.type === "git" ? "commits" : "poll", interval: "1h", snapshots: true },
        server: { port: DEFAULT_BACKEND_PORT },
        serenedb: { host: "serenedb", port: DEFAULT_SERENEDB_PORT, table: DEFAULT_TABLE },
    };
}

/** "15m" | "1h" | "6h" | "24h" | "90s" -> milliseconds (null if unparseable). */
export function parseInterval(interval: string | undefined): number | null {
    if (!interval) return null;
    const m = /^(\d+)\s*(s|m|h|d)$/i.exec(interval.trim());
    if (!m) return null;
    const n = Number(m[1]);
    const unit = m[2].toLowerCase();
    const mult = unit === "s" ? 1000 : unit === "m" ? 60_000 : unit === "h" ? 3_600_000 : 86_400_000;
    return n * mult;
}

/** Random token for widget<->backend admin auth: sk-local-<hex>. */
export function generateToken(): string {
    const bytes = new Uint8Array(12);
    if (typeof crypto !== "undefined" && crypto.getRandomValues) {
        crypto.getRandomValues(bytes);
    } else {
        for (let i = 0; i < bytes.length; i++) bytes[i] = Math.floor(Math.random() * 256);
    }
    return "sk-local-" + Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/** Short human summary of a source, e.g. "github.com/acme/docs @ main". */
export function describeSource(source: Source): string {
    switch (source.type) {
        case "git": {
            const repo = source.url.replace(/^https?:\/\//, "").replace(/\.git$/, "");
            return `${repo} @ ${source.commit || source.branch || "main"}`;
        }
        case "folder":
            return `${source.path} (mounted)`;
        case "site":
            return `${source.url} · depth ${source.depth ?? 2}`;
        case "bucket":
            return source.uri;
    }
}
