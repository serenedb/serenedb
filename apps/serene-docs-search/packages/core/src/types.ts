/* ============================================================
 * serene-search.config.json — the single config artifact shared
 * by the setup wizard (which generates it) and the sync backend
 * (which consumes it).
 * ============================================================ */

export interface GitSource {
    type: "git";
    /** Clone URL, e.g. https://github.com/acme/docs */
    url: string;
    branch?: string;
    /** Pin to an exact commit; disables commit-watch sync. */
    commit?: string;
    /**
     * Restrict indexing to parts of the repo: subdirectories and/or single
     * files. Comma-separated string ("docs, guides/faq.md") or an array.
     */
    subdir?: string | string[];
}

export interface FolderSource {
    type: "folder";
    /** Path as seen by the backend (mounted into the container). */
    path: string;
}

export interface SiteSource {
    type: "site";
    /** Crawl start URL. */
    url: string;
    /** Max link depth from the start URL; "all" = no limit. */
    depth?: number | "all";
    /** Also seed the crawl from sitemap.xml. */
    sitemap?: boolean;
}

export interface BucketSource {
    type: "bucket";
    /** s3://bucket/prefix — credentials come from the backend env. */
    uri: string;
    /** Custom endpoint for R2 / MinIO / other S3-compatible stores. */
    endpoint?: string;
    region?: string;
}

export type Source = GitSource | FolderSource | SiteSource | BucketSource;

export interface UrlMappingOptions {
    /** Site base the indexed files are served under, e.g. "https://docs.acme.dev" or "/docs". */
    baseUrl?: string;
    /** Source path prefix to drop before joining with baseUrl, e.g. "docs/". */
    stripPrefix?: string;
    /** quick-start.md -> quick-start (default true). */
    stripExtensions?: boolean;
    /** Files that map to their directory URL (default ["index", "README"]). */
    indexFiles?: string[];
}

export interface UrlMappingRule extends UrlMappingOptions {
    /** Glob matched against the source path. First matching rule wins. */
    match: string;
}

export interface UrlMapping extends UrlMappingOptions {
    /**
     * Per-path overrides for multi-site corpora. Shared options above remain
     * defaults; the first matching rule overrides them for that file.
     */
    rules?: UrlMappingRule[];
}

export interface ContentConfig {
    /** File extensions to index, with dot: [".md", ".mdx"]. */
    extensions: string[];
    /** Glob patterns to skip. */
    exclude?: string[];
    markdown?: {
        /** "split" = one section per heading (deep-link anchors); "whole" = one row per file. */
        mode: "split" | "whole";
        /**
         * Deepest heading level that opens its own section (1–6, default 4).
         * API references typically document each function under an h4 —
         * "#### ts_starts_with(prefix)" must be findable by name.
         */
        depth?: number;
    };
    html?: {
        /** CSS selectors scoping extraction, e.g. "article, main .content". */
        selectors?: string;
        /** Which tags become indexed sections. */
        tags?: string[];
        /**
         * Elements matching these selectors are removed before extraction —
         * boilerplate, query-result grids, version badges …
         */
        excludeSelectors?: string;
    };
    urlMapping?: UrlMapping;
}

export interface SearchSectionMatch {
    /** Globs matched against the final result URL or current browser URL. */
    urls?: string[];
    /**
     * Globs matched against the final URL pathname and indexed source path.
     * This makes both `/installation/**` and `docs/installation/**` useful.
     */
    paths?: string[];
}

export interface SearchSectionConfig {
    /** Stable machine-readable key used by headless consumers. */
    id: string;
    /** Heading rendered above this result group. */
    label: string;
    /** URL/path globs; the first configured section that matches wins. */
    match: SearchSectionMatch;
}

export interface SearchTypeConfig {
    /** "fulltext" = BM25 only; "hybrid" = BM25 + vector similarity (needs embeddings). */
    type: "fulltext" | "hybrid";
    /**
     * Drop semantic candidates whose cosine distance exceeds this (0..2).
     * Model-dependent — calibrate per embeddings model (nomic-embed-text ≈ 0.45,
     * all-minilm ≈ 0.8). Unset = no cut-off.
     */
    vectorDistanceThreshold?: number;
    /**
     * Curation rules: when the query matches `match` (case-insensitive, "*"
     * wildcards allowed), the section at `url` is pinned to the top.
     */
    pins?: { match: string; url: string }[];
    /** Hybrid fusion tuning (defaults: vectorWeight 0.7, k 60, window 50). */
    rrf?: { vectorWeight?: number; k?: number; window?: number };
    /** Snowball-stem terms so "run" matches "running" (default true). */
    stemming?: boolean;
    /**
     * Solr-format synonym map, one rule per line:
     * "db, database" (bidirectional) or "k8s => kubernetes" (one-way).
     * Applied at analysis time on both the indexed text and the query.
     */
    synonyms?: string;
    /** Words dropped before indexing/querying. Defaults to a small English list. */
    stopwords?: string[];
    /**
     * Optional result groups. The widget keeps relevance order inside each
     * section and prioritizes the section matching the current page.
     */
    sections?: SearchSectionConfig[];
}

export interface AiProvider {
    /**
     * "openai" — any OpenAI-compatible HTTP API (OpenAI, Gemini, OpenRouter, vLLM…).
     * "ollama" — an Ollama server; the backend pulls `model` automatically if missing.
     */
    kind: "openai" | "ollama";
    /**
     * openai: the API root (e.g. https://api.openai.com/v1).
     * ollama: server URL; "http://ollama:11434" refers to the Ollama container
     * the generated docker-compose bundles into the stack.
     */
    baseUrl?: string;
    /** Literal key or "${ENV_VAR}" expanded from the backend environment. */
    apiKey?: string;
    /** Chat model for answers / embeddings model for embeddings. */
    model?: string;
}

export interface AiConfig {
    /** Adds the "Ask AI" tab. */
    enabled: boolean;
    /** Provider for streamed answers (Ask AI). */
    answers?: AiProvider;
    /** Provider for embeddings (hybrid search). May differ from `answers`. */
    embeddings?: AiProvider;
    systemPrompt?: string;
}

export interface SyncConfig {
    /** commits = watch the git branch; poll = re-pull on interval; webhook = only POST /v1/reindex. */
    mode: "commits" | "poll" | "webhook";
    /** Poll / commit-check interval: "15m" | "1h" | "6h" | "24h" or any "<n>(s|m|h)". */
    interval?: string;
    /** Hash file content: skip unchanged sections, prune deleted ones. */
    snapshots?: boolean;
}

export interface ServerConfig {
    /** Backend HTTP port (default 7700). */
    port?: number;
}

export interface SereneDBConfig {
    host?: string;
    port?: number;
    database?: string;
    user?: string;
    password?: string;
    /** Table holding indexed sections (default "serene_docs_sections"). */
    table?: string;
}

/**
 * Optional MCP server for external AI agents (Claude Code, Cursor…). Off by
 * default — the backend's own Ask AI never needs it. When enabled, the deploy
 * compose gets a `docs-search-mcp` service that wraps the backend's API.
 */
export interface McpConfig {
    enabled?: boolean;
}

export interface SereneSearchConfig {
    version: 1;
    /** Display name, shows up in health responses. */
    project?: string;
    source: Source;
    content: ContentConfig;
    search: SearchTypeConfig;
    ai?: AiConfig;
    sync: SyncConfig;
    server?: ServerConfig;
    serenedb?: SereneDBConfig;
    /** Expose an MCP server for external agents (opt-in). */
    mcp?: McpConfig;
}

/* ============================================================
 * Backend HTTP API
 * ============================================================ */

export type SectionKind = "heading" | "text" | "code";

export interface SearchResultItem {
    id: string;
    /** Final URL to navigate to, including #anchor when present. */
    url: string;
    /** Source path (file path or crawled page URL). */
    path: string;
    anchor?: string;
    title: string;
    /** Breadcrumb trail, e.g. "Docs › Replication › Read replicas". */
    crumb: string;
    /** Top-level group label the item is bucketed under. */
    group: string;
    kind: SectionKind;
    /** BM25 relevance (fulltext hits). */
    score?: number;
    /** Vector similarity 0..1 (semantic hits). */
    vecScore?: number;
    /** Found only by the semantic (vector) pass — shown with an "AI suggested" badge. */
    aiSuggested?: boolean;
    /** Placed by a curation rule (search.pins), not by relevance. */
    pinned?: boolean;
    /** Content fragment around the match; lexical hits wrap terms in <mark>…</mark>. */
    snippet?: string;
}

export interface SearchResponse {
    query: string;
    mode: "fulltext" | "hybrid";
    results: SearchResultItem[];
    total: number;
    tookMs: number;
    /** True when results came from the typo-tolerant (fuzzy) fallback. */
    fuzzy?: boolean;
    /** True when no document matched every term — results are partial matches. */
    partial?: boolean;
    /** "Did you mean" — the corrected query when the fuzzy pass fixed a typo. */
    correctedQuery?: string;
}

export interface HealthResponse {
    ok: boolean;
    version: string;
    project?: string;
    serenedb: { connected: boolean; version?: string };
    index: {
        ready: boolean;
        building: boolean;
        sections: number;
        documents: number;
        lastSyncAt?: string;
    };
    features: { ai: boolean; hybrid: boolean };
    /** Search-behaviour hints for the widget. */
    searchType: "fulltext" | "hybrid";
    /** Public grouping rules used by the widget; absent for legacy configs. */
    searchSections?: SearchSectionConfig[];
}

export type StepStatus = "pending" | "running" | "done" | "skipped" | "error";

export interface SyncProgress {
    state: "idle" | "running" | "done" | "error";
    startedAt?: string;
    finishedAt?: string;
    error?: string;
    /** Human summary of the source, e.g. "github.com/acme/docs @ main". */
    source?: string;
    steps: {
        fetch: { status: StepStatus; files?: number; detail?: string };
        parse: { status: StepStatus; sections?: number; detail?: string };
        embed: { status: StepStatus; done?: number; total?: number };
        index: { status: StepStatus; detail?: string };
    };
    /** Set when state = done. */
    sections?: number;
    documents?: number;
    tookMs?: number;
    incremental?: boolean;
}

export interface AskSource {
    n: number;
    id: string;
    path: string;
    url: string;
    title: string;
}

/** SSE events emitted by POST /v1/ask */
export type AskEvent =
    | { type: "sources"; sources: AskSource[] }
    | { type: "delta"; text: string }
    /** The agent ran a retrieval tool (search_docs / read_section). */
    | { type: "tool"; name: string; detail?: string }
    | { type: "done"; model?: string }
    | { type: "error"; message: string };

/** One prior exchange message in an Ask AI conversation, oldest first. */
export interface AskMessage {
    role: "user" | "assistant";
    content: string;
}

/** Full text of one indexed section (GET /v1/section?url=…). */
export interface SectionResponse {
    id: string;
    url: string;
    title: string;
    crumb: string;
    content: string;
}

export interface ConfigStatusResponse {
    /** Whether the backend has a config loaded (file or pushed). */
    configured: boolean;
    config?: SereneSearchConfig;
}
