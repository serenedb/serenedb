import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
    HealthResponse,
    SearchResultItem,
    SearchSectionConfig,
    SereneSearchClient,
} from "@serenedb/docs-search-core";
import { highlightOnPage, rememberHighlight, tokenizeQuery } from "../lib/highlight";
import { formatHotkey, parseHotkey } from "../lib/hotkey";
import {
    groupResultsBySections,
    type SectionResultGroup,
} from "../lib/sections";
import { SearchStorage } from "../lib/storage";
import { useAskAi, type AskState, type AskTurn } from "./useAskAi";
import { useConnection, type ConnectionStatus } from "./useConnection";
import { useRecentQueries } from "./useRecentQueries";

export type { AskState, AskTurn } from "./useAskAi";
export type { ConnectionStatus } from "./useConnection";

export type SearchPhase = "idle" | "searching" | "done" | "no-results";

export type ResultGroup = SectionResultGroup;

export interface UseSereneDocsSearchOptions {
    /** Backend to talk to. Omit to let the first-run wizard configure one. */
    backendUrl?: string;
    /** Admin token — only needed for the setup/sync endpoints. */
    token?: string;
    /** Keyboard shortcut that toggles the modal. Default "mod+k"; false disables. */
    hotkey?: string | false;
    /** Debounce for the instant (fulltext) pass. */
    debounceMs?: number;
    /** Extra delay before the semantic (hybrid) pass fires. */
    semanticDebounceMs?: number;
    limit?: number;
    /** Queries offered in the empty state under "Suggested". */
    suggestions?: string[];
    /**
     * Optional result sections. Overrides rules advertised by the backend;
     * first match wins, and the section matching the current page comes first.
     */
    sections?: SearchSectionConfig[];
    /** Override the browser location used for contextual section priority. */
    contextUrl?: string;
    /** localStorage namespace. */
    storageKey?: string;
    /** SPA navigation (e.g. docusaurus history.push). Default: location.assign. */
    navigate?: (url: string) => void;
    /** Rewrite result URLs before navigation. */
    transformUrl?: (url: string) => string;
    /** Called after a result is chosen. */
    onSelect?: (item: SearchResultItem) => void;
    /** Controlled open state. */
    open?: boolean;
    onOpenChange?: (open: boolean) => void;
}

export interface SereneDocsSearch {
    // modal
    open: boolean;
    setOpen: (open: boolean) => void;
    toggle: () => void;
    hotkeyLabel: string;
    // connection
    status: ConnectionStatus;
    health: HealthResponse | null;
    /** Effective backend address (prop or saved connection), null when unconfigured. */
    backendUrl: string | null;
    client: SereneSearchClient | null;
    /** Point the hook at a backend at runtime (the wizard calls this). */
    connect: (backendUrl: string, token?: string) => void;
    disconnect: () => void;
    refreshHealth: () => Promise<void>;
    // search
    query: string;
    setQuery: (q: string) => void;
    phase: SearchPhase;
    /** True once the semantic pass merged in (hybrid installs). */
    semantic: boolean;
    /** True while the semantic pass is still pending. */
    semanticPending: boolean;
    /** Results came from the typo-tolerant (fuzzy) fallback. */
    fuzzy: boolean;
    /** No document matched every term — showing partial matches. */
    partial: boolean;
    /** "Did you mean" suggestion when the fuzzy pass corrected a typo. */
    correctedQuery: string | null;
    results: SearchResultItem[];
    groups: ResultGroup[];
    /** True when configured sections (rather than legacy result groups) are active. */
    sectioned: boolean;
    activeSectionId?: string;
    total: number;
    tookMs: number;
    semanticTookMs: number | null;
    // selection & navigation
    selectedIndex: number;
    setSelectedIndex: (i: number) => void;
    moveSelection: (delta: 1 | -1) => void;
    select: (item?: SearchResultItem) => void;
    /** Keydown handler covering ↑ ↓ Enter Escape. */
    onKeyDown: (e: React.KeyboardEvent | KeyboardEvent) => void;
    // empty state
    recent: string[];
    suggestions: string[];
    removeRecent: (q: string) => void;
    clearRecent: () => void;
    // ask ai
    aiEnabled: boolean;
    ask: (q: string) => void;
    /** The latest exchange (idle placeholder when the chat is empty). */
    askState: AskState;
    /** Full chat: every question→answer exchange, oldest first. */
    conversation: AskTurn[];
    /** Re-ask the last question, replacing its answer. */
    regenerate: () => void;
    resetAsk: () => void;
    storage: SearchStorage;
}

export function useSereneDocsSearch(
    options: UseSereneDocsSearchOptions = {},
): SereneDocsSearch {
    const {
        hotkey = "mod+k",
        debounceMs = 120,
        semanticDebounceMs = 350,
        limit = 12,
        storageKey = "serene-docs-search",
    } = options;

    const storage = useMemo(() => new SearchStorage(storageKey), [storageKey]);

    const { backendUrl, client, status, setStatus, health, refreshHealth, connect, disconnect } =
        useConnection(options, storage);

    /* ---------- modal open state (controlled or not) ---------- */

    const [openState, setOpenState] = useState(false);
    const open = options.open ?? openState;
    const setOpen = useCallback(
        (next: boolean) => {
            setOpenState(next);
            options.onOpenChange?.(next);
        },
        [options.onOpenChange],
    );
    const toggle = useCallback(() => setOpen(!open), [open, setOpen]);

    // health check on open + offline retry loop
    useEffect(() => {
        if (!open) return;
        void refreshHealth();
    }, [open, refreshHealth]);
    useEffect(() => {
        if (!open || status !== "offline") return;
        const t = window.setInterval(() => void refreshHealth(), 5000);
        return () => window.clearInterval(t);
    }, [open, status, refreshHealth]);

    /* ---------- hotkey ---------- */

    const hotkeyLabel = useMemo(() => formatHotkey(hotkey), [hotkey]);
    useEffect(() => {
        if (hotkey === false || typeof window === "undefined") return;
        const spec = parseHotkey(hotkey);
        const onKey = (e: KeyboardEvent) => {
            const mod = e.metaKey || e.ctrlKey;
            if (
                e.key.toLowerCase() === spec.key &&
                (!spec.mod || mod) &&
                !e.altKey &&
                !e.shiftKey
            ) {
                e.preventDefault();
                toggle();
            }
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [hotkey, toggle]);

    /* ---------- search ---------- */

    const [query, setQueryState] = useState("");
    const [phase, setPhase] = useState<SearchPhase>("idle");
    const [results, setResults] = useState<SearchResultItem[]>([]);
    const [semantic, setSemantic] = useState(false);
    const [semanticPending, setSemanticPending] = useState(false);
    const [fuzzy, setFuzzy] = useState(false);
    const [partial, setPartial] = useState(false);
    const [correctedQuery, setCorrectedQuery] = useState<string | null>(null);
    const [tookMs, setTookMs] = useState(0);
    const [semanticTookMs, setSemanticTookMs] = useState<number | null>(null);
    const [selectedIndex, setSelectedIndex] = useState(0);

    const seqRef = useRef(0);
    const timersRef = useRef<number[]>([]);
    const abortRef = useRef<AbortController | null>(null);

    const clearTimers = () => {
        timersRef.current.forEach((t) => window.clearTimeout(t));
        timersRef.current = [];
        abortRef.current?.abort();
        abortRef.current = null;
    };

    const hybridEnabled = health?.searchType === "hybrid";

    const setQuery = useCallback(
        (q: string) => {
            setQueryState(q);
            clearTimers();
            const seq = ++seqRef.current;
            if (!q.trim() || !client) {
                setPhase("idle");
                setResults([]);
                setSemantic(false);
                setSemanticPending(false);
                setFuzzy(false);
                setPartial(false);
                setCorrectedQuery(null);
                setSelectedIndex(0);
                return;
            }
            setPhase("searching");
            setSemantic(false);
            setSemanticPending(hybridEnabled);
            setSelectedIndex(0);
            // clear the previous response's banner state up front: the error
            // paths below set phase without a fresh response, and a stale
            // "did you mean" must not attach to the new query
            setFuzzy(false);
            setPartial(false);
            setCorrectedQuery(null);

            const ctrl = new AbortController();
            abortRef.current = ctrl;

            // pass 1 — instant fulltext
            timersRef.current.push(
                window.setTimeout(async () => {
                    try {
                        const res = await client.search(q, {
                            mode: "fulltext",
                            limit,
                            signal: ctrl.signal,
                        });
                        if (seqRef.current !== seq) return;
                        setResults(res.results);
                        setTookMs(res.tookMs);
                        setFuzzy(Boolean(res.fuzzy));
                        setPartial(Boolean(res.partial));
                        setCorrectedQuery(res.correctedQuery ?? null);
                        setPhase(res.results.length ? "done" : hybridEnabled ? "searching" : "no-results");
                        if (status === "offline") void refreshHealth();
                    } catch (err) {
                        if (seqRef.current !== seq || (err as Error).name === "AbortError") return;
                        setStatus("offline");
                        setPhase(results.length ? "done" : "no-results");
                    }
                }, debounceMs),
            );

            // pass 2 — semantic merge (hybrid installs only)
            if (hybridEnabled) {
                timersRef.current.push(
                    window.setTimeout(async () => {
                        try {
                            const res = await client.search(q, {
                                mode: "hybrid",
                                limit,
                                signal: ctrl.signal,
                            });
                            if (seqRef.current !== seq) return;
                            setResults(res.results);
                            setSemanticTookMs(res.tookMs);
                            setSemantic(res.mode === "hybrid");
                            setSemanticPending(false);
                            setFuzzy(Boolean(res.fuzzy));
                            setPartial(Boolean(res.partial));
                            setCorrectedQuery(res.correctedQuery ?? null);
                            setPhase(res.results.length ? "done" : "no-results");
                        } catch {
                            if (seqRef.current !== seq) return;
                            setSemanticPending(false);
                            setPhase((p) => (p === "searching" ? (results.length ? "done" : "no-results") : p));
                        }
                    }, debounceMs + semanticDebounceMs),
                );
            }
        },
        [client, hybridEnabled, limit, debounceMs, semanticDebounceMs, status, refreshHealth],
    );

    useEffect(() => clearTimers, []);

    // analytics: report the query once the user stops typing (Typesense
    // counts a query after a pause — same idea, 2.5s of silence)
    const settleReported = useRef("");
    useEffect(() => {
        if (!client || phase === "idle" || phase === "searching") return;
        const q = query.trim().toLowerCase();
        if (!q || settleReported.current === q) return;
        const t = window.setTimeout(() => {
            settleReported.current = q;
            client.reportQuery(q, results.length);
        }, 2500);
        return () => window.clearTimeout(t);
    }, [client, query, phase, results.length]);

    const effectiveSections = options.sections ?? health?.searchSections;
    const currentContextUrl =
        options.contextUrl ??
        (typeof window !== "undefined" ? window.location.href : undefined);
    const organized = useMemo(
        () => groupResultsBySections(results, effectiveSections, currentContextUrl),
        [results, effectiveSections, currentContextUrl],
    );
    const groups = organized.groups;
    /** Rendered/selection order: relevance is unchanged inside each section. */
    const flat = organized.results;

    /* ---------- selection + navigation ---------- */

    const moveSelection = useCallback(
        (delta: 1 | -1) => {
            const n = flat.length;
            if (!n) return;
            setSelectedIndex((i) => (i + delta + n) % n);
        },
        [flat.length],
    );

    const select = useCallback(
        (item?: SearchResultItem) => {
            const chosen = item ?? flat[selectedIndex];
            if (!chosen) return;
            storage.pushRecent(query);
            // a click is also the strongest "this query worked" signal
            const settled = query.trim().toLowerCase();
            if (settled && settleReported.current !== settled) {
                settleReported.current = settled;
                client?.reportQuery(settled, flat.length);
            }
            client?.reportClick(chosen.id, chosen.url, chosen.title);
            const url = options.transformUrl ? options.transformUrl(chosen.url) : chosen.url;
            setOpen(false);
            options.onSelect?.(chosen);

            const terms = tokenizeQuery(query);
            if (options.navigate) {
                options.navigate(url);
                // SPA: the page mounts asynchronously — highlight when it lands
                window.setTimeout(() => void highlightOnPage(chosen.anchor, terms), 80);
            } else {
                const target = new URL(url, window.location.href);
                const samePage =
                    target.pathname === window.location.pathname &&
                    target.origin === window.location.origin;
                if (samePage) {
                    if (chosen.anchor) window.location.hash = chosen.anchor;
                    void highlightOnPage(chosen.anchor, terms);
                } else {
                    rememberHighlight(url, chosen.anchor, query);
                    window.location.assign(url);
                }
            }
        },
        [flat, selectedIndex, query, storage, client, options.navigate, options.transformUrl, options.onSelect, setOpen],
    );

    const onKeyDown = useCallback(
        (e: React.KeyboardEvent | KeyboardEvent) => {
            if (e.key === "ArrowDown") {
                e.preventDefault();
                moveSelection(1);
            } else if (e.key === "ArrowUp") {
                e.preventDefault();
                moveSelection(-1);
            } else if (e.key === "Enter") {
                e.preventDefault();
                select();
            } else if (e.key === "Escape") {
                e.preventDefault();
                setOpen(false);
            }
        },
        [moveSelection, select, setOpen],
    );

    /* ---------- empty state + ask ai ---------- */

    const { recent, removeRecent, clearRecent } = useRecentQueries(storage, open, query);

    const aiEnabled = Boolean(health?.features.ai);
    const { conversation, askState, ask, regenerate, resetAsk } = useAskAi(client);

    return {
        open,
        setOpen,
        toggle,
        hotkeyLabel,
        status,
        health,
        backendUrl,
        client,
        connect,
        disconnect,
        refreshHealth,
        query,
        setQuery,
        phase,
        semantic,
        semanticPending,
        fuzzy,
        partial,
        correctedQuery,
        results: flat,
        groups,
        sectioned: organized.sectioned,
        activeSectionId: organized.activeSectionId,
        total: flat.length,
        tookMs,
        semanticTookMs,
        selectedIndex,
        setSelectedIndex,
        moveSelection,
        select,
        onKeyDown,
        recent,
        suggestions: options.suggestions ?? [],
        removeRecent,
        clearRecent,
        aiEnabled,
        ask,
        askState,
        conversation,
        regenerate,
        resetAsk,
        storage,
    };
}
