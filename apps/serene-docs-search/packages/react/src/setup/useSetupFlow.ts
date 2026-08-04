import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
    DEFAULT_BACKEND_PORT,
    SereneSearchClient,
    defaultConfig,
    generateToken,
    type SereneSearchConfig,
    type SyncProgress,
} from "@serenedb/docs-search-core";
import type { SereneDocsSearch } from "../hooks/useSereneDocsSearch";
import type { ConnState } from "./types";

export type SetupPhase = "wizard" | "deploy" | "indexing" | "search";

export interface UseSetupFlowOptions {
    search: SereneDocsSearch;
    /** First-run setup wizard: "auto" shows it when no backend is known. */
    setup: "auto" | "never";
    /** True when the host pinned backendUrl via props — setup can't be re-run. */
    pinnedBackend: boolean;
    toast: (msg: string) => void;
}

export interface SetupFlow {
    phase: SetupPhase;
    setPhase: (p: SetupPhase) => void;
    step: number;
    goStep: (next: number) => void;
    config: SereneSearchConfig;
    update: (mutate: (c: SereneSearchConfig) => void) => void;
    /** Admin token generated for this wizard run (baked into the compose file). */
    token: string;
    backendUrl: string;
    setBackendUrl: (v: string) => void;
    tokenInput: string;
    setTokenInput: (v: string) => void;
    conn: ConnState;
    setConn: (c: ConnState) => void;
    progress: SyncProgress | null;
    startIndexing: () => Promise<void>;
    finishSetup: () => void;
    /** Forget the saved backend + draft and start first-run setup over. */
    resetSetup: () => void;
    /** True when setup can be (re)entered from the search screen. */
    canReset: boolean;
}

/**
 * First-run state machine: wizard (4 steps) → deploy (compose + connection
 * test) → indexing (progress stream) → search. Lives outside the overlay so
 * closing the modal mid-indexing doesn't lose the draft or the progress feed.
 */
export function useSetupFlow({ search, setup, pinnedBackend, toast }: UseSetupFlowOptions): SetupFlow {
    const [phase, setPhase] = useState<SetupPhase>("search");
    const [step, setStep] = useState(1);
    const [config, setConfig] = useState<SereneSearchConfig>(() =>
        defaultConfig({ type: "git", url: "", branch: "main" }),
    );
    const wizardToken = useRef<string>("");
    const [backendUrl, setBackendUrl] = useState("");
    const [tokenInput, setTokenInput] = useState("");
    const [conn, setConn] = useState<ConnState>({ kind: "idle" });
    const [progress, setProgress] = useState<SyncProgress | null>(null);
    const progressUnsub = useRef<(() => void) | null>(null);

    // restore or seed the wizard draft when the modal opens unconfigured
    useEffect(() => {
        if (!search.open) return;
        if (search.status !== "unconfigured") {
            setPhase((p) => (p === "indexing" ? p : "search"));
            return;
        }
        if (setup === "never") {
            setPhase("search");
            return;
        }
        const draft = search.storage.getDraft();
        if (draft) {
            setConfig(draft.config);
            setStep(Math.min(Math.max(draft.step, 1), 4));
            wizardToken.current = draft.token;
        } else if (!wizardToken.current) {
            wizardToken.current = generateToken();
        }
        setTokenInput((t) => t || wizardToken.current);
        setBackendUrl((u) => u || `http://localhost:${config.server?.port ?? DEFAULT_BACKEND_PORT}`);
        setPhase((p) => (p === "search" ? "wizard" : p));
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [search.open, search.status, setup]);

    const update = useCallback(
        (mutate: (c: SereneSearchConfig) => void) => {
            setConfig((prev) => {
                const next = JSON.parse(JSON.stringify(prev)) as SereneSearchConfig;
                mutate(next);
                search.storage.saveDraft({ config: next, step, token: wizardToken.current });
                return next;
            });
        },
        [search.storage, step],
    );

    const goStep = useCallback(
        (next: number) => {
            setStep(next);
            search.storage.saveDraft({ config, step: next, token: wizardToken.current });
        },
        [config, search.storage],
    );

    /* ---------------- deploy -> indexing ---------------- */

    const adminClient = useMemo(
        () =>
            backendUrl
                ? new SereneSearchClient({ backendUrl, token: tokenInput || undefined })
                : null,
        [backendUrl, tokenInput],
    );

    const startIndexing = useCallback(async () => {
        if (!adminClient || conn.kind !== "ok") return;
        setPhase("indexing");
        setProgress(null);
        try {
            const status = await adminClient.getConfig().catch(() => ({ configured: false }));
            if (!status.configured) {
                await adminClient.putConfig(config);
            }
            await adminClient.sync().catch(() => ({ started: false }));
            progressUnsub.current?.();
            progressUnsub.current = adminClient.progressStream((p) => {
                setProgress(p);
            });
        } catch (err) {
            setProgress({
                state: "error",
                error: (err as Error).message,
                steps: {
                    fetch: { status: "error" },
                    parse: { status: "pending" },
                    embed: { status: "pending" },
                    index: { status: "pending" },
                },
            });
        }
    }, [adminClient, conn.kind, config]);

    // fall back to polling if the SSE stream dies; finalize when done
    useEffect(() => {
        if (phase !== "indexing" || !adminClient) return;
        const poll = window.setInterval(async () => {
            try {
                const p = await adminClient.progress();
                setProgress((prev) => (prev?.state === "done" ? prev : p));
            } catch {
                /* keep last snapshot */
            }
        }, 1500);
        return () => window.clearInterval(poll);
    }, [phase, adminClient]);

    const finishSetup = useCallback(() => {
        progressUnsub.current?.();
        progressUnsub.current = null;
        search.storage.clearDraft();
        search.connect(backendUrl, tokenInput || undefined);
        setPhase("search");
        const sections = progress?.sections;
        toast(
            sections ? `index ready — ${sections.toLocaleString("en-US")} sections · connected` : "connected",
        );
    }, [search, backendUrl, tokenInput, progress?.sections, toast]);

    useEffect(() => {
        if (phase === "indexing" && progress?.state === "done") {
            const t = window.setTimeout(finishSetup, 1600);
            return () => window.clearTimeout(t);
        }
    }, [phase, progress?.state, finishSetup]);

    useEffect(() => () => progressUnsub.current?.(), []);

    const resetSetup = useCallback(() => {
        search.disconnect();
        search.storage.clearDraft();
        setConfig(defaultConfig({ type: "git", url: "", branch: "main" }));
        setStep(1);
        wizardToken.current = generateToken();
        setTokenInput(wizardToken.current);
        setConn({ kind: "idle" });
        setProgress(null);
        setPhase("wizard");
    }, [search]);

    return {
        phase,
        setPhase,
        step,
        goStep,
        config,
        update,
        token: wizardToken.current,
        backendUrl,
        setBackendUrl,
        tokenInput,
        setTokenInput,
        conn,
        setConn,
        progress,
        startIndexing,
        finishSetup,
        resetSetup,
        canReset: setup !== "never" && !pinnedBackend,
    };
}
