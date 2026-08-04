import { useCallback, useRef, useState } from "react";
import type { AskMessage, AskSource, SereneSearchClient } from "@serenedb/docs-search-core";

/** One retrieval step the agent took while answering. */
export interface AskStep {
    name: string;
    detail?: string;
}

/** One question→answer exchange in the Ask AI conversation. */
export interface AskTurn {
    phase: "thinking" | "streaming" | "done" | "error";
    question: string;
    answer: string;
    sources: AskSource[];
    /** Tool calls the agent made (search_docs / read_section). */
    steps: AskStep[];
    model?: string;
    error?: string;
}

/** The latest turn, or an idle placeholder while the conversation is empty. */
export interface AskState {
    phase: "idle" | "thinking" | "streaming" | "done" | "error";
    question: string;
    answer: string;
    sources: AskSource[];
    model?: string;
    error?: string;
}

const IDLE_ASK: AskState = { phase: "idle", question: "", answer: "", sources: [] };

/** Multi-turn "Ask AI" chat over the backend's SSE endpoint. */
export function useAskAi(client: SereneSearchClient | null) {
    const [turns, setTurns] = useState<AskTurn[]>([]);
    const turnsRef = useRef<AskTurn[]>([]);
    const abortRef = useRef<AbortController | null>(null);

    const apply = (updater: (prev: AskTurn[]) => AskTurn[]) => {
        setTurns((prev) => {
            const next = updater(prev);
            turnsRef.current = next;
            return next;
        });
    };

    /** Ask `q` on top of `base` turns (base's finished exchanges become history). */
    const run = useCallback(
        (q: string, base: AskTurn[]) => {
            if (!client || !q.trim()) return;
            // one answer at a time — new questions are ignored while busy
            const current = turnsRef.current[turnsRef.current.length - 1];
            if (current && (current.phase === "thinking" || current.phase === "streaming")) return;
            abortRef.current?.abort();
            const ctrl = new AbortController();
            abortRef.current = ctrl;

            const history: AskMessage[] = base
                .filter((t) => t.phase === "done" && t.answer)
                .flatMap((t) => [
                    { role: "user" as const, content: t.question },
                    { role: "assistant" as const, content: t.answer },
                ]);
            apply(() => [
                ...base,
                { phase: "thinking", question: q, answer: "", sources: [], steps: [] },
            ]);
            const patchLast = (fn: (t: AskTurn) => AskTurn) =>
                apply((list) => list.map((t, i) => (i === list.length - 1 ? fn(t) : t)));

            void client
                .ask(
                    q,
                    (ev) => {
                        switch (ev.type) {
                            case "sources":
                                patchLast((t) => ({ ...t, sources: ev.sources }));
                                break;
                            case "tool":
                                patchLast((t) => ({
                                    ...t,
                                    steps: [...t.steps, { name: ev.name, detail: ev.detail }],
                                }));
                                break;
                            case "delta":
                                patchLast((t) => ({
                                    ...t,
                                    phase: "streaming",
                                    answer: t.answer + ev.text,
                                }));
                                break;
                            case "done":
                                patchLast((t) => ({ ...t, phase: "done", model: ev.model }));
                                break;
                            case "error":
                                patchLast((t) => ({ ...t, phase: "error", error: ev.message }));
                                break;
                        }
                    },
                    ctrl.signal,
                    history,
                )
                .then(() => {
                    patchLast((t) =>
                        t.phase === "streaming" || t.phase === "thinking"
                            ? { ...t, phase: "done" }
                            : t,
                    );
                })
                .catch((err: Error) => {
                    if (err.name === "AbortError") return;
                    patchLast((t) => ({ ...t, phase: "error", error: err.message }));
                });
        },
        [client],
    );

    const ask = useCallback((q: string) => run(q, turnsRef.current), [run]);

    /** Re-ask the last question, replacing its answer. */
    const regenerate = useCallback(() => {
        const last = turnsRef.current[turnsRef.current.length - 1];
        if (last) run(last.question, turnsRef.current.slice(0, -1));
    }, [run]);

    const resetAsk = useCallback(() => {
        abortRef.current?.abort();
        apply(() => []);
    }, []);

    const askState: AskState = turns[turns.length - 1] ?? IDLE_ASK;

    return { conversation: turns, askState, ask, regenerate, resetAsk };
}
