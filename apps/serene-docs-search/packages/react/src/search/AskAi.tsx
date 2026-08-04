import React, { useEffect, useRef, useState } from "react";
import type { AskSource } from "@serenedb/docs-search-core";
import { MiniMarkdown } from "../components/MiniMarkdown";
import { Logo, UserIcon } from "../components/primitives";
import { useSpinner } from "../hooks/useSpinner";
import type { AskTurn, SereneDocsSearch } from "../hooks/useSereneDocsSearch";
import { fmt } from "../lib/format";

export interface AskAiProps {
    search: SereneDocsSearch;
    onOpenSource: (source: AskSource) => void;
}

/** Multi-turn Ask AI chat: question/answer thread + follow-up input. */
export function AskAi({
    search: s,
    onOpenSource,
}: AskAiProps): React.ReactElement {
    const turns = s.conversation;
    const last = turns[turns.length - 1];
    const busy =
        last != null &&
        (last.phase === "thinking" || last.phase === "streaming");
    const spin = useSpinner(busy);
    const [followUp, setFollowUp] = useState("");

    // keep the thread pinned to the bottom while streaming — unless the
    // user scrolled up to read something
    const scrollRef = useRef<HTMLDivElement>(null);
    const stickRef = useRef(true);
    useEffect(() => {
        const el = scrollRef.current;
        if (el && stickRef.current) el.scrollTop = el.scrollHeight;
    }, [turns]);

    const send = () => {
        const q = followUp.trim();
        if (!q || busy) return;
        stickRef.current = true;
        s.ask(q);
        setFollowUp("");
    };

    if (turns.length === 0) {
        return (
            <div className="sds-ai-scroll">
                <div className="sds-unconfigured" style={{ padding: "24px" }}>
                    <div>type a question above and press ↵</div>
                    <div>answers cite the indexed docs — nothing else</div>
                </div>
            </div>
        );
    }

    return (
        <div className="sds-ai-wrap">
            <div
                className="sds-ai-scroll sds-ai-chat"
                ref={scrollRef}
                onScroll={(e) => {
                    const el = e.currentTarget;
                    stickRef.current =
                        el.scrollHeight - el.scrollTop - el.clientHeight < 80;
                }}>
                {turns.map((t, i) => (
                    <Turn
                        key={i}
                        turn={t}
                        spin={spin}
                        search={s}
                        onOpenSource={onOpenSource}
                    />
                ))}
            </div>
            <div className="sds-ai-followup">
                <span className="sds-ai-followup-gt">&gt;</span>
                <input
                    value={followUp}
                    placeholder={busy ? "answering…" : "Ask a follow-up…"}
                    spellCheck={false}
                    onChange={(e) => setFollowUp(e.target.value)}
                    onKeyDown={(e) => {
                        if (e.key === "Enter") {
                            e.preventDefault();
                            send();
                        } else if (e.key === "Escape") {
                            s.setOpen(false);
                        }
                    }}
                />
            </div>
        </div>
    );
}

const SOURCES_PREVIEW = 3;

const STEP_LABELS: Record<string, string> = {
    search_docs: "searching",
    read_section: "reading",
    think: "thinking",
    plan: "planning",
};

function StepList({ steps }: { steps: AskTurn["steps"] }): React.ReactElement {
    return (
        <div className="sds-ai-steps">
            {steps.map((step, j) => (
                <div key={j} className="sds-ai-step">
                    → {STEP_LABELS[step.name] ?? step.name}
                    {step.detail ? `: ${step.detail}` : ""}
                </div>
            ))}
        </div>
    );
}

function Turn({
    turn: t,
    spin,
    search: s,
    onOpenSource,
}: {
    turn: AskTurn;
    spin: string;
    search: SereneDocsSearch;
    onOpenSource: (source: AskSource) => void;
}): React.ReactElement {
    const [allSources, setAllSources] = useState(false);
    const [trailOpen, setTrailOpen] = useState(false);
    const visibleSources = allSources
        ? t.sources
        : t.sources.slice(0, SOURCES_PREVIEW);

    const openCitation = (n: number) => {
        const src = t.sources.find((x) => x.n === n);
        if (src) onOpenSource(src);
    };

    return (
        <div className="sds-ai-turn">
            <div className="sds-ai-msg sds-ai-msg-user">
                <span className="sds-ai-avatar">
                    <UserIcon />
                </span>
                <div className="sds-ai-q-text">{t.question}</div>
            </div>

            <div className="sds-ai-msg">
                <span className="sds-ai-avatar sds-ai-avatar-planet">
                    <Logo />
                </span>
                <div className="sds-ai-msg-body">
                    {t.phase === "thinking" && (
                        <div className="sds-ai-thinking">
                            <div className="lead">
                                {spin} AI{" "}
                                {t.steps.length ? "RESEARCHING" : "THINKING"} —{" "}
                                {s.health
                                    ? `${fmt(s.health.index.sections)} sections indexed`
                                    : "searching the index"}
                            </div>
                            {t.steps.length > 0 ? (
                                <StepList steps={t.steps} />
                            ) : (
                                <div
                                    className="done-line"
                                    style={{ opacity: 0.25 }}>
                                    → searching the docs
                                </div>
                            )}
                        </div>
                    )}

                    {(t.phase === "streaming" || t.phase === "done") &&
                        t.steps.length > 0 && (
                            <div className="sds-ai-answer-label">
                                Here&rsquo;s what I found
                            </div>
                        )}

                    {t.phase !== "thinking" && t.steps.length > 0 && (
                        <div className="sds-ai-trail">
                            <button
                                type="button"
                                className="sds-ai-trail-toggle"
                                onClick={() => setTrailOpen((v) => !v)}>
                                {trailOpen ? "▾" : "▸"} researched —{" "}
                                {t.steps.length}{" "}
                                {t.steps.length === 1 ? "step" : "steps"}
                            </button>
                            {trailOpen && <StepList steps={t.steps} />}
                        </div>
                    )}

                    {t.phase === "error" && (
                        <div className="sds-ai-error">⚠ {t.error}</div>
                    )}

                    {(t.phase === "streaming" || t.phase === "done") && (
                        <>
                            <div className="sds-ai-prose">
                                <MiniMarkdown
                                    text={t.answer}
                                    onCitation={openCitation}
                                />
                                {t.phase === "streaming" && (
                                    <span className="sds-ai-cursor" />
                                )}
                            </div>

                            {t.phase === "done" && t.sources.length > 0 && (
                                <>
                                    <div className="sds-ai-sources-label">
                                        Sources
                                        <span className="sds-ai-sources-count">
                                            {" "}
                                            · {t.sources.length}
                                        </span>
                                    </div>
                                    <div className="sds-ai-sources">
                                        {visibleSources.map((src) => (
                                            <button
                                                key={src.n}
                                                type="button"
                                                className="sds-ai-source"
                                                onClick={() =>
                                                    onOpenSource(src)
                                                }>
                                                <i>[{src.n}]</i> {src.path}
                                                <span className="arrow">
                                                    ↗
                                                </span>
                                            </button>
                                        ))}
                                        {t.sources.length > SOURCES_PREVIEW && (
                                            <button
                                                type="button"
                                                className="sds-ai-sources-more"
                                                onClick={() =>
                                                    setAllSources((v) => !v)
                                                }>
                                                {allSources
                                                    ? "show less ↑"
                                                    : `view all (${t.sources.length}) ↓`}
                                            </button>
                                        )}
                                    </div>
                                </>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}
