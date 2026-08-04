import React from "react";
import type { SearchResultItem } from "@serenedb/docs-search-core";
import { Button } from "../components/primitives";
import { useSpinner } from "../hooks/useSpinner";
import type { SereneDocsSearch } from "../hooks/useSereneDocsSearch";
import { fmt } from "../lib/format";

export interface SearchViewProps {
    search: SereneDocsSearch;
    onAskInstead: () => void;
    /** Wipe the saved connection + wizard draft and re-run first-run setup. */
    onResetSetup?: () => void;
}

export function SearchView({ search, onAskInstead, onResetSetup }: SearchViewProps): React.ReactElement {
    const s = search;
    const spin = useSpinner(s.phase === "searching" || s.semanticPending);
    const offline = s.status === "offline";
    const showStatus = s.query.trim().length > 0 && s.phase !== "idle";
    const listRef = useMergeAnimation(s.results, s.query);
    const resultIndexes = new Map(s.results.map((item, index) => [item.id, index]));

    const renderHit = (item: SearchResultItem) => {
        const index = resultIndexes.get(item.id) ?? 0;
        return (
            <Hit
                key={item.id}
                item={item}
                query={s.query}
                active={index === s.selectedIndex}
                onHover={() => s.setSelectedIndex(index)}
                onClick={() => s.select(item)}
            />
        );
    };

    return (
        <div style={{ display: "flex", flexDirection: "column", minHeight: 0 }}>
            {showStatus && (
                <div className="sds-statusbar">
                    {s.semanticPending && s.phase === "searching" && s.results.length === 0 ? (
                        <span className="thinking">
                            {spin} SEARCHING — fulltext pass over the index
                        </span>
                    ) : s.semanticPending ? (
                        <span className="thinking">
                            {spin} AI THINKING — semantic pass
                            {s.health ? ` over ${fmt(s.health.index.sections)} sections` : ""}
                        </span>
                    ) : (
                        <span>
                            {s.total} results ·{" "}
                            {s.fuzzy
                                ? "fuzzy — typo-tolerant match"
                                : s.partial
                                  ? "partial — no doc matches every term"
                                  : s.semantic
                                    ? "hybrid — fulltext + vector, re-ranked"
                                    : s.health?.searchType === "hybrid"
                                      ? "fulltext — semantic pass pending"
                                      : "fulltext — bm25"}
                        </span>
                    )}
                    <span>
                        {s.tookMs} ms
                        {s.semantic && s.semanticTookMs != null ? ` + ${s.semanticTookMs} ms semantic` : ""}
                    </span>
                </div>
            )}

            <div className="sds-results">
                {offline && (
                    <div className="sds-offline">
                        <div className="sds-offline-msg">
                            ⚠ backend unreachable{s.backendUrl ? ` — ${s.backendUrl}` : ""}
                        </div>
                        <div className="sds-offline-side">
                            <span className="sds-offline-note">
                                {s.results.length > 0 ? "showing last results · " : ""}retry in 5 s
                            </span>
                            <button
                                type="button"
                                className="sds-offline-retry"
                                onClick={() => void s.refreshHealth()}
                            >
                                retry now ↺
                            </button>
                            {onResetSetup && (
                                <button
                                    type="button"
                                    className="sds-offline-retry"
                                    title="Forget this backend and run first-run setup again"
                                    onClick={onResetSetup}
                                >
                                    reset setup ⟲
                                </button>
                            )}
                        </div>
                    </div>
                )}

                {s.status === "unconfigured" && (
                    <div className="sds-unconfigured">
                        <div>search backend is not configured</div>
                        <div>pass backendUrl to the widget, or run the setup wizard</div>
                        {onResetSetup && (
                            <div style={{ marginTop: 10 }}>
                                <button
                                    type="button"
                                    className="sds-offline-retry"
                                    onClick={onResetSetup}
                                >
                                    run setup wizard →
                                </button>
                            </div>
                        )}
                    </div>
                )}

                {s.phase === "idle" && s.status !== "unconfigured" && (
                    <EmptyState search={s} />
                )}

                {s.correctedQuery && (s.phase === "done" || s.phase === "no-results") && (
                    <div className="sds-didyoumean">
                        did you mean{" "}
                        <button type="button" onClick={() => s.setQuery(s.correctedQuery!)}>
                            {s.correctedQuery}
                        </button>
                        ?
                    </div>
                )}

                {(s.phase === "done" || (s.phase === "searching" && s.results.length > 0)) && (
                    <div className="sds-hits" ref={listRef}>
                        {s.sectioned
                            ? s.groups.map((group) => (
                                  <React.Fragment key={group.id ?? group.label}>
                                      <div
                                          className={`sds-group-label${group.active ? " active" : ""}`}
                                          data-section-id={group.id}
                                          role="heading"
                                          aria-level={2}
                                      >
                                          {group.label}
                                      </div>
                                      {group.items.map(renderHit)}
                                  </React.Fragment>
                              ))
                            : s.results.map(renderHit)}
                    </div>
                )}

                {s.phase === "no-results" && (
                    <div className="sds-nores">
                        <div className="sds-nores-q">
                            0 results for <b>“{s.query}”</b>
                        </div>
                        <div className="sds-nores-tips">
                            <div>— check spelling or try a shorter query</div>
                            {s.semantic && <div>— semantic pass found nothing similar either</div>}
                        </div>
                        {s.aiEnabled && (
                            <div className="sds-nores-cta">
                                <Button variant="secondary" size="sm" onClick={onAskInstead}>
                                    Ask AI instead →
                                </Button>
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}

function EmptyState({ search: s }: { search: SereneDocsSearch }): React.ReactElement {
    return (
        <div>
            {s.recent.length > 0 && (
                <>
                    <div className="sds-empty-label">Recent</div>
                    {s.recent.map((q) => (
                        <div key={q} className="sds-empty-row">
                            <button
                                type="button"
                                className="sds-empty-item"
                                onClick={() => s.setQuery(q)}
                            >
                                <i>↺</i>
                                <span>{q}</span>
                            </button>
                            <button
                                type="button"
                                className="sds-empty-x"
                                title="Remove from recent"
                                aria-label={`Remove "${q}" from recent searches`}
                                onClick={(e) => {
                                    e.stopPropagation();
                                    s.removeRecent(q);
                                }}
                            >
                                ×
                            </button>
                        </div>
                    ))}
                </>
            )}
            {s.suggestions.length > 0 && (
                <>
                    <div className="sds-empty-label">Suggested</div>
                    {s.suggestions.map((q) => (
                        <button
                            key={q}
                            type="button"
                            className="sds-empty-item"
                            onClick={() => s.setQuery(q)}
                        >
                            <i>→</i>
                            <span>{q}</span>
                        </button>
                    ))}
                </>
            )}
            {s.recent.length === 0 && s.suggestions.length === 0 && (
                <div className="sds-unconfigured" style={{ padding: "32px 24px" }}>
                    <div>
                        type to search
                        {s.health ? ` ${fmt(s.health.index.sections)} sections` : ""} — fulltext
                        {s.health?.searchType === "hybrid" ? " + semantic" : ""}
                    </div>
                </div>
            )}
        </div>
    );
}

const KIND_GLYPH: Record<SearchResultItem["kind"], string> = {
    heading: "#",
    text: "¶",
    code: "</>",
};

function Hit({
    item,
    query,
    active,
    onHover,
    onClick,
}: {
    item: SearchResultItem;
    query: string;
    active: boolean;
    onHover: () => void;
    onClick: () => void;
}): React.ReactElement {
    const ref = React.useRef<HTMLButtonElement>(null);
    React.useEffect(() => {
        if (active) ref.current?.scrollIntoView({ block: "nearest" });
    }, [active]);
    return (
        <button
            ref={ref}
            type="button"
            className={`sds-hit${active ? " on" : ""}`}
            data-hit-id={item.id}
            data-ai={item.aiSuggested ? "1" : undefined}
            onMouseMove={onHover}
            onClick={onClick}
        >
            <span className="sds-hit-glyph">{KIND_GLYPH[item.kind] ?? "¶"}</span>
            <span className="sds-hit-text">
                <span className="sds-hit-title">{highlight(item.title, query)}</span>
                {item.snippet && (
                    <span className="sds-hit-snippet">{renderSnippet(item.snippet)}</span>
                )}
                <span className="sds-hit-crumb">{item.crumb}</span>
            </span>
            <span className="sds-hit-meta">
                {active ? (
                    "↵"
                ) : item.pinned ? (
                    <span className="sds-hit-badge">⚑ pinned</span>
                ) : item.aiSuggested ? (
                    <span className="sds-hit-badge">✦ AI suggested</span>
                ) : (
                    ""
                )}
            </span>
        </button>
    );
}

/** Backend snippets wrap matches in <mark>…</mark>; render only that markup. */
function renderSnippet(snippet: string): React.ReactNode {
    const parts = snippet.split(/<mark>(.*?)<\/mark>/g);
    return parts.map((part, i) =>
        i % 2 === 1 ? <mark key={i}>{part}</mark> : <React.Fragment key={i}>{part}</React.Fragment>,
    );
}

/**
 * FLIP "merge-in" animation for the second (semantic) pass: when the hybrid
 * response lands on the same query, rows that were already on screen slide to
 * their new fused positions and rows the vector pass added materialize with a
 * purple flash. New queries repaint instantly — the effect only plays when
 * results are being *merged*, not replaced.
 */
function useMergeAnimation(
    results: SearchResultItem[],
    query: string,
): React.RefObject<HTMLDivElement | null> {
    const listRef = React.useRef<HTMLDivElement>(null);
    const prevTops = React.useRef(new Map<string, number>());
    const prevQuery = React.useRef("");

    React.useLayoutEffect(() => {
        const container = listRef.current;
        const sameQuery = prevQuery.current === query;
        prevQuery.current = query;

        const next = new Map<string, number>();
        if (!container) {
            prevTops.current = next;
            return;
        }
        const rows = container.querySelectorAll<HTMLElement>("[data-hit-id]");
        for (const el of rows) next.set(el.dataset.hitId!, el.getBoundingClientRect().top);

        const reduced = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
        if (sameQuery && !reduced && prevTops.current.size > 0 && "animate" in Element.prototype) {
            let entered = 0;
            for (const el of rows) {
                const id = el.dataset.hitId!;
                const before = prevTops.current.get(id);
                const after = next.get(id)!;
                if (before != null) {
                    const dy = before - after;
                    if (Math.abs(dy) > 1) {
                        el.animate(
                            [{ transform: `translateY(${dy}px)` }, { transform: "translateY(0)" }],
                            { duration: 320, easing: "cubic-bezier(0.22, 0.7, 0.3, 1)" },
                        );
                    }
                } else {
                    const delay = Math.min(entered++ * 45, 270);
                    el.animate(
                        [
                            { opacity: 0, transform: "translateY(7px) scale(0.985)" },
                            { opacity: 1, transform: "none" },
                        ],
                        { duration: 340, delay, easing: "cubic-bezier(0.2, 0.8, 0.3, 1)", fill: "backwards" },
                    );
                    el.animate(
                        [
                            { backgroundColor: "color-mix(in srgb, var(--sds-thirdly) 16%, transparent)" },
                            { backgroundColor: "transparent" },
                        ],
                        { duration: 1100, delay, easing: "ease-out", fill: "backwards" },
                    );
                }
            }
        }
        prevTops.current = next;
    }, [results, query]);

    return listRef;
}

/** Wrap query-token matches in <mark>, like the design's title highlighting. */
function highlight(title: string, query: string): React.ReactNode {
    const tokens = [
        ...new Set(
            query
                .toLowerCase()
                .split(/[^\p{L}\p{N}_]+/u)
                .filter((t) => t.length >= 2),
        ),
    ];
    if (tokens.length === 0) return title;
    const pattern = tokens.map(escapeRe).join("|");
    const parts = title.split(new RegExp(`(${pattern})`, "iu"));
    return parts.map((part, i) =>
        i % 2 === 1 ? <mark key={i}>{part}</mark> : <React.Fragment key={i}>{part}</React.Fragment>,
    );
}

function escapeRe(s: string): string {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
