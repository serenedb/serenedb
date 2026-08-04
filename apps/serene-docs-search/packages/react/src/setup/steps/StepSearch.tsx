import React from "react";
import {
    DEFAULT_SYSTEM_PROMPT,
    defaultProvider,
    type SearchSectionConfig,
} from "@serenedb/docs-search-core";
import { CheckLine, Field } from "../../components/primitives";
import type { StepProps } from "../types";
import { ProviderFields } from "./ProviderFields";

export function StepSearch({ config, update }: StepProps): React.ReactElement {
    const st = config.search.type;
    const ai = config.ai ?? { enabled: false };
    const sections = config.search.sections ?? [];

    const addSection = () =>
        update((c) => {
            const list = (c.search.sections ??= []);
            let n = list.length + 1;
            while (list.some((section) => section.id === `section-${n}`)) n++;
            list.push({
                id: `section-${n}`,
                label: `Section ${n}`,
                match: { paths: [] },
            });
        });

    const patterns = (section: SearchSectionConfig, key: "urls" | "paths") =>
        section.match[key]?.join(", ") ?? "";

    const updatePatterns = (
        index: number,
        key: "urls" | "paths",
        value: string,
    ) =>
        update((c) => {
            const items = value
                .split(",")
                .map((item) => item.trim())
                .filter(Boolean);
            const match = c.search.sections![index].match;
            if (items.length) match[key] = items;
            else delete match[key];
        });

    const searchCard = (
        type: "fulltext" | "hybrid",
        name: React.ReactNode,
        desc: string,
    ) => (
        <button
            type="button"
            className={`sds-card-btn${st === type ? " on" : ""}`}
            onClick={() =>
                update((c) => {
                    c.search.type = type;
                    if (type === "hybrid") {
                        (c.ai ??= { enabled: false }).embeddings ??= defaultProvider(
                            "openai",
                            "embeddings",
                        );
                    }
                })
            }
        >
            <span className="sds-card-dot">{st === type ? "●" : "○"}</span>
            <span className="sds-card-text">
                <span className="sds-card-name">{name}</span>
                <span className="sds-card-desc">{desc}</span>
            </span>
        </button>
    );

    return (
        <div>
            <div className="sds-step-kicker">03 · Search</div>
            <h2 className="sds-step-title">How should search behave?</h2>
            <p className="sds-step-sub">Both run inside SereneDB — no external search service.</p>

            <div className="sds-cards-col">
                {searchCard("fulltext", "Full-text", "BM25 ranking. Instant, zero extra dependencies.")}
                {searchCard(
                    "hybrid",
                    <>
                        Hybrid<span className="sds-card-tag">RECOMMENDED</span>
                    </>,
                    "Full-text + vector similarity. Needs an embeddings model.",
                )}
            </div>
            {st === "hybrid" && (
                <div className="sds-hint" style={{ marginTop: 8 }}>
                    &gt; full-text hits render instantly — semantic matches merge in once embeddings resolve
                </div>
            )}

            {st === "hybrid" && (
                <div className="sds-panel sds-stack sds-mt-s" style={{ padding: 12 }}>
                    <div>
                        <div className="sds-panel-title">Embeddings provider</div>
                        <div className="sds-panel-sub">
                            Vectorizes sections at index time and queries at search time.
                        </div>
                    </div>
                    <ProviderFields
                        role="embeddings"
                        provider={ai.embeddings}
                        update={update}
                    />
                </div>
            )}

            <div className="sds-mt-s">
                <Field label="Synonyms — optional, solr format">
                    <textarea
                        className="sds-textarea"
                        rows={2}
                        placeholder={"db, database\nk8s => kubernetes"}
                        value={config.search.synonyms ?? ""}
                        onChange={(e) =>
                            update((c) => (c.search.synonyms = e.target.value || undefined))
                        }
                    />
                </Field>
                <div className="sds-hint" style={{ marginTop: 6 }}>
                    &gt; stemming and stopwords are on by default — "run" finds "running"
                </div>
            </div>

            <div className="sds-divider" />

            <div className="sds-panel sds-stack sds-section-config">
                <div className="sds-panel-row">
                    <div>
                        <div className="sds-panel-title">Result sections</div>
                        <div className="sds-panel-sub">
                            Group by URL/source path. Put specific paths before broad sites.
                        </div>
                    </div>
                    <button type="button" className="sds-section-add" onClick={addSection}>
                        + add section
                    </button>
                </div>

                {sections.length === 0 && (
                    <div className="sds-hint">
                        &gt; optional — without sections, the original relevance list is unchanged
                    </div>
                )}

                {sections.map((section, index) => (
                    <div className="sds-section-editor" key={`${section.id}-${index}`}>
                        <div className="sds-row sds-section-editor-head">
                            <Field label="Label" className="sds-grow-1">
                                <input
                                    className="sds-input"
                                    value={section.label}
                                    placeholder="Docs"
                                    onChange={(event) =>
                                        update(
                                            (c) =>
                                                (c.search.sections![index].label =
                                                    event.target.value),
                                        )
                                    }
                                />
                            </Field>
                            <Field label="ID" className="sds-grow-1">
                                <input
                                    className="sds-input"
                                    value={section.id}
                                    placeholder="docs"
                                    onChange={(event) =>
                                        update(
                                            (c) =>
                                                (c.search.sections![index].id =
                                                    event.target.value),
                                        )
                                    }
                                />
                            </Field>
                            <button
                                type="button"
                                className="sds-section-remove"
                                aria-label={`Remove ${section.label || "section"}`}
                                title="Remove section"
                                onClick={() =>
                                    update((c) => {
                                        c.search.sections!.splice(index, 1);
                                        if (c.search.sections!.length === 0) {
                                            delete c.search.sections;
                                        }
                                    })
                                }
                            >
                                ×
                            </button>
                        </div>
                        <Field label="URL globs — comma-separated">
                            <input
                                className="sds-input"
                                value={patterns(section, "urls")}
                                placeholder="https://docs.example.com/**"
                                onChange={(event) =>
                                    updatePatterns(index, "urls", event.target.value)
                                }
                            />
                        </Field>
                        <Field label="Path globs — URL pathname or indexed source path">
                            <input
                                className="sds-input"
                                value={patterns(section, "paths")}
                                placeholder="/installation/**, docs/installation/**"
                                onChange={(event) =>
                                    updatePatterns(index, "paths", event.target.value)
                                }
                            />
                        </Field>
                    </div>
                ))}

                {sections.length > 0 && (
                    <div className="sds-hint">
                        &gt; first match owns a result; the section matching the current page is
                        shown first
                    </div>
                )}
            </div>

            <div className="sds-divider" />

            <CheckLine
                on={ai.enabled}
                onToggle={() =>
                    update((c) => {
                        const cur = (c.ai ??= { enabled: false });
                        cur.enabled = !cur.enabled;
                        if (cur.enabled) {
                            cur.answers ??= defaultProvider("openai", "answers");
                            cur.systemPrompt ||= DEFAULT_SYSTEM_PROMPT;
                        }
                    })
                }
            >
                <span className="sds-card-name">AI answers</span>
                <span className="sds-card-desc">
                    Adds an “Ask AI” tab that answers from your indexed docs, with citations.
                </span>
            </CheckLine>

            {ai.enabled && (
                <div className="sds-panel sds-stack sds-mt-s" style={{ padding: 12 }}>
                    <div>
                        <div className="sds-panel-title">Answers provider</div>
                        <div className="sds-panel-sub">
                            Independent from embeddings — e.g. a hosted API for answers, Ollama for
                            embeddings, or two different Ollama servers.
                        </div>
                    </div>
                    <ProviderFields role="answers" provider={ai.answers} update={update} />
                    <Field label="System prompt">
                        <textarea
                            className="sds-textarea"
                            rows={2}
                            value={ai.systemPrompt ?? ""}
                            onChange={(e) =>
                                update((c) => ((c.ai ??= { enabled: false }).systemPrompt = e.target.value))
                            }
                        />
                    </Field>
                </div>
            )}

            <div className="sds-divider" />

            <CheckLine
                on={config.mcp?.enabled ?? false}
                onToggle={() =>
                    update((c) => ((c.mcp ??= {}).enabled = !(c.mcp?.enabled ?? false)))
                }
            >
                <span className="sds-card-name">MCP server</span>
                <span className="sds-card-desc">
                    Adds a container that lets AI agents (Claude Code, Cursor…) search these docs.
                    Publish its <code>/mcp</code> URL and pass that URL to the widget as
                    <code> mcp.endpoint</code>. The widget and backend work without it.
                </span>
            </CheckLine>
        </div>
    );
}
