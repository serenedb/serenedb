import React from "react";
import type { SereneSearchConfig, StepStatus, SyncProgress } from "@serenedb/docs-search-core";
import { fmt } from "../lib/format";
import { summarizeSource } from "./summary";

export interface IndexingProps {
    progress: SyncProgress | null;
    config: SereneSearchConfig | null;
}

const BAR_CELLS = 17;

/** The design's terminal-style indexing progress. */
export function Indexing({ progress, config }: IndexingProps): React.ReactElement {
    const p = progress;
    const steps = p?.steps;
    const hybrid = config?.search.type === "hybrid";

    const embedBar = () => {
        const e = steps?.embed;
        if (!e || e.status === "pending") return "…";
        if (e.status === "skipped") return "skipped";
        const total = e.total ?? 0;
        const done = e.done ?? 0;
        const pct = total > 0 ? Math.round((done / total) * 100) : e.status === "done" ? 100 : 0;
        const filled = Math.round((pct / 100) * BAR_CELLS);
        return `[${"█".repeat(filled).padEnd(BAR_CELLS, "░")}] ${pct}%`;
    };

    return (
        <div>
            <div className="sds-ix">
                <div>
                    <span className="sds-ix-dollar">$</span> serene-search sync{" "}
                    {p?.incremental ? "" : "--initial"}
                </div>
                <Line
                    on={vis(steps?.fetch.status)}
                    label={`→ fetch    ${p?.source ?? (config ? summarizeSource(config) : "…")}`}
                    meta={steps?.fetch.files != null ? `${fmt(steps.fetch.files)} files` : "…"}
                    status={steps?.fetch.status}
                />
                <Line
                    on={vis(steps?.parse.status)}
                    label={`→ parse    ${(config?.content.extensions ?? []).join(" ")} → sections`}
                    meta={steps?.parse.sections != null ? `${fmt(steps.parse.sections)} sections` : "…"}
                    status={steps?.parse.status}
                />
                {hybrid && (
                    <Line
                        on={vis(steps?.embed.status)}
                        label={`→ embed    ${config?.ai?.embeddings?.model ?? "embeddings"}`}
                        meta={embedBar()}
                        metaClass="v"
                        status={steps?.embed.status}
                    />
                )}
                <Line
                    on={vis(steps?.index.status)}
                    label={`→ index    ${hybrid ? "fulltext + vector (hnsw)" : "fulltext (bm25)"}`}
                    meta={steps?.index.status === "done" ? "ok" : "…"}
                    status={steps?.index.status}
                />
                {p?.state === "done" && (
                    <div className="sds-ix-line">
                        <span className="sds-ix-good">
                            ✓ ready    {fmt(p.sections ?? 0)} sections · {fmt(p.documents ?? 0)} documents
                        </span>
                        <span className="sds-ix-meta ok">{Math.round((p.tookMs ?? 0) / 1000)} s</span>
                    </div>
                )}
                {p?.state === "error" && (
                    <div className="sds-ix-line">
                        <span className="sds-ix-err">✕ failed   {p.error}</span>
                    </div>
                )}
            </div>
            <div className="sds-hint" style={{ marginTop: 10 }}>
                &gt; initial build only — later syncs are incremental
                {config?.sync.snapshots !== false ? " (snapshots on)" : ""}
            </div>
        </div>
    );
}

function vis(status: StepStatus | undefined): number {
    if (!status || status === "pending") return 0.25;
    return 1;
}

function Line({
    on,
    label,
    meta,
    metaClass,
    status,
}: {
    on: number;
    label: string;
    meta: string;
    metaClass?: string;
    status?: StepStatus;
}): React.ReactElement {
    return (
        <div className="sds-ix-line" style={{ opacity: on }}>
            <span>{label}</span>
            <span className={`sds-ix-meta ${status === "error" ? "" : (metaClass ?? "")}`}>
                {status === "error" ? "✕" : meta}
            </span>
        </div>
    );
}
