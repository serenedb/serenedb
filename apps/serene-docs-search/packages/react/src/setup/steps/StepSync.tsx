import React from "react";
import { CheckLine } from "../../components/primitives";
import { summarizeInclude, summarizeSearch, summarizeSource, summarizeSync } from "../summary";
import type { StepProps } from "../types";

export function StepSync({ config, update }: StepProps): React.ReactElement {
    const sy = config.sync;
    const isGit = config.source.type === "git";

    const syncCard = (
        mode: "commits" | "poll" | "webhook",
        name: string,
        desc: string,
        extra?: React.ReactNode,
    ) => (
        <button
            type="button"
            className={`sds-card-btn${sy.mode === mode ? " on" : ""}`}
            onClick={() => update((c) => (c.sync.mode = mode))}
            disabled={mode === "commits" && !isGit}
            style={mode === "commits" && !isGit ? { opacity: 0.45, cursor: "not-allowed" } : undefined}
        >
            <span className="sds-card-dot">{sy.mode === mode ? "●" : "○"}</span>
            <span className="sds-card-text">
                <span className="sds-card-name">{name}</span>
                <span className="sds-card-desc">{desc}</span>
            </span>
            {extra}
        </button>
    );

    return (
        <div>
            <div className="sds-step-kicker">04 · Sync</div>
            <h2 className="sds-step-title">When should the index refresh?</h2>
            <p className="sds-step-sub">The backend re-pulls the source and updates SereneDB in place.</p>

            <div className="sds-cards-col">
                {syncCard(
                    "commits",
                    "Git commits",
                    isGit
                        ? "Watch the branch — re-index only what a commit touched."
                        : "Available for git sources.",
                )}
                {syncCard(
                    "poll",
                    "Poll interval",
                    "Re-pull the source on a schedule.",
                    <select
                        className="sds-select sds-select-sm"
                        value={sy.interval ?? "1h"}
                        onClick={(e) => e.stopPropagation()}
                        onChange={(e) =>
                            update((c) => {
                                c.sync.mode = "poll";
                                c.sync.interval = e.target.value;
                            })
                        }
                    >
                        <option value="15m">every 15 min</option>
                        <option value="1h">every hour</option>
                        <option value="6h">every 6 h</option>
                        <option value="24h">daily</option>
                    </select>,
                )}
                {syncCard("webhook", "Webhook", "POST /v1/reindex from CI after a deploy.")}
            </div>

            <div className="sds-mt-s">
                <CheckLine
                    on={sy.snapshots !== false}
                    onToggle={() => update((c) => (c.sync.snapshots = !(sy.snapshots !== false)))}
                >
                    content snapshots — hash files, skip unchanged, prune deleted
                </CheckLine>
            </div>

            <div className="sds-summary sds-mt">
                <div className="sds-field-label" style={{ marginBottom: 6 }}>
                    Config preview
                </div>
                <div className="sds-summary-grid">
                    <div>
                        <i>source</i>
                        {summarizeSource(config)}
                    </div>
                    <div>
                        <i>include</i>
                        {summarizeInclude(config)}
                    </div>
                    <div>
                        <i>search</i>
                        {summarizeSearch(config)}
                    </div>
                    <div>
                        <i>sync</i>
                        {summarizeSync(config)}
                    </div>
                </div>
            </div>
        </div>
    );
}
