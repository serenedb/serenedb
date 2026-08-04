import React from "react";
import { CheckLine, Field } from "../../components/primitives";
import type { StepProps } from "../types";

export function StepSource({ config, update }: StepProps): React.ReactElement {
    const src = config.source;
    const pick = (type: "git" | "folder" | "site" | "bucket") =>
        update((c) => {
            if (c.source.type === type) return;
            c.source =
                type === "git"
                    ? { type, url: "", branch: "main" }
                    : type === "folder"
                      ? { type, path: "./docs" }
                      : type === "site"
                        ? { type, url: "", depth: 2, sitemap: true }
                        : { type, uri: "" };
            c.sync.mode = type === "git" ? "commits" : c.sync.mode === "commits" ? "poll" : c.sync.mode;
        });

    const card = (
        type: "git" | "folder" | "site" | "bucket",
        badge: string,
        name: string,
        desc: string,
    ) => (
        <button
            type="button"
            className={`sds-card-btn${src.type === type ? " on" : ""}`}
            onClick={() => pick(type)}
        >
            <span className="sds-card-badge">{badge}</span>
            <span className="sds-card-text">
                <span className="sds-card-name">{name}</span>
                <span className="sds-card-desc">{desc}</span>
            </span>
        </button>
    );

    return (
        <div>
            <div className="sds-step-kicker">01 · Source</div>
            <h2 className="sds-step-title">Where do your docs live?</h2>
            <p className="sds-step-sub">
                Pick a source. The sync backend pulls from it — your files never leave your infra.
            </p>
            <div className="sds-cards">
                {card("git", "git", "Git repository", "Clone a repo at a branch or pinned commit.")}
                {card("folder", "fs", "Local folder", "A path mounted into the sync container.")}
                {card("site", "http", "Website", "Crawl a live site and index its HTML.")}
                {card("bucket", "s3", "Object storage", "S3-compatible bucket: AWS, R2, MinIO.")}
            </div>

            {src.type === "git" && (
                <div className="sds-mt sds-stack">
                    <div className="sds-row">
                        <Field label="Repository URL" className="sds-grow-2">
                            <input
                                className="sds-input"
                                value={src.url}
                                placeholder="https://github.com/acme/docs"
                                onChange={(e) => update((c) => ((c.source as typeof src).url = e.target.value))}
                            />
                        </Field>
                        <Field label="Branch" className="sds-grow-1">
                            <input
                                className="sds-input"
                                value={src.branch ?? ""}
                                placeholder="main"
                                onChange={(e) =>
                                    update((c) => ((c.source as typeof src).branch = e.target.value || undefined))
                                }
                            />
                        </Field>
                        <Field label="Pin commit" className="sds-grow-1">
                            <input
                                className="sds-input"
                                value={src.commit ?? ""}
                                placeholder="optional"
                                onChange={(e) =>
                                    update((c) => ((c.source as typeof src).commit = e.target.value || undefined))
                                }
                            />
                        </Field>
                    </div>
                    <Field label="Subdirectories / files — index only parts of the repo, comma-separated">
                        <input
                            className="sds-input"
                            value={Array.isArray(src.subdir) ? src.subdir.join(", ") : (src.subdir ?? "")}
                            placeholder="docs/, guides/faq.md (optional)"
                            onChange={(e) =>
                                update((c) => ((c.source as typeof src).subdir = e.target.value || undefined))
                            }
                        />
                    </Field>
                    <div className="sds-hint">
                        &gt; private repo — add a read-only deploy key to the backend env
                    </div>
                </div>
            )}

            {src.type === "folder" && (
                <div className="sds-mt">
                    <Field label="Path">
                        <input
                            className="sds-input"
                            value={src.path}
                            placeholder="./docs"
                            onChange={(e) => update((c) => ((c.source as typeof src).path = e.target.value))}
                        />
                    </Field>
                    <div className="sds-hint" style={{ marginTop: 8 }}>
                        &gt; mounted read-only into the sync container
                    </div>
                </div>
            )}

            {src.type === "site" && (
                <div className="sds-mt sds-stack">
                    <div className="sds-row" style={{ alignItems: "flex-end" }}>
                        <Field label="Start URL" className="sds-grow-3">
                            <input
                                className="sds-input"
                                value={src.url}
                                placeholder="https://docs.acme.dev"
                                onChange={(e) => update((c) => ((c.source as typeof src).url = e.target.value))}
                            />
                        </Field>
                        <Field label="Max depth" className="sds-grow-1">
                            <select
                                className="sds-select"
                                value={String(src.depth ?? 2)}
                                onChange={(e) =>
                                    update(
                                        (c) =>
                                            ((c.source as typeof src).depth =
                                                e.target.value === "all" ? "all" : Number(e.target.value)),
                                    )
                                }
                            >
                                <option value="1">1</option>
                                <option value="2">2</option>
                                <option value="3">3</option>
                                <option value="all">no limit</option>
                            </select>
                        </Field>
                    </div>
                    <CheckLine
                        on={src.sitemap !== false}
                        onToggle={() =>
                            update((c) => ((c.source as typeof src).sitemap = !(src.sitemap !== false)))
                        }
                    >
                        follow sitemap.xml
                    </CheckLine>
                </div>
            )}

            {src.type === "bucket" && (
                <div className="sds-mt sds-stack">
                    <Field label="Bucket URI">
                        <input
                            className="sds-input"
                            value={src.uri}
                            placeholder="s3://acme-docs/guides"
                            onChange={(e) => update((c) => ((c.source as typeof src).uri = e.target.value))}
                        />
                    </Field>
                    <div className="sds-row">
                        <Field label="Endpoint — R2 / MinIO" className="sds-grow-2">
                            <input
                                className="sds-input"
                                value={src.endpoint ?? ""}
                                placeholder="https://<account>.r2.cloudflarestorage.com (optional)"
                                onChange={(e) =>
                                    update((c) => ((c.source as typeof src).endpoint = e.target.value || undefined))
                                }
                            />
                        </Field>
                        <Field label="Region" className="sds-grow-1">
                            <input
                                className="sds-input"
                                value={src.region ?? ""}
                                placeholder="us-east-1"
                                onChange={(e) =>
                                    update((c) => ((c.source as typeof src).region = e.target.value || undefined))
                                }
                            />
                        </Field>
                    </div>
                    <div className="sds-hint">
                        &gt; credentials come from the backend env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), never the browser
                    </div>
                </div>
            )}
        </div>
    );
}
