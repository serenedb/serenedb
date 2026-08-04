import React from "react";
import {
    KNOWN_EXTENSIONS,
    type SereneSearchConfig,
    type UrlMappingRule,
} from "@serenedb/docs-search-core";
import { Field } from "../../components/primitives";
import type { StepProps } from "../types";

const HTML_TAG_CHIPS: { key: string; label: string; tags: string[] }[] = [
    { key: "h", label: "h1–h4", tags: ["h1", "h2", "h3", "h4"] },
    { key: "p", label: "p", tags: ["p"] },
    { key: "li", label: "li", tags: ["li"] },
    { key: "code", label: "pre / code", tags: ["pre", "code"] },
    { key: "table", label: "table", tags: ["table"] },
];

export function StepContent({ config, update }: StepProps): React.ReactElement {
    const exts = config.content.extensions;
    // a crawled site is HTML by definition: no file-type chips, no markdown
    // splitting, no file->URL mapping — pages already carry their own URLs
    const isSite = config.source.type === "site";
    const toggleExt = (ext: string) =>
        update((c) => {
            const list = c.content.extensions;
            const i = list.indexOf(ext);
            if (i >= 0) list.splice(i, 1);
            else list.push(ext);
        });

    const tags = config.content.html?.tags ?? [];
    const tagOn = (chip: (typeof HTML_TAG_CHIPS)[number]) => chip.tags.every((t) => tags.includes(t));
    const toggleTags = (chip: (typeof HTML_TAG_CHIPS)[number]) =>
        update((c) => {
            const html = (c.content.html ??= {});
            const current = new Set(html.tags ?? []);
            const on = chip.tags.every((t) => current.has(t));
            for (const t of chip.tags) {
                if (on) current.delete(t);
                else current.add(t);
            }
            html.tags = [...current];
        });

    const mdOn = !isSite && (exts.includes(".md") || exts.includes(".mdx"));
    const mdSplit = (config.content.markdown?.mode ?? "split") === "split";
    const mdDepth = Math.min(Math.max(config.content.markdown?.depth ?? 4, 1), 6);
    const htmlOn = isSite || exts.includes(".html");
    const mappingRules = config.content.urlMapping?.rules ?? [];

    const addMappingRule = () =>
        update((c) => {
            ((c.content.urlMapping ??= {}).rules ??= []).push({ match: "" });
        });

    const updateMappingRule = (
        index: number,
        key: "match" | "baseUrl" | "stripPrefix",
        value: string,
    ) =>
        update((c) => {
            const rule = c.content.urlMapping!.rules![index];
            if (key === "match") rule.match = value;
            else rule[key] = value || undefined;
        });

    return (
        <div>
            <div className="sds-step-kicker">02 · Content</div>
            <h2 className="sds-step-title">What should be indexed?</h2>
            <p className="sds-step-sub">
                {isSite
                    ? "Crawled pages are HTML — scope what gets extracted and which paths to skip."
                    : "Only matched files are parsed and stored in SereneDB."}
            </p>

            {!isSite && (
                <>
                    <div className="sds-field-label" style={{ marginBottom: 6 }}>
                        File types
                    </div>
                    <div className="sds-chips">
                        {KNOWN_EXTENSIONS.map((ext) => (
                            <button
                                key={ext}
                                type="button"
                                className={`sds-chip${exts.includes(ext) ? " on" : ""}`}
                                onClick={() => toggleExt(ext)}
                            >
                                {ext}
                            </button>
                        ))}
                    </div>
                </>
            )}

            {mdOn && (
                <div className="sds-panel sds-stack sds-mt-s">
                    <div className="sds-panel-row">
                        <div style={{ minWidth: 0 }}>
                            <div className="sds-panel-title">.md / .mdx parsing</div>
                            <div className="sds-panel-sub">
                                Splitting by headings gives the deep-link anchors search jumps to.
                            </div>
                        </div>
                        <select
                            className="sds-select sds-select-sm"
                            value={config.content.markdown?.mode ?? "split"}
                            onChange={(e) =>
                                update(
                                    (c) =>
                                        ((c.content.markdown ??= { mode: "split" }).mode = e.target
                                            .value as "split" | "whole"),
                                )
                            }
                        >
                            <option value="split">split by headings</option>
                            <option value="whole">whole file</option>
                        </select>
                    </div>
                    {mdSplit && (
                        <div>
                            <div className="sds-field-label" style={{ marginBottom: 6 }}>
                                Headings that open a section — h1–h{mdDepth}
                            </div>
                            <div className="sds-chips">
                                {[1, 2, 3, 4, 5, 6].map((n) => (
                                    <button
                                        key={n}
                                        type="button"
                                        className={`sds-chip${n <= mdDepth ? " on" : ""}`}
                                        onClick={() =>
                                            update(
                                                (c) =>
                                                    ((c.content.markdown ??= { mode: "split" }).depth = n),
                                            )
                                        }
                                    >
                                        h{n}
                                    </button>
                                ))}
                            </div>
                            <div className="sds-hint" style={{ marginTop: 6 }}>
                                &gt; deeper headings stay inside their parent section — h4 catches
                                API-reference function docs
                            </div>
                        </div>
                    )}
                </div>
            )}

            {htmlOn && (
                <div className="sds-panel sds-stack sds-mt-s">
                    <div>
                        <div className="sds-panel-title">.html extraction</div>
                        <div className="sds-panel-sub">
                            Scope to content containers, then pick which tags become sections.
                        </div>
                    </div>
                    <Field label="Content selectors">
                        <input
                            className="sds-input"
                            value={config.content.html?.selectors ?? ""}
                            placeholder="article, main .content"
                            onChange={(e) =>
                                update((c) => ((c.content.html ??= {}).selectors = e.target.value))
                            }
                        />
                    </Field>
                    <Field label="Exclude selectors — dropped before extraction">
                        <input
                            className="sds-input"
                            value={config.content.html?.excludeSelectors ?? ""}
                            placeholder="pre.language-plaintext, .badge (optional)"
                            onChange={(e) =>
                                update(
                                    (c) =>
                                        ((c.content.html ??= {}).excludeSelectors =
                                            e.target.value || undefined),
                                )
                            }
                        />
                    </Field>
                    <div>
                        <div className="sds-field-label" style={{ marginBottom: 6 }}>
                            Extract tags
                        </div>
                        <div className="sds-chips">
                            {HTML_TAG_CHIPS.map((chip) => (
                                <button
                                    key={chip.key}
                                    type="button"
                                    className={`sds-chip${tagOn(chip) ? " on" : ""}`}
                                    onClick={() => toggleTags(chip)}
                                >
                                    {chip.label}
                                </button>
                            ))}
                        </div>
                    </div>
                </div>
            )}

            <div className="sds-mt-s">
                <Field label={isSite ? "Exclude paths" : "Exclude"}>
                    <input
                        className="sds-input"
                        value={(config.content.exclude ?? []).join(", ")}
                        placeholder={
                            isSite
                                ? "/blog/**, /changelog/**"
                                : "**/node_modules/**, **/CHANGELOG.md"
                        }
                        onChange={(e) =>
                            update(
                                (c) =>
                                    (c.content.exclude = e.target.value
                                        .split(",")
                                        .map((s) => s.trim())
                                        .filter(Boolean)),
                            )
                        }
                    />
                </Field>
            </div>

            {!isSite && (
                <div className="sds-mt-s">
                    <Field label="URL mapping — where these files are served">
                        <div className="sds-row">
                            <input
                                className="sds-input sds-grow-1"
                                value={config.content.urlMapping?.baseUrl ?? ""}
                                placeholder="base URL, e.g. /docs or https://docs.acme.dev"
                                onChange={(e) =>
                                    update(
                                        (c) =>
                                            ((c.content.urlMapping ??= {}).baseUrl =
                                                e.target.value || undefined),
                                    )
                                }
                            />
                            <input
                                className="sds-input sds-grow-1"
                                value={config.content.urlMapping?.stripPrefix ?? ""}
                                placeholder="strip path prefix, e.g. docs/"
                                onChange={(e) =>
                                    update(
                                        (c) =>
                                            ((c.content.urlMapping ??= {}).stripPrefix =
                                                e.target.value || undefined),
                                    )
                                }
                            />
                        </div>
                    </Field>
                    <div className="sds-hint" style={{ marginTop: 6 }}>
                        &gt; docs/quick-start.md → {previewUrl(config)}
                    </div>

                    <div className="sds-panel sds-stack sds-section-config sds-mt-s">
                        <div className="sds-panel-row">
                            <div>
                                <div className="sds-panel-title">Per-path public sites</div>
                                <div className="sds-panel-sub">
                                    Map one indexed corpus to separate domains. First matching glob wins.
                                </div>
                            </div>
                            <button
                                type="button"
                                className="sds-section-add"
                                onClick={addMappingRule}
                            >
                                + add mapping
                            </button>
                        </div>

                        {mappingRules.length === 0 && (
                            <div className="sds-hint">
                                &gt; example: blog/** → https://blog.example.com, then ** →
                                https://docs.example.com
                            </div>
                        )}

                        {mappingRules.map((rule, index) => (
                            <UrlMappingRuleEditor
                                key={`${rule.match}-${index}`}
                                index={index}
                                rule={rule}
                                updateRule={updateMappingRule}
                                remove={() =>
                                    update((c) => {
                                        c.content.urlMapping!.rules!.splice(index, 1);
                                        if (c.content.urlMapping!.rules!.length === 0) {
                                            delete c.content.urlMapping!.rules;
                                        }
                                    })
                                }
                            />
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}

function UrlMappingRuleEditor({
    index,
    rule,
    updateRule,
    remove,
}: {
    index: number;
    rule: UrlMappingRule;
    updateRule: (
        index: number,
        key: "match" | "baseUrl" | "stripPrefix",
        value: string,
    ) => void;
    remove: () => void;
}): React.ReactElement {
    return (
        <div className="sds-section-editor">
            <div className="sds-row sds-section-editor-head">
                <Field label="Source path glob" className="sds-grow-1">
                    <input
                        className="sds-input"
                        value={rule.match}
                        placeholder={index === 0 ? "blog/**" : "**"}
                        onChange={(event) => updateRule(index, "match", event.target.value)}
                    />
                </Field>
                <Field label="Public base URL" className="sds-grow-1">
                    <input
                        className="sds-input"
                        value={rule.baseUrl ?? ""}
                        placeholder={
                            index === 0
                                ? "https://blog.example.com"
                                : "https://docs.example.com"
                        }
                        onChange={(event) => updateRule(index, "baseUrl", event.target.value)}
                    />
                </Field>
                <button
                    type="button"
                    className="sds-section-remove"
                    aria-label={`Remove URL mapping ${index + 1}`}
                    title="Remove URL mapping"
                    onClick={remove}
                >
                    ×
                </button>
            </div>
            <Field label="Strip source prefix — optional">
                <input
                    className="sds-input"
                    value={rule.stripPrefix ?? ""}
                    placeholder={index === 0 ? "blog/" : ""}
                    onChange={(event) => updateRule(index, "stripPrefix", event.target.value)}
                />
            </Field>
        </div>
    );
}

function previewUrl(config: SereneSearchConfig): string {
    const m = config.content.urlMapping ?? {};
    let p = "docs/quick-start";
    const prefix = m.stripPrefix?.replace(/^\/+|\/+$/g, "");
    if (prefix && p.startsWith(prefix + "/")) p = p.slice(prefix.length + 1);
    const base = (m.baseUrl ?? "").replace(/\/+$/, "");
    if (!p) return base || "/";
    return `${base}/${p}`.replace(/\/{2,}/g, "/").replace(/^(https?:)\//, "$1//");
}
