import React, { useCallback, useEffect, useRef, useState } from "react";
import type { AskSource } from "@serenedb/docs-search-core";
import { Logo } from "../components/primitives";
import type { SereneDocsSearch } from "../hooks/useSereneDocsSearch";
import { AskAi } from "./AskAi";
import {
    McpSetup,
    type McpClient,
    type SereneDocsSearchMcpOptions,
} from "./McpSetup";
import { SearchView } from "./SearchView";

export interface SearchScreenProps {
    search: SereneDocsSearch;
    placeholder: string;
    /** Wipe the saved connection + wizard draft and re-run first-run setup. */
    onResetSetup?: () => void;
    /** False hides the MCP tab; options customize its generated setup. */
    mcp?: false | SereneDocsSearchMcpOptions;
}

/** The modal's main screen: query input, Search / Ask AI / MCP tabs and hints footer. */
export function SearchScreen({ search, placeholder, onResetSetup, mcp = {} }: SearchScreenProps): React.ReactElement {
    const [tab, setTab] = useState<"search" | "ai" | "mcp">("search");
    const [mcpClient, setMcpClient] = useState<McpClient>("codex");
    const aiAvailable = search.aiEnabled;
    const mcpAvailable = mcp !== false;
    useEffect(() => {
        if (!aiAvailable && tab === "ai") setTab("search");
        if (!mcpAvailable && tab === "mcp") setTab("search");
    }, [aiAvailable, mcpAvailable, tab]);

    /** Switch to the AI tab and ask explicitly (Enter / "Ask AI instead"). */
    const switchToAi = useCallback(
        (question?: string) => {
            const q = (question ?? search.query).trim();
            setTab("ai");
            if (q) search.ask(q); // the hook ignores it while an answer is running
        },
        [search],
    );

    const openSource = useCallback(
        (src: AskSource) => {
            search.select({
                id: src.id,
                url: src.url,
                path: src.path,
                title: src.title,
                anchor: src.url.split("#")[1],
                crumb: "",
                group: "",
                kind: "text",
            });
        },
        [search],
    );

    // autofocus the search input on open and on tab switch
    const inputRef = useRef<HTMLInputElement>(null);
    useEffect(() => {
        if (tab === "mcp") return;
        const t = window.setTimeout(() => inputRef.current?.focus(), 30);
        return () => window.clearTimeout(t);
    }, [tab]);

    return (
        <>
            <div className="sds-search-head">
                <span className="sds-search-gt">&gt;</span>
                <input
                    ref={inputRef}
                    className="sds-search-input"
                    value={search.query}
                    placeholder={placeholder}
                    spellCheck={false}
                    onChange={(e) => search.setQuery(e.target.value)}
                    onKeyDown={(e) => {
                        if (tab === "mcp") {
                            if (e.key === "Escape") search.setOpen(false);
                            return;
                        }
                        if (tab === "ai") {
                            if (e.key === "Enter") {
                                e.preventDefault();
                                switchToAi(search.query);
                            } else if (e.key === "Escape") {
                                search.setOpen(false);
                            }
                            return;
                        }
                        search.onKeyDown(e);
                    }}
                />
                {search.status === "offline" ? (
                    <span className="sds-search-count bad">○ offline</span>
                ) : (
                    <span className="sds-search-count">
                        <span className="good">●</span>{" "}
                        {search.health
                            ? `${search.health.index.sections.toLocaleString("en-US")} docs`
                            : "connecting…"}
                    </span>
                )}
                <button
                    type="button"
                    className="sds-esc"
                    onClick={() => search.setOpen(false)}
                >
                    esc
                </button>
            </div>

            {(aiAvailable || mcpAvailable) && (
                <div className="sds-tabs-row">
                    <div className="sds-tabs" role="tablist" aria-label="Search modes">
                        <button
                            type="button"
                            role="tab"
                            aria-selected={tab === "search"}
                            className={`sds-tab${tab === "search" ? " on" : ""}`}
                            onClick={() => setTab("search")}
                        >
                            SEARCH
                        </button>
                        {aiAvailable && (
                            <button
                                type="button"
                                role="tab"
                                aria-selected={tab === "ai"}
                                className={`sds-tab${tab === "ai" ? " on" : ""}`}
                                onClick={() => setTab("ai")}
                            >
                                ASK AI
                            </button>
                        )}
                        {mcpAvailable && (
                            <button
                                type="button"
                                role="tab"
                                aria-selected={tab === "mcp"}
                                className={`sds-tab${tab === "mcp" ? " on" : ""}`}
                                onClick={() => setTab("mcp")}
                            >
                                MCP
                            </button>
                        )}
                    </div>
                    {tab === "ai" && search.conversation.length > 0 && (
                        <button
                            type="button"
                            className="sds-tabs-action"
                            onClick={() => search.resetAsk()}
                        >
                            clear conversation ×
                        </button>
                    )}
                    {tab === "mcp" && (
                        <div
                            className="sds-tabs sds-mcp-client-tabs"
                            role="group"
                            aria-label="MCP client"
                        >
                            <button
                                type="button"
                                className={`sds-tab${mcpClient === "codex" ? " on" : ""}`}
                                aria-pressed={mcpClient === "codex"}
                                onClick={() => setMcpClient("codex")}
                            >
                                CODEX
                            </button>
                            <button
                                type="button"
                                className={`sds-tab${mcpClient === "claude" ? " on" : ""}`}
                                aria-pressed={mcpClient === "claude"}
                                onClick={() => setMcpClient("claude")}
                            >
                                CLAUDE
                            </button>
                        </div>
                    )}
                </div>
            )}

            {tab === "search" ? (
                <SearchView
                    search={search}
                    onAskInstead={() => switchToAi()}
                    onResetSetup={onResetSetup}
                />
            ) : tab === "ai" ? (
                <AskAi search={search} onOpenSource={openSource} />
            ) : (
                <McpSetup
                    options={mcp || {}}
                    client={mcpClient}
                />
            )}

            <div className="sds-hints">
                {tab !== "mcp" && (
                    <>
                        <span>
                            <span className="sds-kbd">↵</span> select
                        </span>
                        <span>
                            <span className="sds-kbd">↑↓</span> navigate
                        </span>
                    </>
                )}
                <span>
                    <span className="sds-kbd">esc</span> close
                </span>
                <div className="sds-hints-spacer" />
                <a
                    className="sds-brand"
                    href="https://serenedb.com"
                    target="_blank"
                    rel="noreferrer"
                >
                    search by <Logo /> SereneDB
                </a>
            </div>
        </>
    );
}
