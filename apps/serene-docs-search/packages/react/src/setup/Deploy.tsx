import React, { useState } from "react";
import {
    CONFIG_FILENAME,
    SereneSearchClient,
    configForDownload,
    generateCompose,
    type SereneSearchConfig,
} from "@serenedb/docs-search-core";
import { Button, TerminalCode } from "../components/primitives";
import { useSpinner } from "../hooks/useSpinner";
import { fmt } from "../lib/format";
import type { ConnState } from "./types";

export interface DeployProps {
    config: SereneSearchConfig;
    token: string;
    backendUrl: string;
    setBackendUrl: (v: string) => void;
    tokenInput: string;
    setTokenInput: (v: string) => void;
    conn: ConnState;
    setConn: (c: ConnState) => void;
    toast: (msg: string) => void;
}

export function Deploy(props: DeployProps): React.ReactElement {
    const { config, token, backendUrl, setBackendUrl, tokenInput, setTokenInput, conn, setConn, toast } = props;
    const [copied, setCopied] = useState(false);
    const spin = useSpinner(conn.kind === "testing");
    const compose = generateCompose({ config, token });

    const copyCompose = async () => {
        try {
            await navigator.clipboard.writeText(compose);
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1600);
        } catch {
            toast("copy failed — select the text manually");
        }
    };

    const downloadConfig = () => {
        const blob = new Blob([configForDownload(config)], { type: "application/json" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = CONFIG_FILENAME;
        a.click();
        URL.revokeObjectURL(a.href);
        toast(`↓ ${CONFIG_FILENAME} — generated from your setup`);
    };

    const test = async () => {
        if (conn.kind === "testing") return;
        setConn({ kind: "testing" });
        try {
            const client = new SereneSearchClient({ backendUrl, token: tokenInput || undefined });
            const h = await client.health();
            if (!h.serenedb.connected) {
                setConn({ kind: "fail", message: "backend up, SereneDB unreachable" });
            } else {
                setConn({ kind: "ok", version: h.serenedb.version, sections: h.index.sections });
            }
        } catch (err) {
            setConn({ kind: "fail", message: (err as Error).message });
        }
    };

    return (
        <div>
            <div className="sds-step-kicker">Deploy · Docker</div>
            <h2 className="sds-step-title">Run the stack</h2>
            <p className="sds-step-sub">
                Two containers: SereneDB and a thin sync backend. The widget talks to the backend only.
            </p>

            <div className="sds-terminal">
                <div className="sds-terminal-head">
                    <div className="sds-terminal-name">docker-compose.yml</div>
                    <div className="sds-header-spacer" />
                    <button type="button" className="sds-terminal-link" onClick={downloadConfig}>
                        ↓ {CONFIG_FILENAME}
                    </button>
                    <button type="button" className="sds-terminal-copy" onClick={copyCompose}>
                        {copied ? "copied ✓" : "copy"}
                    </button>
                </div>
                <TerminalCode text={compose.trimEnd()} />
            </div>

            <div style={{ marginTop: 16 }}>
                <div className="sds-field-label" style={{ marginBottom: 6 }}>
                    Connect the widget
                </div>
                <div className="sds-row">
                    <div className="sds-grow-2">
                        <input
                            className="sds-input"
                            value={backendUrl}
                            placeholder="http://localhost:7700"
                            onChange={(e) => setBackendUrl(e.target.value)}
                        />
                    </div>
                    <div className="sds-grow-1">
                        <input
                            className="sds-input"
                            value={tokenInput}
                            placeholder="SERENE_SEARCH_TOKEN"
                            onChange={(e) => setTokenInput(e.target.value)}
                        />
                    </div>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginTop: 10 }}>
                    <Button variant="secondary" size="sm" onClick={() => void test()}>
                        Test connection
                    </Button>
                    <div className={`sds-conn ${conn.kind}`}>
                        {conn.kind === "idle" && "○ not tested"}
                        {conn.kind === "testing" && `${spin} probing ${backendUrl} …`}
                        {conn.kind === "ok" &&
                            `● connected${conn.version ? ` · ${shortVersion(conn.version)}` : ""} · ${
                                conn.sections > 0 ? `${fmt(conn.sections)} sections indexed` : "index empty"
                            }`}
                        {conn.kind === "fail" && `✕ ${conn.message}`}
                    </div>
                </div>
            </div>
        </div>
    );
}

function shortVersion(v: string): string {
    const m = /serenedb[^\d]*([\d.]+)/i.exec(v);
    return m ? `serenedb ${m[1]}` : v.split(" ").slice(0, 2).join(" ").slice(0, 32);
}
