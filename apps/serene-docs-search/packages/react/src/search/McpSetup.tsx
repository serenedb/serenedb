import React, { useMemo, useState } from "react";

/** Consumer-facing options for the Search modal's MCP setup tab. */
export interface SereneDocsSearchMcpOptions {
    /** Name written to Codex / Claude MCP configuration. Default: serene-docs. */
    serverName?: string;
    /** Public Streamable HTTP MCP endpoint, for example https://mcp.example.com/mcp. */
    endpoint?: string;
}

export interface McpClientSetup {
    command: string;
}

export type McpClient = "codex" | "claude";

export interface McpSetupInstructions {
    serverName: string;
    connection: "endpoint";
    codex: McpClientSetup;
    claude: McpClientSetup;
}

/** Pure instruction generator, exported for hosts that want their own MCP UI. */
export function createMcpSetupInstructions(
    options: SereneDocsSearchMcpOptions = {},
): McpSetupInstructions | null {
    const endpoint = options.endpoint?.trim();
    if (!endpoint) return null;

    const serverName = normalizeServerName(options.serverName);
    return {
        serverName,
        connection: "endpoint",
        codex: {
            command: `codex mcp add ${serverName} --url ${shellQuote(endpoint)}`,
        },
        claude: {
            command: `claude mcp add --transport http ${serverName} ${shellQuote(endpoint)}`,
        },
    };
}

export function McpSetup({
    options = {},
    client,
}: {
    options?: SereneDocsSearchMcpOptions;
    client: McpClient;
}): React.ReactElement {
    const instructions = useMemo(
        () => createMcpSetupInstructions(options),
        [options],
    );
    const [copied, setCopied] = useState<string | null>(null);
    const [copyError, setCopyError] = useState(false);

    const copy = async (key: string, value: string) => {
        try {
            await navigator.clipboard.writeText(value);
            setCopyError(false);
            setCopied(key);
            window.setTimeout(() => setCopied((current) => (current === key ? null : current)), 1600);
        } catch {
            setCopied(null);
            setCopyError(true);
        }
    };

    if (!instructions) {
        return (
            <div className="sds-mcp-scroll" role="tabpanel" aria-label="MCP setup">
                <div className="sds-mcp-empty">
                    <h2>Configure an MCP endpoint</h2>
                    <p>
                        Pass the public Streamable HTTP URL as <code>mcp.endpoint</code>.
                    </p>
                </div>
            </div>
        );
    }

    const selected = instructions[client];
    const clientLabel = client === "codex" ? "Codex" : "Claude";

    return (
        <div className="sds-mcp-scroll" role="tabpanel" aria-label="MCP setup">
            <h2 className="sds-mcp-title">Use this documentation in your coding agent.</h2>
            <p className="sds-mcp-lead">Choose your client, copy the command.</p>

            <div
                className="sds-mcp-command"
                role="region"
                aria-label={`${clientLabel} command`}
                aria-live="polite"
            >
                <div className="sds-mcp-command-head">
                    <span>CLI COMMAND</span>
                    <button
                        type="button"
                        onClick={() => copy(`${client}-command`, selected.command)}
                        aria-label={`Copy ${clientLabel} command`}
                    >
                        {copied === `${client}-command` ? "copied ✓" : "copy"}
                    </button>
                </div>
                <pre>{selected.command}</pre>
            </div>
            {copyError && (
                <div className="sds-mcp-copy-error" role="status">
                    Copy failed — select the command manually.
                </div>
            )}
        </div>
    );
}

function normalizeServerName(value?: string): string {
    const normalized = (value ?? "serene-docs")
        .trim()
        .replace(/[^A-Za-z0-9_-]+/g, "-")
        .replace(/^-+|-+$/g, "");
    return normalized || "serene-docs";
}

function shellQuote(value: string): string {
    if (/^[A-Za-z0-9_./:@%+=,-]+$/.test(value)) return value;
    return `'${value.replace(/'/g, `'"'"'`)}'`;
}
