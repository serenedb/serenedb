import { BUNDLED_OLLAMA_URL, CONFIG_FILENAME, DEFAULT_BACKEND_PORT } from "./defaults";
import type { AiProvider, SereneSearchConfig } from "./types";

export interface ComposeOptions {
    config: SereneSearchConfig;
    /** SERENE_SEARCH_TOKEN baked into the backend env. */
    token: string;
    serenedbImage?: string;
    backendImage?: string;
    mcpImage?: string;
}

const DEFAULT_MCP_PORT = 7710;

/**
 * Renders the docker-compose.yml shown on the wizard's deploy screen.
 * Two containers: SereneDB and the sync backend. The widget only ever
 * talks to the backend.
 */
export function generateCompose(opts: ComposeOptions): string {
    const { config, token } = opts;
    const port = config.server?.port ?? DEFAULT_BACKEND_PORT;
    const serenedbImage = opts.serenedbImage ?? "serenedb/serenedb:latest";
    const backendImage = opts.backendImage ?? "serenedb/docs-search-backend:latest";
    const mcpImage = opts.mcpImage ?? "serenedb/docs-search-mcp:latest";
    const providers = [config.ai?.answers, config.ai?.embeddings].filter(
        (p): p is AiProvider => Boolean(p),
    );
    const bundleOllama = providers.some(usesBundledOllama);

    const lines: string[] = [];
    lines.push("services:");
    lines.push("  serenedb:");
    lines.push(`    image: ${serenedbImage}`);
    lines.push("    volumes: [serene-data:/var/lib/serenedb]");
    lines.push("");
    if (bundleOllama) {
        lines.push("  ollama:");
        lines.push("    image: ollama/ollama:latest");
        lines.push("    volumes: [ollama-data:/root/.ollama]");
        lines.push("");
    }
    lines.push("  search-backend:");
    lines.push(`    image: ${backendImage}`);
    lines.push(`    ports: ["${port}:${port}"]`);
    lines.push("    environment:");
    lines.push(`      SERENE_SEARCH_TOKEN: ${token}  # admin auth (setup & manual sync)`);
    for (const env of new Set(
        providers
            .map((p) => p.apiKey)
            .filter((k): k is string => Boolean(k?.startsWith("${")))
            .map((k) => k.slice(2, -1)),
    )) {
        lines.push(`      ${env}: \${${env}}  # forwarded from your shell / .env`);
    }
    lines.push("    volumes:");
    lines.push(`      - ./${CONFIG_FILENAME}:/etc/serene/config.json:ro`);
    if (config.source.type === "folder") {
        lines.push(`      - ${config.source.path}:/data/docs:ro  # your local docs`);
    }
    lines.push(`    depends_on: [serenedb${bundleOllama ? ", ollama" : ""}]`);
    lines.push("");
    if (config.mcp?.enabled) {
        lines.push("  # MCP server — lets AI agents (Claude Code, Cursor…) search these docs");
        lines.push("  docs-search-mcp:");
        lines.push(`    image: ${mcpImage}`);
        lines.push(`    command: ["--http", "${DEFAULT_MCP_PORT}", "--backend", "http://search-backend:${port}", "--token", "${token}"]`);
        lines.push(`    ports: ["${DEFAULT_MCP_PORT}:${DEFAULT_MCP_PORT}"]`);
        lines.push("    depends_on: [search-backend]");
        lines.push("");
    }
    lines.push("volumes:");
    lines.push("  serene-data: {}");
    if (bundleOllama) lines.push("  ollama-data: {}");
    return lines.join("\n") + "\n";
}

/** True when the provider points at the compose-bundled Ollama container. */
export function usesBundledOllama(p: AiProvider): boolean {
    if (p.kind !== "ollama") return false;
    const base = p.baseUrl ?? BUNDLED_OLLAMA_URL;
    try {
        return new URL(base).hostname === "ollama";
    } catch {
        return false;
    }
}

/**
 * The config as downloaded from the deploy screen. Local folder sources
 * are rewritten to the in-container mount point the compose file sets up.
 */
export function configForDownload(config: SereneSearchConfig): string {
    const copy: SereneSearchConfig = JSON.parse(JSON.stringify(config));
    if (copy.source.type === "folder") {
        copy.source.path = "/data/docs";
    }
    return JSON.stringify(copy, null, 2) + "\n";
}
