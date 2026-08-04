import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
    READ_SECTION_DESCRIPTION,
    SEARCH_DOCS_DESCRIPTION,
    SereneSearchClient,
} from "@serenedb/docs-search-core";
import { formatHits, formatSection } from "./format";

export interface McpOptions {
    backendUrl: string;
    token?: string;
    /** Docs site origin used to absolutize result urls, e.g. https://serenedb.com */
    siteUrl?: string;
}

const text = (t: string) => ({ content: [{ type: "text" as const, text: t }] });

/**
 * The docs-search MCP server: the same search the docs widget uses, exposed
 * as tools for any MCP client (Claude Code, Cursor, custom agents, …).
 */
export function buildServer(opts: McpOptions): McpServer {
    const client = new SereneSearchClient({
        backendUrl: opts.backendUrl,
        token: opts.token,
    });

    // learned lazily from /v1/health so hybrid installs get semantic search
    let searchMode: "fulltext" | "hybrid" | undefined;
    const mode = async (): Promise<"fulltext" | "hybrid"> => {
        if (!searchMode) {
            try {
                searchMode = (await client.health()).searchType;
            } catch {
                searchMode = "fulltext";
            }
        }
        return searchMode;
    };

    const server = new McpServer({
        name: "serene-docs-search",
        version: "0.9.0",
    });

    server.tool(
        "search_docs",
        `${SEARCH_DOCS_DESCRIPTION} Follow up with read_section for a hit's full text.`,
        {
            query: z
                .string()
                .describe(
                    "Search query — keywords, an identifier or a question",
                ),
            limit: z
                .number()
                .int()
                .min(1)
                .max(10)
                .optional()
                .describe("Max results (default 5)"),
        },
        async ({ query, limit }) => {
            const res = await client.search(query, {
                mode: await mode(),
                limit: limit ?? 5,
            });
            return text(formatHits(res.results, opts.siteUrl));
        },
    );

    server.tool(
        "read_section",
        `${READ_SECTION_DESCRIPTION} Give the url returned from search_docs.`,
        {
            url: z
                .string()
                .describe("Section url exactly as returned by search_docs"),
        },
        async ({ url }) => {
            // current indexes store absolute urls — try the url as given
            // first; older indexes store site-relative ones, so fall back to
            // the origin-stripped form
            const rel =
                opts.siteUrl && url.startsWith(opts.siteUrl.replace(/\/+$/, ""))
                    ? url.slice(opts.siteUrl.replace(/\/+$/, "").length)
                    : url.replace(/^https?:\/\/[^/]+/, "");
            const section =
                (await client.section(url)) ??
                (rel && rel !== url ? await client.section(rel) : null);
            if (!section) return text(`No section found for url: ${url}`);
            return text(formatSection(section, opts.siteUrl));
        },
    );

    server.tool(
        "docs_health",
        "Check the documentation search backend: connectivity, index size and search type.",
        {},
        async () => {
            const h = await client.health();
            return text(
                `backend: ${opts.backendUrl}\n` +
                    `serenedb connected: ${h.serenedb.connected}\n` +
                    `sections indexed: ${h.index.sections}\n` +
                    `search type: ${h.searchType}\nai answers: ${h.features.ai}`,
            );
        },
    );

    return server;
}
