import http from "node:http";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { buildServer, type McpOptions } from "./server";

const HELP = `serene-docs-mcp — MCP server for SereneDocsSearch

Usage:
  serene-docs-mcp --backend <url> [--token <token>] [--site-url <url>] [--http <port>]

Options:
  --backend   SereneDocsSearch backend url (or SERENE_SEARCH_BACKEND_URL).
              Default: http://localhost:7700
  --token     Admin token, only needed for private backends (or SERENE_SEARCH_TOKEN)
  --site-url  Docs site origin used to absolutize result urls,
              e.g. https://serenedb.com (or SERENE_SEARCH_SITE_URL)
  --http      Serve MCP over streamable HTTP on this port instead of stdio
  --help      Show this help

Examples:
  # start a package-installed local stdio server:
  serene-docs-mcp --backend https://search.example.com

  # remote HTTP endpoint:
  serene-docs-mcp --backend http://localhost:7700 --http 7710   # -> http://localhost:7710/mcp
`;

function parseArgs(argv: string[]): { opts: McpOptions; httpPort?: number } {
    const get = (flag: string): string | undefined => {
        const i = argv.indexOf(flag);
        return i >= 0 ? argv[i + 1] : undefined;
    };
    if (argv.includes("--help") || argv.includes("-h")) {
        process.stdout.write(HELP);
        process.exit(0);
    }
    const backendUrl =
        get("--backend") || process.env.SERENE_SEARCH_BACKEND_URL || "http://localhost:7700";
    const opts: McpOptions = {
        backendUrl,
        token: get("--token") || process.env.SERENE_SEARCH_TOKEN || undefined,
        siteUrl: get("--site-url") || process.env.SERENE_SEARCH_SITE_URL || undefined,
    };
    const httpFlag = get("--http");
    return { opts, httpPort: httpFlag ? Number(httpFlag) : undefined };
}

async function main(): Promise<void> {
    const { opts, httpPort } = parseArgs(process.argv.slice(2));

    if (httpPort == null) {
        // stdio: one long-lived server per client process
        const server = buildServer(opts);
        await server.connect(new StdioServerTransport());
        console.error(`serene-docs-mcp: stdio server up (backend ${opts.backendUrl})`);
        return;
    }

    if (!Number.isFinite(httpPort) || httpPort <= 0) {
        console.error("serene-docs-mcp: invalid --http port");
        process.exit(1);
    }

    // streamable HTTP, stateless: a fresh server+transport per request
    const httpServer = http.createServer((req, res) => {
        void (async () => {
            const url = new URL(req.url ?? "/", "http://localhost");
            if (url.pathname !== "/mcp") {
                res.writeHead(404, { "Content-Type": "application/json" });
                res.end(JSON.stringify({ error: "not found — MCP lives at /mcp" }));
                return;
            }
            if (req.method !== "POST") {
                res.writeHead(405, { "Content-Type": "application/json" });
                res.end(
                    JSON.stringify({
                        jsonrpc: "2.0",
                        error: { code: -32000, message: "Method not allowed" },
                        id: null,
                    }),
                );
                return;
            }
            const chunks: Buffer[] = [];
            for await (const chunk of req) chunks.push(chunk as Buffer);
            let body: unknown;
            try {
                body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "null");
            } catch {
                res.writeHead(400).end();
                return;
            }
            const server = buildServer(opts);
            const transport = new StreamableHTTPServerTransport({
                sessionIdGenerator: undefined,
            });
            res.on("close", () => {
                void transport.close();
                void server.close();
            });
            await server.connect(transport);
            await transport.handleRequest(req, res, body);
        })().catch((err) => {
            console.error("serene-docs-mcp:", (err as Error).message);
            if (!res.headersSent) res.writeHead(500);
            res.end();
        });
    });
    httpServer.listen(httpPort, () => {
        console.error(
            `serene-docs-mcp: http://localhost:${httpPort}/mcp (backend ${opts.backendUrl})`,
        );
    });
}

main().catch((err) => {
    console.error("serene-docs-mcp:", err);
    process.exit(1);
});
