/**
 * The docs-tool contract, shared by the in-process Ask AI agent (backend) and
 * the MCP server so their tool descriptions and result formatting never drift.
 * The actual retrieval lives once in the backend (repositories); these are the
 * LLM-facing shapes both consumers render.
 */

/** A search hit as fed to an LLM (search_docs output). */
export interface DocsHit {
    title: string;
    crumb: string;
    url: string;
    snippet: string;
}

export const SEARCH_DOCS_DESCRIPTION =
    "Search the product documentation. Returns numbered sections [n] with title, url and a snippet.";
export const READ_SECTION_DESCRIPTION =
    "Read the full documentation page containing a section found earlier — includes all subsections with their code examples.";

export const SEARCH_DOCS_QUERY_HINT =
    "Keyword query, 2-6 words naming a concrete feature or concept (no pronouns)";

/**
 * OpenAI function-calling tool definitions for the agentic loop. read_section
 * addresses a hit by the source number [n] the agent assigned during search.
 */
export const SEARCH_DOCS_TOOL = {
    type: "function",
    function: {
        name: "search_docs",
        description: SEARCH_DOCS_DESCRIPTION,
        parameters: {
            type: "object",
            properties: { query: { type: "string", description: SEARCH_DOCS_QUERY_HINT } },
            required: ["query"],
        },
    },
} as const;

export const READ_SECTION_TOOL = {
    type: "function",
    function: {
        name: "read_section",
        description: `${READ_SECTION_DESCRIPTION} Reference it by its source number n.`,
        parameters: {
            type: "object",
            properties: { n: { type: "integer", description: "source number from search_docs" } },
            required: ["n"],
        },
    },
} as const;

export const DOCS_AGENT_TOOLS = [SEARCH_DOCS_TOOL, READ_SECTION_TOOL];

/** One search hit rendered for an LLM, prefixed with its citation number. */
export function formatDocsHit(n: number, hit: DocsHit): string {
    const crumb = hit.crumb ? ` — ${hit.crumb}` : "";
    const snippet = hit.snippet ? `\n${hit.snippet}` : "";
    return `[${n}] ${hit.title}${crumb}\nURL: ${hit.url}${snippet}`;
}

/** A run of hits as one block, numbered from `start` (default 1). */
export function formatDocsHits(hits: DocsHit[], start = 1): string {
    if (hits.length === 0) return "No results.";
    return hits.map((h, i) => formatDocsHit(start + i, h)).join("\n\n");
}

/** A full section/page under a label line ("[3] Hybrid Search" or a title). */
export function formatDocsSection(label: string, text: string): string {
    return `${label}\n${text}`;
}
