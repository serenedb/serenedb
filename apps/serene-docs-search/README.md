<picture align=left>
    <source media="(prefers-color-scheme: dark)" width="592" srcset="https://github.com/user-attachments/assets/7bcd81a5-df10-4bd3-be01-d9eaa9fd7eff">
    <source media="(prefers-color-scheme: light)" width="592" srcset="https://github.com/user-attachments/assets/3e416648-011b-44a6-b3cc-fad0ebbbbd2f">
    <img alt="serenedb+sereneui" width="592" src="https://github.com/user-attachments/assets/7bcd81a5-df10-4bd3-be01-d9eaa9fd7eff" />
</picture>

[![Star Us](https://img.shields.io/badge/Star%20Us-9865e8?style=for-the-badge&logo=github&logoColor=white)](https://github.com/serenedb/serenedb)
[![Apache License 2.0](https://img.shields.io/badge/License-Apache%202.0-a2b9f4?style=for-the-badge)](LICENSE)
[![Website](https://img.shields.io/website?up_message=VISIT&down_message=FIXING&color=fbe5f5&url=https%3A%2F%2Fwww.serenedb.com&style=for-the-badge)](https://www.serenedb.com)

</div>

[![Watch the video](https://github.com/user-attachments/assets/2ea82d69-7e58-4214-8d05-21146bff38fd)](https://youtu.be/hpxR1v2DXUA)

DocSearch-style documentation search, self-hosted on [SereneDB](https://serenedb.com).
One widget, one backend container, one database -- full-text (BM25), hybrid semantic
search and streamed "Ask AI" answers with citations, over your own docs.

```
Git / folder / website / S3  ── pulls ──────────┐
                                                ▼
┌────────────┐   /v1/search /v1/ask   ┌──────────────────┐                    ┌──────────┐
│   widget   │ ─────────────────────► │  search-backend  │ ─────────────────► │ SereneDB │
│ (React/JS) │                        │  (sync + API)    │   inverted index   └──────────┘
└────────────┘                        └──────────────────┘   bm25 + hnsw
```

## Packages

| Package | What it is |
| --- | --- |
| `@serenedb/docs-search-react` | `<SereneDocsSearch />` modal + `useSereneDocsSearch()` headless hook |
| `@serenedb/docs-search-embed` | The same widget as a single `<script>` tag (React bundled in) |
| `@serenedb/docs-search-backend` | Sync + search server (Express, `pg`), ships as a Docker image |
| `@serenedb/docs-search-core` | Shared types, API client, config & compose generators |
| `@serenedb/docs-search-mcp` | MCP server (stdio/HTTP) -- lets AI agents search & read your docs |

## Quick start (React)

```bash
npm install @serenedb/docs-search-react@latest
```

```tsx
import { SereneDocsSearch } from "@serenedb/docs-search-react";
import "@serenedb/docs-search-react/styles.css";

// renders the "> Search docs... ⌘K" trigger + the modal
<SereneDocsSearch backendUrl="https://search.your-domain.dev" />
```

Don't have a backend yet? Render `<SereneDocsSearch />` with **no** `backendUrl` and
open it: the **first-run wizard** walks through source -> content -> search type -> sync,
generates a `docker-compose.yml` + `serene-search.config.json`, tests the connection,
triggers the initial index build and drops you into working search. Once the stack
runs, hardcode `backendUrl` in props so end users never see the wizard.

### Props (main ones)

| Prop | Default | Meaning |
| --- | --- | --- |
| `backendUrl` | -- | Search backend. Omit to allow the first-run wizard |
| `token` | -- | Admin token; only needed for setup/manual sync, never ship it to end users |
| `hotkey` | `"mod+k"` | Toggle shortcut, `false` to disable |
| `theme` | `"auto"` | `light` / `dark` / follow the host page (`data-theme`, `.dark`, media query) |
| `navigate` | `location.assign` | SPA router hook, e.g. `(url) => history.push(url)` |
| `suggestions` | `[]` | Queries shown in the empty state |
| `sections` | backend config | Ordered result groups; overrides `search.sections` advertised by the backend |
| `contextUrl` | `window.location.href` | URL used to decide which section is current (mainly useful for SSR/tests) |
| `mcp` | `{}` | MCP tab options: an explicit Streamable HTTP `endpoint` and optional `serverName`; `false` hides the tab |
| `open` / `onOpenChange` | -- | Controlled mode for your own trigger button |
| `trigger` | `true` | Render the built-in trigger button |
| `transformUrl`, `onSelect`, `limit`, `debounceMs`, `placeholder`, `storageKey`, `setup`, `container`, `zIndex` | | see `SereneDocsSearchProps` |

Result clicks navigate to the section anchor, flash the target heading and highlight
the query terms on the page (CSS Custom Highlight API, no DOM mutation).

### Headless hook

Everything the modal does, without our UI:

```tsx
import { useSereneDocsSearch } from "@serenedb/docs-search-react";

const s = useSereneDocsSearch({ backendUrl: "http://localhost:7700" });
// s.open / s.setOpen / s.toggle -- modal state + hotkey handling
// s.query / s.setQuery -- debounced two-phase search (instant fulltext, semantic merge)
// s.groups / s.results / s.semantic / s.tookMs -- grouped, RRF-fused results
// s.selectedIndex / s.moveSelection / s.select / s.onKeyDown -- keyboard navigation
// s.ask(q) / s.askState -- streamed AI answers with citations
// s.status / s.health -- online / offline / unconfigured + index stats
```

### Contextual result sections

Define optional sections either in `search.sections` in the backend config or
directly on the widget. Direct props win, so one backend can serve differently
grouped UIs. Rules are ordered: the first match owns a result. The section whose
rule matches the current browser location is shown first; the server's ranking
remains unchanged inside every section. Empty sections are omitted, unmatched
hits appear under **Other results**, and headings render only when the result set
actually contains at least two non-empty groups.

```tsx
<SereneDocsSearch
  backendUrl="https://search.example.com"
  sections={[
    {
      id: "docs",
      label: "Docs",
      match: { urls: ["https://docs.example.com/**"], paths: ["docs/**"] },
    },
    {
      id: "blog",
      label: "Blog",
      match: { urls: ["https://blog.example.com/**"], paths: ["blog/**"] },
    },
  ]}
/>
```

`urls` match final result URLs and the current page URL. `paths` match both the
final URL pathname and the indexed source path. `*` stays inside one path segment;
`**` crosses segments. Put a narrow section such as `/installation/**` before a
broad `https://docs.example.com/**` section.

## Quick start (script tag)

```html
<link rel="stylesheet" href="https://unpkg.com/@serenedb/docs-search-embed@latest/dist/serene-docs-search.css">
<script src="https://unpkg.com/@serenedb/docs-search-embed@latest/dist/serene-docs-search.js"></script>
<script>
  const search = SereneDocsSearch.init({
    container: "#search",              // where the trigger renders (optional)
    backendUrl: "http://localhost:7700",
  });
  // search.open() / search.close() / search.destroy()
</script>
```

Or zero-JS auto-init: `<script src="..." data-backend-url="http://localhost:7700" data-container="#search"></script>`.

## The backend

Two containers; the widget only ever talks to the backend:

```yaml
services:
  serenedb:
    image: serenedb/serenedb:latest
    volumes: [serene-data:/var/lib/serenedb]

  search-backend:
    image: serenedb/docs-search-backend:latest
    ports: ["7700:7700"]
    environment:
      SERENE_SEARCH_TOKEN: sk-local-...        # admin auth (setup & manual sync)
    volumes:
      - ./serene-search.config.json:/etc/serene/config.json:ro
    depends_on: [serenedb]

volumes:
  serene-data: {}
```

The wizard generates both files, or write `serene-search.config.json` by hand:

```jsonc
{
  "version": 1,
  "source": { "type": "git", "url": "https://github.com/acme/docs", "branch": "main" },
  // or { "type": "folder", "path": "/data/docs" }
  // or { "type": "site", "url": "https://docs.acme.dev", "depth": 2, "sitemap": true }
  // or { "type": "bucket", "uri": "s3://acme-docs/guides" }
  "content": {
    "extensions": [".md", ".mdx"],          // also .html .rst .txt .ipynb .pdf
    "markdown": { "mode": "split" },        // one section per heading, h1-h4 by default
                                            // ("depth": 3 for coarser sections; h4 keeps
                                            //  API-reference functions findable by name)
    "html": { "selectors": "article, main", "tags": ["h1","h2","h3","p","li","pre","code"] },
    "exclude": ["**/node_modules/**"],
    "urlMapping": {
      "stripExtensions": true,
      "rules": [
        { "match": "docs/**", "baseUrl": "https://docs.example.com", "stripPrefix": "docs/" },
        { "match": "blog/**", "baseUrl": "https://blog.example.com", "stripPrefix": "blog/" }
      ]
    }
  },
  "search": {
    "type": "hybrid",                       // or "fulltext" (no embeddings needed)
    "sections": [
      { "id": "docs", "label": "Docs", "match": { "paths": ["docs/**"] } },
      { "id": "blog", "label": "Blog", "match": { "paths": ["blog/**"] } }
    ]
  },
  "ai": {
    "enabled": true,                        // adds the "Ask AI" tab
    "answers": {
      "kind": "openai",                    // any OpenAI-compatible endpoint
      "baseUrl": "https://api.openai.com/v1",
      "apiKey": "${OPENAI_API_KEY}",        // literal or env expansion
      "model": "gpt-4o-mini"
    },
    "embeddings": {
      "kind": "openai",
      "baseUrl": "https://api.openai.com/v1",
      "apiKey": "${OPENAI_API_KEY}",
      "model": "text-embedding-3-small"
    }
  },
  "sync": { "mode": "commits", "interval": "1h", "snapshots": true }
}
```

- **Sync modes**: `commits` (watch the git branch, cheap `ls-remote`), `poll`
  (re-pull on interval), `webhook` (`POST /v1/reindex` from CI). `snapshots`
  hashes every section -- unchanged rows are skipped, deleted ones pruned.
- **Search**: one SereneDB inverted index covers BM25 full-text (title boosted)
  and the `ivf` vector column; hybrid results are RRF-fused, and hits that only
  the vector pass surfaced are badged "AI suggested" in the widget. Queries are
  search-as-you-type: all terms ANDed, the trailing term matches as a prefix.
- **Relevance**: the analyzer stems ("run" finds "running"), folds accents and
  drops stopwords on both the indexed text and the query; `search.synonyms`
  takes a solr-format map ("db, database" / "k8s => kubernetes"). When the
  strict query finds nothing, a typo-tolerant pass reruns it with
  Damerau-Levenshtein matching, and the response carries a "did you mean"
  correction computed inside SereneDB against a corpus vocabulary table
  (similarity scored by the engine, corpus frequency breaks ties). Every hit
  ships a snippet -- a fragment of the section body with the matched words
  `<mark>`ed.
- **Ask AI**: an agentic chat over the same index -- the model drives retrieval
  through `search_docs` / `read_section` tool calls (a first search is always
  seeded), streams the answer with `[n]` citations and keeps multi-turn
  history. Providers without tool support fall back to single-shot RAG.
- **Auth**: `SERENE_SEARCH_TOKEN` guards admin endpoints (`PUT /v1/config`,
  `POST /v1/sync`, `POST /v1/reindex`). Search/ask/health are public -- end-user
  embeds never need the token.

### HTTP API

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/health` | connectivity, index stats, features |
| `POST /v1/search` | `{ q, mode?: "fulltext" \| "hybrid", limit? }` |
| `POST /v1/ask` | SSE stream: (`tool` \| `sources` \| `delta`)* -> `done`; `{ q, history? }` |
| `GET /v1/section?url=...` | full text of one indexed section |
| `POST /v1/sync` · `POST /v1/reindex` | trigger a sync (admin) |
| `GET /v1/sync/progress` | JSON snapshot, `?stream=1` for SSE |
| `GET/PUT /v1/config` | read (redacted) / replace config (admin) |

## MCP server

`@serenedb/docs-search-mcp` exposes the indexed docs to any MCP client
(Codex, Claude Code, Cursor, custom agents) as three tools: `search_docs`,
`read_section` and `docs_health`. It talks to the backend over its public
HTTP API, so it works against any deployed instance. When **MCP** is active, a
compact Codex / Claude selector appears at the right of the modal's main tab
row and the panel shows one copyable direct-HTTP CLI command at a time. Set the
public Streamable HTTP endpoint explicitly:

```tsx
<SereneDocsSearch
  backendUrl="https://search.example.com"
  mcp={{
    endpoint: "https://mcp.example.com/mcp",
    serverName: "product-docs",
  }}
/>
```

`endpoint` is required before the tab can generate a command; the widget never
guesses an MCP URL from `backendUrl` and never generates a local package-wrapper
command. `serverName` defaults to `serene-docs`. Pass `mcp={false}` to hide the
tab. The optional generated MCP compose service remains independent from the
widget and Ask AI.

## Development

```bash
npm install
npm run build          # all five packages
npm test               # unit tests: parsers, ranking, client, react hook
npm run eval           # relevance eval against a running backend (opt-in test suite)
```

### Relevance evaluation

`packages/backend/test/relevance.cases.json` holds ~60 graded queries over the SereneDB
docs corpus (exact titles, prefixes, typos, SQL keywords, code identifiers,
phrases, semantic paraphrases, partial matches). `npm run eval` runs
`test/relevance.test.ts` against a live backend: every graded query is a test
asserting its page lands in the top 3, and an aggregate test gates hit@1 >= 75%
and MRR@10 >= 0.85. It needs a running stack, so plain `npm test` skips it;
control it with `EVAL_BACKEND` (default `http://localhost:7700`), `EVAL_MODE`
(`hybrid` | `fulltext`) and `EVAL_CAT` (one category). Reference numbers
(hybrid, nomic embeddings): **hit@3 100%, hit@1 81%, MRR 0.898**; fusion
defaults (vectorWeight 0.7, k 60, window 50) were confirmed optimal by sweep --
they are tunable per install via `search.rrf` if your corpus behaves
differently.

Backend image: `docker build -f packages/backend/Dockerfile -t serenedb/docs-search-backend:latest .`
