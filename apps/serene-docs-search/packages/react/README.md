# @serenedb/docs-search-react

DocSearch-style documentation search modal + headless hook, powered by
[SereneDB](https://serenedb.com). Full-text (BM25), hybrid semantic search and
streamed "Ask AI" answers with citations -- over your own docs, self-hosted.

Part of [SereneDocsSearch](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search).

## Install

```bash
npm install @serenedb/docs-search-react
```

## Usage

```tsx
import { SereneDocsSearch } from "@serenedb/docs-search-react";
import "@serenedb/docs-search-react/styles.css";

// renders the "> Search docs... ⌘K" trigger + the modal
<SereneDocsSearch backendUrl="https://search.your-domain.dev" />
```

Don't have a backend yet? Render `<SereneDocsSearch />` with **no** `backendUrl`
and open it: the first-run wizard walks through source -> content -> search type ->
sync, generates a `docker-compose.yml` + `serene-search.config.json` and drops
you into working search.

### Headless hook

Everything the modal does, without the UI:

```tsx
import { useSereneDocsSearch } from "@serenedb/docs-search-react";

const s = useSereneDocsSearch({ backendUrl: "http://localhost:7700" });
// s.query / s.setQuery -- debounced two-phase search (instant fulltext, semantic merge)
// s.groups / s.results / s.semantic -- grouped, RRF-fused results
// s.ask(q) / s.askState -- streamed AI answers with citations
// s.selectedIndex / s.onKeyDown -- keyboard navigation
```

The full list of props (`hotkey`, `theme`, `navigate`, `sections`, `mcp`,
controlled mode, ...), backend setup and configuration reference live in the
[SereneDocsSearch README](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search#readme).

## Related packages

- [`@serenedb/docs-search-embed`](https://www.npmjs.com/package/@serenedb/docs-search-embed) -- the same widget as a single `<script>` tag
- [`@serenedb/docs-search-mcp`](https://www.npmjs.com/package/@serenedb/docs-search-mcp) -- MCP server for AI agents
- [`@serenedb/docs-search-core`](https://www.npmjs.com/package/@serenedb/docs-search-core) -- shared types and API client

## License

MIT
