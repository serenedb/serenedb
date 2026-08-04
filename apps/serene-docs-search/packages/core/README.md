# @serenedb/docs-search-core

Shared internals for [SereneDocsSearch](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search)
-- documentation search powered by [SereneDB](https://serenedb.com).

You normally don't install this directly; it's consumed by
[`@serenedb/docs-search-react`](https://www.npmjs.com/package/@serenedb/docs-search-react),
[`@serenedb/docs-search-embed`](https://www.npmjs.com/package/@serenedb/docs-search-embed)
and [`@serenedb/docs-search-mcp`](https://www.npmjs.com/package/@serenedb/docs-search-mcp).
Use it when building your own UI or agent on top of a SereneDocsSearch backend.

## What's inside

- **Types** -- the config schema (`SereneSearchConfig`) and API request/response
  types for `/v1/search`, `/v1/ask`, `/v1/health`, sync and config endpoints
- **`SereneSearchClient`** -- typed API client for the backend, including the
  SSE-streamed `ask()`
- **Defaults** -- ports, config filename, known extensions, default config
  generator
- **Compose generator** -- `generateCompose()` produces the
  `docker-compose.yml` used by the first-run wizard
- **Agent tools** -- `search_docs` / `read_section` tool definitions and result
  formatters shared by Ask AI and the MCP server

```ts
import { SereneSearchClient } from "@serenedb/docs-search-core";

const client = new SereneSearchClient({ backendUrl: "http://localhost:7700" });
const res = await client.search("create index", { mode: "hybrid" });
```

Full documentation:
[SereneDocsSearch README](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search#readme).

## License

Apache-2.0
