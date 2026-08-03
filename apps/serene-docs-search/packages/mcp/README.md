# @serenedb/docs-search-mcp

MCP server for [SereneDocsSearch](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search)
-- lets AI agents (Claude Code, Codex, Cursor, custom clients) search and read
documentation indexed in [SereneDB](https://serenedb.com). It talks to the
search backend over its public HTTP API, so it works against any deployed
instance.

## Tools

- `search_docs` -- full-text / hybrid search over the indexed docs
- `read_section` -- full text of one indexed section
- `docs_health` -- connectivity and index stats

## Usage

```bash
# stdio (for local MCP clients)
npx @serenedb/docs-search-mcp --backend https://search.example.com

# streamable HTTP on port 7710 -> http://localhost:7710/mcp
npx @serenedb/docs-search-mcp --backend http://localhost:7700 --http 7710
```

| Flag | Env | Meaning |
| --- | --- | --- |
| `--backend` | `SERENE_SEARCH_BACKEND_URL` | SereneDocsSearch backend url (default `http://localhost:7700`) |
| `--token` | `SERENE_SEARCH_TOKEN` | Admin token, only needed for private backends |
| `--site-url` | `SERENE_SEARCH_SITE_URL` | Docs site origin used to absolutize result urls |
| `--http <port>` | -- | Serve MCP over streamable HTTP instead of stdio |

Example Claude Code registration:

```bash
claude mcp add product-docs -- npx @serenedb/docs-search-mcp --backend https://search.example.com
```

Backend setup and the full documentation live in the
[SereneDocsSearch README](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search#readme).

## License

MIT
