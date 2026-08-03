# @serenedb/docs-search-embed

[SereneDocsSearch](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search)
as a single `<script>` tag -- no build step, no framework. React is bundled in.
DocSearch-style search modal with full-text (BM25), hybrid semantic search and
streamed "Ask AI" answers, powered by [SereneDB](https://serenedb.com).

## Usage

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

Or zero-JS auto-init:

```html
<script src="https://unpkg.com/@serenedb/docs-search-embed@latest/dist/serene-docs-search.js"
        data-backend-url="http://localhost:7700"
        data-container="#search"></script>
```

`init()` accepts the same options as the React component's props (`hotkey`,
`theme`, `sections`, `mcp`, ...) -- see the
[SereneDocsSearch README](https://github.com/serenedb/serenedb/tree/main/apps/serene-docs-search#readme)
for the full reference and backend setup.

Using React already? Prefer
[`@serenedb/docs-search-react`](https://www.npmjs.com/package/@serenedb/docs-search-react).

## License

MIT
