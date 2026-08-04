import { defineConfig } from "tsup";

export default defineConfig({
    entry: ["src/index.ts"],
    format: ["esm"],
    clean: true,
    banner: { js: "#!/usr/bin/env node" },
    // core is inlined so the published package only depends on the MCP SDK
    noExternal: ["@serenedb/docs-search-core"],
});
