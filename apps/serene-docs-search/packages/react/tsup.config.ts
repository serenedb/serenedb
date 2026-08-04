import { defineConfig } from "tsup";

export default defineConfig({
    entry: ["src/index.ts"],
    format: ["esm", "cjs"],
    dts: true,
    clean: true,
    // core is inlined so the published package is a single dependency-free tarball
    noExternal: ["@serenedb/docs-search-core"],
});
