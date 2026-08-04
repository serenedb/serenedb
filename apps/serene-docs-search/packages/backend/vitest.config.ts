import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const src = (p: string) =>
    path.resolve(path.dirname(fileURLToPath(import.meta.url)), "src", p);

// mirror tsconfig "paths" — vitest resolves through vite, not tsc
export default defineConfig({
    resolve: {
        alias: {
            "@controllers": src("controllers"),
            "@services": src("services"),
            "@repositories": src("repositories"),
            "@routes": src("routes"),
            "@middlewares": src("middlewares"),
            "@utils": src("utils"),
            "@database": src("database"),
        },
    },
});
