import path from "node:path";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";
import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import svgr from "vite-plugin-svgr";
import type { Plugin } from "vite";
import type { PluginContext } from "rollup";
import { compareScreenshot } from "./commands/compareScreenshot";

const dirname =
    typeof __dirname !== "undefined"
        ? __dirname
        : path.dirname(fileURLToPath(import.meta.url));

const libpgQueryWasmPath = path.resolve(
    dirname,
    "../../../node_modules/libpg-query/wasm/libpg-query.wasm",
);

const libpgQueryWasmPlugin = (): Plugin => ({
    name: "libpg-query-wasm",
    configureServer(server: any) {
        server.middlewares.use(
            "/libpg-query.wasm",
            (_req: any, res: any, next: any) => {
                try {
                    const wasm = fs.readFileSync(libpgQueryWasmPath);
                    res.setHeader("Content-Type", "application/wasm");
                    res.statusCode = 200;
                    res.end(wasm);
                } catch {
                    next();
                }
            },
        );
    },
    generateBundle(this: PluginContext) {
        const wasm = fs.readFileSync(libpgQueryWasmPath);
        this.emitFile({
            type: "asset",
            fileName: "libpg-query.wasm",
            source: wasm,
        });
    },
});

export default defineConfig({
    plugins: [
        libpgQueryWasmPlugin(),
        svgr(),
        react(),
        tailwindcss(),
        storybookTest({
            configDir: dirname,
            tags: {
                include: ["test"],
            },
        }),
    ],
    resolve: {
        alias: {
            "@": path.resolve(dirname, "../src"),
            react: path.resolve(dirname, "../../../node_modules/react"),
            "react-dom": path.resolve(
                dirname,
                "../../../node_modules/react-dom",
            ),
        },
        dedupe: [
            "react",
            "react-dom",
            "react-router",
            "react-router-dom",
            "@tanstack/react-query",
            "@tanstack/query-core",
        ],
    },
    server: {
        fs: {
            allow: ["../../.."],
        },
    },
    test: {
        name: "storybook",
        browser: {
            enabled: true,
            headless: true,
            provider: playwright({
                contextOptions: {
                    deviceScaleFactor: 1,
                },
            }),
            instances: [{ browser: "chromium" }],
            viewport: {
                width: 960,
                height: 720,
            },
            commands: {
                compareScreenshot,
            },
        },
        setupFiles: [
            "./.storybook/vitest.setup.ts",
            "./.storybook/vitest.matchers.ts",
        ],
    },
});
