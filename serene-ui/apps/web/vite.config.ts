import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import fs from "fs";
import path from "path";
import tailwindcss from "@tailwindcss/vite";
import svgr from "vite-plugin-svgr";
import type { PluginContext } from "rollup";

export default defineConfig(({ mode }) => {
    const appType = mode.includes("docker") ? "docker" : "electron";
    const libpgQueryWasmPath = path.resolve(
        __dirname,
        "../../node_modules/libpg-query/wasm/libpg-query.wasm",
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
                    } catch (error) {
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

    const config = {
        base: "./",
        plugins: [libpgQueryWasmPlugin(), svgr(), react(), tailwindcss()],
        build: {
            outDir: `dist/${appType}`,
        },
        resolve: {
            alias: {
                "@": path.resolve(__dirname, "./src"),
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
                allow: ["../.."],
            },
        },
    };
    return config;
});
