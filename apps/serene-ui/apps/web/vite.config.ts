import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import fs from "fs";
import path from "path";
import { createRequire } from "module";
import tailwindcss from "@tailwindcss/vite";
import svgr from "vite-plugin-svgr";
import type { PluginContext } from "rollup";

export default defineConfig(({ mode }) => {
    const sharedFrontendPath = path.resolve(
        __dirname,
        "../../packages/shared-frontend",
    );
    const workspaceRequire = createRequire(import.meta.url);
    const appType = mode.includes("docker") ? "docker" : "electron";
    const isProdElectron = mode.includes("prod-electron");
    const resolvePackageAlias = (
        packageName: string,
        esmRelativePath?: string,
    ) => {
        try {
            const requirePath = workspaceRequire.resolve(packageName, {
                paths: [sharedFrontendPath],
            });
            if (!esmRelativePath) {
                return requirePath;
            }
            const esmPath = path.resolve(
                path.dirname(requirePath),
                esmRelativePath,
            );
            return fs.existsSync(esmPath) ? esmPath : requirePath;
        } catch {
            return null;
        }
    };
    // These packages are runtime deps of the grid stack, but in Docker's
    // no-lock workspace install they may be hoisted away from Rollup's
    // normal lookup path. Resolve them explicitly from shared-frontend.
    const transitiveAliases = Object.fromEntries(
        [
            [
                "canvas-hypertxt",
                resolvePackageAlias("canvas-hypertxt", "../js/index.js"),
            ],
            [
                "react-number-format",
                resolvePackageAlias(
                    "react-number-format",
                    "./react-number-format.es.js",
                ),
            ],
            [
                "@emotion/is-prop-valid",
                resolvePackageAlias(
                    "@emotion/is-prop-valid",
                    "./emotion-is-prop-valid.esm.js",
                ),
            ],
            ["@linaria/core", resolvePackageAlias("@linaria/core", "./index.mjs")],
        ].filter((entry): entry is [string, string] => entry[1] !== null),
    );
    const libpgQueryWasmPath = path.resolve(
        __dirname,
        "../../node_modules/libpg-query/wasm/libpg-query.wasm",
    );
    const libpgQueryWasmPlugin = (): Plugin => ({
        name: "libpg-query-wasm",
        configureServer(server: any) {
            server.middlewares.use((req: any, res: any, next: any) => {
                const requestPath = req.url?.split("?")[0] ?? "";

                if (!requestPath.endsWith("/libpg-query.wasm")) {
                    next();
                    return;
                }

                try {
                    const wasm = fs.readFileSync(libpgQueryWasmPath);
                    res.setHeader("Content-Type", "application/wasm");
                    res.statusCode = 200;
                    res.end(wasm);
                } catch (error) {
                    next();
                }
            });
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
        // Browser-based modes should use absolute asset URLs so route paths
        // like /dashboards don't rewrite wasm requests to /dashboards/*.wasm.
        base: isProdElectron ? "./" : "/",
        plugins: [libpgQueryWasmPlugin(), svgr(), react(), tailwindcss()],
        build: {
            outDir: `dist/${appType}`,
        },
        resolve: {
            alias: {
                "@": path.resolve(__dirname, "./src"),
                ...transitiveAliases,
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
