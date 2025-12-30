import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";
import tailwindcss from "@tailwindcss/vite";
import svgr from "vite-plugin-svgr";

export default defineConfig(({ mode }) => {
    const appType = mode.includes("docker") ? "docker" : "electron";

    const config = {
        base: "./",
        plugins: [svgr(), react(), tailwindcss()],
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
            proxy: {
                "/rpc": {
                    target: "http://0.0.0.0:3000",
                    changeOrigin: true,
                },
            },
        },
    };
    return config;
});
