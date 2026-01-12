import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";
import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import svgr from "vite-plugin-svgr";
import { compareScreenshot } from "./commands/compareScreenshot";

const dirname =
    typeof __dirname !== "undefined"
        ? __dirname
        : path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
    base: "./",
    plugins: [
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
            provider: playwright(),
            instances: [{ browser: "chromium" }],
            viewport: {
                width: 1920,
                height: 1080,
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
