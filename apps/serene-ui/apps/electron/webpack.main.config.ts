import type { Configuration } from "webpack";
import path from "path";

import { rules } from "./webpack.rules";
import { plugins } from "./webpack.plugins";

export const mainConfig: Configuration = {
    entry: {
        main: "./src/index.ts",
        "query-worker":
            "../../packages/shared-backend/dist/utils/worker-pool/query-worker.js",
    },
    output: {
        filename: (pathData) =>
            pathData.chunk?.name === "main" ? "index.js" : "[name].js",
    },
    module: {
        rules,
    },
    plugins,
    node: {
        __dirname: false,
        __filename: false,
    },
    resolve: {
        extensions: [".ts", ".tsx", ".js", ".jsx", ".css", ".json"],
        symlinks: true,
        fullySpecified: false,
        extensionAlias: {
            ".js": [".ts", ".tsx", ".js"],
            ".jsx": [".tsx", ".jsx"],
            ".mjs": [".mts", ".mjs"],
        },
        alias: {
            "@serene-ui/shared-core": path.resolve(
                __dirname,
                "../../packages/shared-core/src",
            ),
            "@serene-ui/shared-backend": path.resolve(
                __dirname,
                "../../packages/shared-backend/src",
            ),
            "@serene-ui/shared-frontend": path.resolve(
                __dirname,
                "../../packages/shared-frontend/src",
            ),
        },
    },
};
