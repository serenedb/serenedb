import type IForkTsCheckerWebpackPlugin from "fork-ts-checker-webpack-plugin";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const ForkTsCheckerWebpackPlugin: typeof IForkTsCheckerWebpackPlugin = require("fork-ts-checker-webpack-plugin");
// eslint-disable-next-line @typescript-eslint/no-var-requires
const CopyWebpackPlugin = require("copy-webpack-plugin");

export const plugins = [
    new ForkTsCheckerWebpackPlugin({
        logger: "webpack-infrastructure",
    }),
    new CopyWebpackPlugin({
        patterns: [
            {
                from: "../web/dist/electron",
                to: "web",
            },
            {
                from: "./assets",
                to: "assets",
            },
            {
                from: "../../packages/shared-backend/src/migrations",
                to: "migrations",
            },
        ],
    }),
];
