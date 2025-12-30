import type { ForgeConfig } from "@electron-forge/shared-types";
import { MakerSquirrel } from "@electron-forge/maker-squirrel";
import { MakerZIP } from "@electron-forge/maker-zip";
import { MakerDeb } from "@electron-forge/maker-deb";
import { MakerRpm } from "@electron-forge/maker-rpm";
import { MakerDMG } from "@electron-forge/maker-dmg";
import { AutoUnpackNativesPlugin } from "@electron-forge/plugin-auto-unpack-natives";
import { WebpackPlugin } from "@electron-forge/plugin-webpack";
import { FusesPlugin } from "@electron-forge/plugin-fuses";
import { FuseV1Options, FuseVersion } from "@electron/fuses";

import { mainConfig } from "./webpack.main.config";
import { rendererConfig } from "./webpack.renderer.config";
import path from "path";
import dotenv from "dotenv";

const isDevelopment = process.env.ENV_MODE === "development";
const envFile = isDevelopment ? ".env.dev" : ".env";
dotenv.config({ path: envFile });

const hasCorrectDotenv =
    process.env.APPLE_ID &&
    process.env.APPLE_ID_PASSWORD &&
    process.env.APPLE_TEAM_ID;

const config: ForgeConfig = {
    packagerConfig: {
        osxSign: hasCorrectDotenv ? {} : undefined,
        osxNotarize: hasCorrectDotenv
            ? {
                  appleId: process.env.APPLE_ID || "",
                  appleIdPassword: process.env.APPLE_ID_PASSWORD || "",
                  teamId: process.env.APPLE_TEAM_ID || "",
              }
            : undefined,
        asar: true,
        name: "SereneUI",
        icon: path.resolve(__dirname, "./assets/icons/app.icns"),
        executableName: "serene-ui-electron",
    },
    rebuildConfig: {},
    makers: [
        new MakerSquirrel({
            authors: "SereneDB",
            description:
                "A database client built for SereneDB and compatible with PostgreSQL",
        }),
        new MakerZIP({}, ["darwin"]),
        new MakerDMG({ format: "ULFO" }),
        new MakerRpm({}),
        new MakerDeb({}, ["linux"]),
    ],
    plugins: [
        new AutoUnpackNativesPlugin({}),
        new WebpackPlugin({
            mainConfig,
            renderer: {
                config: rendererConfig,
                entryPoints: [
                    {
                        html: "../web/dist/electron/index.html",
                        js: "./src/renderer.ts",
                        name: "main_window",
                        preload: {
                            js: "./src/preload.ts",
                        },
                    },
                ],
            },
        }),
        new FusesPlugin({
            version: FuseVersion.V1,
            [FuseV1Options.RunAsNode]: false,
            [FuseV1Options.EnableCookieEncryption]: true,
            [FuseV1Options.EnableNodeOptionsEnvironmentVariable]: false,
            [FuseV1Options.EnableNodeCliInspectArguments]: false,
            [FuseV1Options.EnableEmbeddedAsarIntegrityValidation]: true,
            [FuseV1Options.OnlyLoadAppFromAsar]: true,
        }),
    ],
};

export default config;
