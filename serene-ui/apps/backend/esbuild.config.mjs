import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import esbuild from "esbuild";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const nativeModules = ["better-sqlite3", "@napi-rs/keyring", "pg-native"];

const external = nativeModules;

const mainConfig = {
    entryPoints: ["src/index.ts"],
    bundle: true,
    platform: "node",
    target: "node18",
    format: "cjs",
    outfile: "dist/index.js",
    external,
    sourcemap: true,
    minify: false,
    treeShaking: true,
    metafile: true,
    banner: {
        js: `const { pathToFileURL } = require('url');
const import_meta = { url: pathToFileURL(__filename).href };
`,
    },
    define: {
        "import.meta": "import_meta",
    },
    loader: {
        ".node": "file",
        ".json": "json",
    },
    logLevel: "info",
};

const workerConfig = {
    entryPoints: [
        "../../packages/shared-backend/src/utils/worker-pool/query-worker.ts",
    ],
    bundle: true,
    platform: "node",
    target: "node18",
    format: "cjs",
    outfile: "dist/query-worker.js",
    external,
    sourcemap: true,
    minify: false,
    treeShaking: true,
    banner: {
        js: `const { pathToFileURL } = require('url');
const import_meta = { url: pathToFileURL(__filename).href };
`,
    },
    define: {
        "import.meta": "import_meta",
    },
    loader: {
        ".node": "file",
        ".json": "json",
    },
    logLevel: "info",
};

function copyConfigFiles() {
    const configSrc = path.join(__dirname, "src/config");
    const configDest = path.join(__dirname, "dist/config");

    if (!fs.existsSync(configDest)) {
        fs.mkdirSync(configDest, { recursive: true });
    }

    const configFiles = fs
        .readdirSync(configSrc)
        .filter((f) => f.endsWith(".json"));
    configFiles.forEach((file) => {
        fs.copyFileSync(
            path.join(configSrc, file),
            path.join(configDest, file),
        );
    });

    console.log(`✓ Copied ${configFiles.length} config file(s)`);
}

function copyMigrations() {
    const migrationsSrc = path.join(
        __dirname,
        "..",
        "..",
        "packages",
        "shared-backend",
        "src",
        "migrations",
    );
    const migrationsDest = path.join(__dirname, "dist/migrations");

    if (fs.existsSync(migrationsSrc)) {
        if (!fs.existsSync(migrationsDest)) {
            fs.mkdirSync(migrationsDest, { recursive: true });
        }

        const migrationFiles = fs.readdirSync(migrationsSrc);
        migrationFiles.forEach((file) => {
            fs.copyFileSync(
                path.join(migrationsSrc, file),
                path.join(migrationsDest, file),
            );
        });

        console.log(`✓ Copied ${migrationFiles.length} migration file(s)`);
    } else {
        if (!fs.existsSync(migrationsDest)) {
            fs.mkdirSync(migrationsDest, { recursive: true });
        }
        console.log(`✓ Created empty migrations directory`);
    }
}

async function build() {
    try {
        await Promise.all([
            esbuild.build(mainConfig),
            esbuild.build(workerConfig),
        ]);

        copyConfigFiles();
        copyMigrations();

        console.log("✓ Build complete!");

        const bundleSize = fs.statSync(
            path.join(__dirname, "dist/index.js"),
        ).size;
        console.log(
            `✓ Bundle size: ${(bundleSize / 1024 / 1024).toFixed(2)} MB`,
        );
    } catch (error) {
        console.error("Build failed:", error);
        process.exit(1);
    }
}

build();
