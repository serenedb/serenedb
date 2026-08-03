#!/usr/bin/env node
import "reflect-metadata";
import { app } from "./app";
import { loadConfig } from "./config";
import { createServer } from "./server";

async function main(): Promise<void> {
    const config = loadConfig(app.env);
    if (config) {
        try {
            await app.applyConfig(config, { persist: false });
            console.log(`config loaded: source=${config.source.type}, search=${config.search.type}`);
        } catch (err) {
            console.error("failed to apply config on boot:", (err as Error).message);
            console.error("serving in unconfigured mode — fix the config or push a new one");
        }
    } else {
        console.log("no config yet — waiting for the setup wizard (PUT /v1/config)");
    }

    const server = createServer();
    server.listen(app.env.port, () => {
        console.log(`SereneDocsSearch backend listening on :${app.env.port}`);
    });

    const bye = async () => {
        await app.shutdown();
        process.exit(0);
    };
    process.on("SIGINT", bye);
    process.on("SIGTERM", bye);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
