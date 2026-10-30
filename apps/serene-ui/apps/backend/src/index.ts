import {
    initDatabase,
    loadDefaultConnection,
    loadImportQueries,
    logger,
    setWorkerPath,
} from "@serene-ui/shared-backend";
import config from "config";
import path from "path";
import { initServer } from "./server";

const PORT: number = config.get("port") || 3000;
const HOST: string =
    process.env.BACKEND_HOST ||
    (config.has("host") ? config.get("host") : "0.0.0.0");

const run = async () => {
    const dbPath = path.join(__dirname, "db.sqlite");

    const isDevMode = __dirname.includes("/src");
    const migrationsPath = isDevMode
        ? path.join(
              __dirname,
              "../../../packages/shared-backend/src/migrations",
          )
        : path.join(__dirname, "migrations");

    initDatabase(dbPath, migrationsPath);
    loadDefaultConnection();
    loadImportQueries();

    const workerPath = isDevMode
        ? path.join(
              __dirname,
              "../../../packages/shared-backend/dist/utils/worker-pool/query-worker.js",
          )
        : path.join(__dirname, "query-worker.js");
    setWorkerPath(workerPath);

    initServer(PORT, HOST);
};

run().catch((err) => {
    logger.error(err);
});
