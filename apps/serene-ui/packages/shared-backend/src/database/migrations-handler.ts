import fs from "fs";
import path from "path";
import { DBClient } from "./db-init";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const handleMigrations = (migrationsPath?: string) => {
    DBClient.prepare(
        `
       CREATE TABLE IF NOT EXISTS migrations (
          id TEXT PRIMARY KEY,
          run_at TEXT NOT NULL
       )
    `,
    ).run();

    const migrationsDir = migrationsPath
        ? path.join(migrationsPath, "../migrations")
        : path.join(__dirname, "../migrations");
    const migrationFiles = fs.readdirSync(migrationsDir).sort();

    for (const file of migrationFiles) {
        const migrationId = path.basename(file);
        const alreadyRun = DBClient.prepare(
            "SELECT 1 FROM migrations WHERE id = ?",
        ).get(migrationId);

        if (!alreadyRun) {
            const sql = fs.readFileSync(
                path.join(migrationsDir, file),
                "utf-8",
            );
            DBClient.exec(sql);
            DBClient.prepare(
                "INSERT INTO migrations (id, run_at) VALUES (?, ?)",
            ).run(migrationId, new Date().toISOString());
        }
    }
};
