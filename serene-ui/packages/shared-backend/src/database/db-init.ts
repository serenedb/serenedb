import Database from "better-sqlite3";
import { handleMigrations } from "./migrations-handler";

let DBClient: Database.Database;

export const initDatabase = (dbPath: string, migrationsPath?: string) => {
    DBClient = new Database(dbPath);
    handleMigrations(migrationsPath);
};

export { DBClient };
