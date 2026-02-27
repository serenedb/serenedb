import fs from "fs";
import path from "path";
import { SavedQueryRepository } from "../../repositories/saved-query/saved-query.repository.js";
import { logger } from "../../utils/logger.js";

const resolveImportQueriesDir = (
    importQueriesPackName: string,
): string | null => {
    const candidates = [
        path.resolve(process.cwd(), "import_queries"),
        path.resolve(process.cwd(), "../import_queries"),
        path.resolve(process.cwd(), "../../import_queries"),
    ];

    for (const importQueriesRoot of candidates) {
        if (
            !fs.existsSync(importQueriesRoot) ||
            !fs.statSync(importQueriesRoot).isDirectory()
        ) {
            continue;
        }

        const importQueriesDir = path.join(
            importQueriesRoot,
            importQueriesPackName,
        );
        if (
            fs.existsSync(importQueriesDir) &&
            fs.statSync(importQueriesDir).isDirectory()
        ) {
            return importQueriesDir;
        }
    }

    return null;
};

export const loadImportQueries = (): void => {
    const importQueriesPackName = process.env.IMPORT_QUERIES?.trim();

    if (!importQueriesPackName) {
        return;
    }

    if (!/^[a-zA-Z0-9._-]+$/.test(importQueriesPackName)) {
        logger.warn(
            `Skipped import queries preload: invalid IMPORT_QUERIES value "${importQueriesPackName}"`,
        );
        return;
    }

    const importQueriesDir = resolveImportQueriesDir(importQueriesPackName);
    if (!importQueriesDir) {
        logger.warn(
            `Skipped import queries preload: import queries pack "${importQueriesPackName}" not found`,
        );
        return;
    }

    const sqlFiles = fs
        .readdirSync(importQueriesDir)
        .filter((fileName) => fileName.toLowerCase().endsWith(".sql"))
        .sort();

    if (!sqlFiles.length) {
        logger.warn(
            `Skipped import queries preload: no .sql files found in "${importQueriesPackName}"`,
        );
        return;
    }

    for (const sqlFile of sqlFiles) {
        const filePath = path.join(importQueriesDir, sqlFile);
        const queryName = path.basename(sqlFile, ".sql");
        const query = fs.readFileSync(filePath, "utf-8");

        if (!query.trim()) {
            logger.warn(
                `Skipped empty SQL file "${sqlFile}" in "${importQueriesPackName}"`,
            );
            continue;
        }

        const existingQuery = SavedQueryRepository.findOne({ name: queryName });

        if (existingQuery) {
            SavedQueryRepository.update(existingQuery.id, { query });
            continue;
        }

        SavedQueryRepository.create({
            name: queryName,
            query,
            bind_vars: [],
            usage_count: 0,
        });
    }

    logger.info(
        `Import queries preload complete: loaded ${sqlFiles.length} saved queries from "${importQueriesPackName}"`,
    );
};
