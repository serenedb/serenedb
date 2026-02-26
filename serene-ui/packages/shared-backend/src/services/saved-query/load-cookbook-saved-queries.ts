import fs from "fs";
import path from "path";
import { SavedQueryRepository } from "../../repositories/saved-query/saved-query.repository.js";
import { logger } from "../../utils/logger.js";

const resolveCookbookDir = (cookbookName: string): string | null => {
    const candidates = [
        path.resolve(process.cwd(), "cookbooks"),
        path.resolve(process.cwd(), "../cookbooks"),
        path.resolve(process.cwd(), "../../cookbooks"),
    ];

    for (const cookbooksRoot of candidates) {
        if (
            !fs.existsSync(cookbooksRoot) ||
            !fs.statSync(cookbooksRoot).isDirectory()
        ) {
            continue;
        }

        const cookbookDir = path.join(cookbooksRoot, cookbookName);
        if (
            fs.existsSync(cookbookDir) &&
            fs.statSync(cookbookDir).isDirectory()
        ) {
            return cookbookDir;
        }
    }

    return null;
};

export const loadCookbookSavedQueries = (): void => {
    const cookbookName = process.env.LOAD_COOKBOOK?.trim();

    if (!cookbookName) {
        return;
    }

    if (!/^[a-zA-Z0-9._-]+$/.test(cookbookName)) {
        logger.warn(
            `Skipped cookbook preload: invalid LOAD_COOKBOOK value "${cookbookName}"`,
        );
        return;
    }

    const cookbookDir = resolveCookbookDir(cookbookName);
    if (!cookbookDir) {
        logger.warn(
            `Skipped cookbook preload: cookbook "${cookbookName}" not found`,
        );
        return;
    }

    const sqlFiles = fs
        .readdirSync(cookbookDir)
        .filter((fileName) => fileName.toLowerCase().endsWith(".sql"))
        .sort();

    if (!sqlFiles.length) {
        logger.warn(
            `Skipped cookbook preload: no .sql files found in "${cookbookName}"`,
        );
        return;
    }

    for (const sqlFile of sqlFiles) {
        const filePath = path.join(cookbookDir, sqlFile);
        const queryName = path.basename(sqlFile, ".sql");
        const query = fs.readFileSync(filePath, "utf-8");

        if (!query.trim()) {
            logger.warn(
                `Skipped empty SQL file "${sqlFile}" in "${cookbookName}"`,
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
        `Cookbook preload complete: loaded ${sqlFiles.length} saved queries from "${cookbookName}"`,
    );
};
