import {
    buildDelete,
    buildInsert,
    buildSelect,
    buildUpdate,
} from "../../utils/request-builder.js";
import { savedQueryToRow, rowToSavedQuery } from "./saved-query.mappers.js";
import { SavedQuerySchema } from "@serene-ui/shared-core";
import { DBClient } from "../../database/db-init.js";
import { SavedQueryRow } from "./saved-query.types.js";

export const SavedQueryRepository = {
    findById: (id: number): SavedQuerySchema | null => {
        const { sql, values } = buildSelect("saved_queries", {
            where: { id },
        });
        const row = DBClient.prepare(sql).get(...values);
        return row ? rowToSavedQuery(row as SavedQueryRow) : null;
    },

    findOne: (query?: Partial<SavedQuerySchema>): SavedQuerySchema | null => {
        const { sql, values } = buildSelect("saved_queries", {
            where: query,
            limit: 1,
        });
        const row = DBClient.prepare(sql).get(...values);
        return row ? rowToSavedQuery(row as SavedQueryRow) : null;
    },

    findMany: (query?: Partial<SavedQuerySchema>): SavedQuerySchema[] => {
        const { sql, values } = buildSelect("saved_queries", {
            where: query,
        });
        const rows = DBClient.prepare(sql).all(...values);
        return rows.map((row) => rowToSavedQuery(row as SavedQueryRow));
    },

    create: (savedQuery: Omit<SavedQuerySchema, "id">): SavedQuerySchema => {
        const savedQueryRow = savedQueryToRow(savedQuery);

        const { sql, values } = buildInsert("saved_queries", savedQueryRow);
        const result = DBClient.prepare(sql).run(...values);

        const { sql: selectSql, values: selectValues } = buildSelect(
            "saved_queries",
            {
                where: { id: result.lastInsertRowid as number },
            },
        );
        const row = DBClient.prepare(selectSql).get(...selectValues);

        return rowToSavedQuery(row as SavedQueryRow);
    },

    update: (
        id: number,
        updates: Partial<SavedQuerySchema>,
    ): SavedQuerySchema | null => {
        const entries = Object.entries(updates).filter(
            ([key, val]) => key !== "id" && val !== undefined,
        );

        if (!entries.length) return null;

        const processedUpdates: Record<string, any> = {};
        entries.forEach(([key, val]) => {
            if (typeof val === "boolean") {
                processedUpdates[key] = val ? 1 : 0;
            } else if (key === "bind_vars") {
                processedUpdates[key] = JSON.stringify(val ?? []);
            } else {
                processedUpdates[key] = val;
            }
        });

        const { sql, values } = buildUpdate("saved_queries", processedUpdates, {
            where: { id },
        });
        DBClient.prepare(sql).run(...values);

        const { sql: selectSql, values: selectValues } = buildSelect(
            "saved_queries",
            {
                where: { id },
            },
        );
        const row = DBClient.prepare(selectSql).get(...selectValues);

        return row ? rowToSavedQuery(row as SavedQueryRow) : null;
    },

    delete: (id: number): SavedQuerySchema | null => {
        const { sql: selectSql, values: selectValues } = buildSelect(
            "saved_queries",
            {
                where: { id },
            },
        );
        const row = DBClient.prepare(selectSql).get(...selectValues);
        if (!row) return null;

        const { sql, values } = buildDelete("saved_queries", {
            where: { id },
        });
        DBClient.prepare(sql).run(...values);
        return rowToSavedQuery(row as SavedQueryRow);
    },
};
