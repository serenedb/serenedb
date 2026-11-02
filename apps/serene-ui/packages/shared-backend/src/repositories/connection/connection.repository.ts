import { ConnectionSchema } from "@serene-ui/shared-core";
import { connectionToRow, rowToConnection } from "./connection.mappers.js";
import {
    buildDelete,
    buildInsert,
    buildSelect,
    buildUpdate,
} from "../../utils/request-builder.js";
import { DBClient } from "../../database/db-init.js";
import { ORPCError } from "@orpc/server";
import { ConnectionRow } from "./connection.types.js";

export const ConnectionRepository = {
    findById: (id: number): ConnectionSchema | null => {
        const { sql, values } = buildSelect("connections", {
            where: { id },
        });
        const row = DBClient.prepare(sql).get(...values);
        return row ? rowToConnection(row) : null;
    },

    findOne: (query?: Partial<ConnectionSchema>): ConnectionSchema | null => {
        const { sql, values } = buildSelect("connections", {
            where: query,
            limit: 1,
        });
        const row = DBClient.prepare(sql).get(...values);
        return row ? rowToConnection(row) : null;
    },

    findMany: (query?: Partial<ConnectionSchema>): ConnectionSchema[] => {
        const { sql, values } = buildSelect("connections", {
            where: query,
        });
        const rows = DBClient.prepare(sql).all(...values);
        return rows.map((row) => rowToConnection(row as ConnectionRow));
    },

    create: (
        connection: Omit<ConnectionSchema, "id">,
    ): ConnectionSchema | null => {
        const connectionRow = connectionToRow(connection);

        const { sql, values } = buildInsert("connections", connectionRow);

        const query = DBClient.prepare(sql);

        const result = query.run(...values);

        const { sql: selectSql, values: selectValues } = buildSelect(
            "connections",
            {
                where: { id: result.lastInsertRowid },
            },
        );
        const row = DBClient.prepare(selectSql).get(...selectValues);

        return row ? rowToConnection(row) : null;
    },

    update: (
        id: number,
        updates: Partial<ConnectionSchema>,
    ): ConnectionSchema | null => {
        if (!Object.keys(updates).length) return null;

        const { sql: checkSql, values: checkValues } = buildSelect(
            "connections",
            {
                where: { id },
            },
        );

        if (!DBClient.prepare(checkSql).get(...checkValues)) {
            throw new ORPCError("BAD_REQUEST", {
                message: "Connection not found",
            });
        }

        const processedUpdates = Object.fromEntries(
            Object.entries(updates).map(([key, val]) => [
                key,
                typeof val === "boolean" ? (val ? 1 : 0) : val,
            ]),
        );
        const { sql, values } = buildUpdate("connections", processedUpdates, {
            where: { id },
        });
        DBClient.prepare(sql).run(...values);

        const { sql: selectSql, values: selectValues } = buildSelect(
            "connections",
            {
                where: { id },
            },
        );

        const row = DBClient.prepare(selectSql).get(...selectValues);
        return row ? rowToConnection(row) : null;
    },

    delete: (id: number): ConnectionSchema | null => {
        const { sql: selectSql, values: selectValues } = buildSelect(
            "connections",
            {
                where: { id },
            },
        );

        const row = DBClient.prepare(selectSql).get(...selectValues);
        if (!row) {
            throw new ORPCError("BAD_REQUEST", {
                message: "Connection not found",
            });
        }

        const { sql, values } = buildDelete("connections", {
            where: { id },
        });
        DBClient.prepare(sql).run(...values);

        return rowToConnection(row);
    },
};
