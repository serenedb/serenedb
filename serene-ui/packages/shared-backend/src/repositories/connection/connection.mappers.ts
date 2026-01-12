import { ConnectionSchema } from "@serene-ui/shared-core";
import type { ConnectionRow } from "./connection.types.js";

/**
 * Converts a Connection entity to a flat database row format.
 * Handles discriminated union by extracting mode-specific properties.
 */
export function connectionToRow(
    connection: Omit<ConnectionSchema, "id">,
): Omit<ConnectionRow, "id"> {
    const host = "host" in connection ? connection.host : null;
    const socket = "socket" in connection ? connection.socket : null;
    const port = "port" in connection ? connection.port : 5432;
    const user = connection.user ?? null;
    const password = connection.password ?? null;
    const database = connection.database ?? null;
    const ssl = connection.ssl ? 1 : 0;

    return {
        ...connection,
        ssl,
        host,
        socket,
        port,
        user,
        password,
        database,
    };
}

/**
 * Converts a flat database row to a Connection entity.
 * Reconstructs the discriminated union based on the mode field.
 */
export function rowToConnection(row: ConnectionRow): ConnectionSchema {
    const r = row as any;

    const baseConnection = {
        id: r.id,
        name: r.name,
        type: r.type,
        ssl: Boolean(r.ssl),
        authMethod: r.authMethod,
        user: r.user ?? undefined,
        password: r.password ?? undefined,
        port: r.port,
        database: r.database ?? undefined,
    };

    if (r.mode === "host") {
        return {
            ...baseConnection,
            mode: "host",
            host: r.host,
        } as ConnectionSchema;
    } else {
        return {
            ...baseConnection,
            mode: "socket",
            socket: r.socket,
        } as ConnectionSchema;
    }
}
