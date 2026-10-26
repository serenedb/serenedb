import type { SavedQuerySchema } from "@serene-ui/shared-core";
import type { SavedQueryRow } from "./saved-query.types.js";

/**
 * Converts a SavedQuery entity to a flat database row format.
 * Serializes bind_vars to JSON string for database storage.
 */
export function savedQueryToRow(
    savedQuery: Omit<SavedQuerySchema, "id">,
): Omit<SavedQueryRow, "id"> {
    const bind_vars = savedQuery.bind_vars
        ? JSON.stringify(savedQuery.bind_vars)
        : null;
    return {
        ...savedQuery,
        bind_vars,
    };
}

/**
 * Converts a flat database row to a SavedQuery entity.
 * Deserializes JSON string back to bind_vars array.
 */
export function rowToSavedQuery(row: SavedQueryRow): SavedQuerySchema {
    const r = row as any;

    return {
        id: r.id,
        name: r.name,
        query: r.query,
        usage_count: r.usage_count,
        bind_vars: r.bind_vars ? JSON.parse(r.bind_vars.toString()) : [],
    };
}
