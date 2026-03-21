import type {
    DashboardBaseSchema,
    DashboardBlockSchema,
    DashboardSchema,
} from "@serene-ui/shared-core";
import type { DashboardRow } from "./dashboard.types.js";

export function dashboardToRow(
    dashboard: Omit<
        DashboardBaseSchema,
        "id" | "created_at" | "updated_at"
    >,
): Omit<DashboardRow, "id" | "created_at" | "updated_at"> {
    return {
        name: dashboard.name,
        favorite: dashboard.favorite ? 1 : 0,
        auto_refresh: dashboard.auto_refresh ? 1 : 0,
        refresh_interval: dashboard.refresh_interval,
        row_limit: dashboard.row_limit,
    };
}

export function rowToDashboard(
    row: DashboardRow,
    blocks: DashboardBlockSchema[],
): DashboardSchema {
    return {
        id: row.id ?? 0,
        name: row.name ?? "",
        favorite: Boolean(row.favorite ?? 0),
        auto_refresh: Boolean(row.auto_refresh ?? 0),
        refresh_interval: row.refresh_interval ?? 60,
        row_limit: row.row_limit ?? 1000,
        created_at: row.created_at ?? undefined,
        updated_at: row.updated_at ?? undefined,
        blocks,
    };
}
