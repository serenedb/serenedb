import {
    DashboardBlockSchema as DashboardBlockEntitySchema,
    type DashboardBlockSchema,
} from "@serene-ui/shared-core";
import type { DashboardBlockRow } from "./dashboard-block.types.js";

export type DashboardBlockWrite = DashboardBlockSchema extends infer T
    ? T extends { id: number }
        ? Omit<T, "id"> & {
              id?: number;
          }
        : never
    : never;

const parseJson = <T>(value: string | null): T | undefined => {
    if (!value) {
        return undefined;
    }

    return JSON.parse(value) as T;
};

const toOptionalString = (value: string | null): string | undefined =>
    value ?? undefined;

const toOptionalNumber = (value: number | null): number | undefined =>
    value ?? undefined;

const queryBlockBaseFromRow = (row: DashboardBlockRow) => ({
    connection_id: toOptionalNumber(row.connection_id),
    database: toOptionalString(row.database),
    query: row.query ?? "",
    name: toOptionalString(row.name),
    description: toOptionalString(row.description),
    custom_refresh_interval_enabled: Boolean(
        row.custom_refresh_interval_enabled ?? 0,
    ),
    custom_refresh_interval: toOptionalNumber(row.custom_refresh_interval),
    custom_row_limit_enabled: Boolean(row.custom_row_limit_enabled ?? 0),
    custom_row_limit: toOptionalNumber(row.custom_row_limit),
});

export const dashboardBlockToRow = (
    block: DashboardBlockWrite,
    position: number,
): DashboardBlockRow => {
    const baseRow: DashboardBlockRow = {
        id: block.id ?? null,
        dashboard_id: block.dashboard_id,
        type: block.type,
        position,
        bounds: JSON.stringify(block.bounds),
        text: null,
        connection_id: null,
        database: null,
        query: null,
        name: null,
        description: null,
        custom_refresh_interval_enabled: 0,
        custom_refresh_interval: 60,
        custom_row_limit_enabled: 0,
        custom_row_limit: 1000,
        config: null,
    };

    switch (block.type) {
        case "text":
            return {
                ...baseRow,
                text: block.text,
            };
        case "spacer":
            return baseRow;
        case "table":
            return {
                ...baseRow,
                connection_id: block.connection_id ?? null,
                database: block.database ?? null,
                query: block.query,
                name: block.name,
                description: block.description ?? null,
                custom_refresh_interval_enabled:
                    block.custom_refresh_interval_enabled ? 1 : 0,
                custom_refresh_interval: block.custom_refresh_interval,
                custom_row_limit_enabled: block.custom_row_limit_enabled
                    ? 1
                    : 0,
                custom_row_limit: block.custom_row_limit,
                config: block.columns
                    ? JSON.stringify({
                          columns: block.columns,
                      })
                    : null,
            };
        case "single_string":
            return {
                ...baseRow,
                connection_id: block.connection_id ?? null,
                database: block.database ?? null,
                query: block.query,
                name: block.name,
                description: block.description ?? null,
                custom_refresh_interval_enabled:
                    block.custom_refresh_interval_enabled ? 1 : 0,
                custom_refresh_interval: block.custom_refresh_interval,
                custom_row_limit_enabled: block.custom_row_limit_enabled
                    ? 1
                    : 0,
                custom_row_limit: block.custom_row_limit,
                config: JSON.stringify({
                    column: block.column,
                    fallback_value: block.fallback_value,
                }),
            };
        case "bar_chart":
            return {
                ...baseRow,
                connection_id: block.connection_id ?? null,
                database: block.database ?? null,
                query: block.query,
                name: block.name,
                description: block.description ?? null,
                custom_refresh_interval_enabled:
                    block.custom_refresh_interval_enabled ? 1 : 0,
                custom_refresh_interval: block.custom_refresh_interval,
                custom_row_limit_enabled: block.custom_row_limit_enabled
                    ? 1
                    : 0,
                custom_row_limit: block.custom_row_limit,
                config: JSON.stringify({
                    variant: block.variant,
                    category_key: block.category_key,
                    default_active_key: block.default_active_key,
                    value_label: block.value_label,
                    is_stacked: block.is_stacked,
                    series: block.series,
                }),
            };
        case "line_chart":
            return {
                ...baseRow,
                connection_id: block.connection_id ?? null,
                database: block.database ?? null,
                query: block.query,
                name: block.name,
                description: block.description ?? null,
                custom_refresh_interval_enabled:
                    block.custom_refresh_interval_enabled ? 1 : 0,
                custom_refresh_interval: block.custom_refresh_interval,
                custom_row_limit_enabled: block.custom_row_limit_enabled
                    ? 1
                    : 0,
                custom_row_limit: block.custom_row_limit,
                config: JSON.stringify({
                    variant: block.variant,
                    x_axis_key: block.x_axis_key,
                    default_active_key: block.default_active_key,
                    value_label: block.value_label,
                    line_type: block.line_type,
                    series: block.series,
                }),
            };
        case "pie_chart":
            return {
                ...baseRow,
                connection_id: block.connection_id ?? null,
                database: block.database ?? null,
                query: block.query,
                name: block.name,
                description: block.description ?? null,
                custom_refresh_interval_enabled:
                    block.custom_refresh_interval_enabled ? 1 : 0,
                custom_refresh_interval: block.custom_refresh_interval,
                custom_row_limit_enabled: block.custom_row_limit_enabled
                    ? 1
                    : 0,
                custom_row_limit: block.custom_row_limit,
                config: JSON.stringify({
                    interactive: block.interactive,
                    variant: block.variant,
                    name_key: block.name_key,
                    value_key: block.value_key,
                    default_active_key: block.default_active_key,
                    value_label: block.value_label,
                    color_key: block.color_key,
                    show_labels: block.show_labels,
                    show_center_label: block.show_center_label,
                    center_value: block.center_value,
                    center_label: block.center_label,
                }),
            };
    }

    const unsupportedBlock: never = block;
    throw new Error(
        `Unsupported dashboard block payload: ${JSON.stringify(unsupportedBlock)}`,
    );
};

export const rowToDashboardBlock = (
    row: DashboardBlockRow,
): DashboardBlockSchema => {
    const baseBlock = {
        id: row.id ?? 0,
        dashboard_id: row.dashboard_id ?? 0,
        bounds: JSON.parse(row.bounds ?? "{}"),
    };
    const config = parseJson<Record<string, unknown>>(row.config) ?? {};

    switch (row.type) {
        case "text":
            return DashboardBlockEntitySchema.parse({
                ...baseBlock,
                type: "text",
                text: row.text ?? "",
            });
        case "spacer":
            return DashboardBlockEntitySchema.parse({
                ...baseBlock,
                type: "spacer",
            });
        case "table":
            return DashboardBlockEntitySchema.parse({
                ...baseBlock,
                ...queryBlockBaseFromRow(row),
                type: "table",
                columns: config.columns,
            });
        case "single_string":
            return DashboardBlockEntitySchema.parse({
                ...baseBlock,
                ...queryBlockBaseFromRow(row),
                type: "single_string",
                column: config.column,
                fallback_value: config.fallback_value,
            });
        case "bar_chart":
            return DashboardBlockEntitySchema.parse({
                ...baseBlock,
                ...queryBlockBaseFromRow(row),
                type: "bar_chart",
                variant: config.variant,
                category_key: config.category_key,
                default_active_key: config.default_active_key,
                value_label: config.value_label,
                is_stacked: config.is_stacked,
                series: config.series,
            });
        case "line_chart":
            return DashboardBlockEntitySchema.parse({
                ...baseBlock,
                ...queryBlockBaseFromRow(row),
                type: "line_chart",
                variant: config.variant,
                x_axis_key: config.x_axis_key,
                default_active_key: config.default_active_key,
                value_label: config.value_label,
                line_type: config.line_type,
                series: config.series,
            });
        case "pie_chart":
            return DashboardBlockEntitySchema.parse({
                ...baseBlock,
                ...queryBlockBaseFromRow(row),
                type: "pie_chart",
                interactive: config.interactive,
                variant: config.variant,
                name_key: config.name_key,
                value_key: config.value_key,
                default_active_key: config.default_active_key,
                value_label: config.value_label,
                color_key: config.color_key,
                show_labels: config.show_labels,
                show_center_label: config.show_center_label,
                center_value: config.center_value,
                center_label: config.center_label,
            });
        default:
            throw new Error(`Unsupported dashboard block type: ${row.type}`);
    }
};
