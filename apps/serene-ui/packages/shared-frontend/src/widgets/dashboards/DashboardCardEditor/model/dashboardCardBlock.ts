import type { DashboardBlockSchema } from "@serene-ui/shared-core";

export const isDashboardQueryBlock = (
    block: DashboardBlockSchema,
): block is Extract<
    DashboardBlockSchema,
    | { type: "table" }
    | { type: "single_string" }
    | { type: "bar_chart" }
    | { type: "line_chart" }
    | { type: "area_chart" }
    | { type: "pie_chart" }
> => {
    return (
        block.type === "table" ||
        block.type === "single_string" ||
        block.type === "bar_chart" ||
        block.type === "line_chart" ||
        block.type === "area_chart" ||
        block.type === "pie_chart"
    );
};

export const isDashboardChartBlock = (
    block: DashboardBlockSchema,
): block is Extract<
    DashboardBlockSchema,
    { type: "bar_chart" | "line_chart" | "area_chart" | "pie_chart" }
> => {
    return (
        block.type === "bar_chart" ||
        block.type === "line_chart" ||
        block.type === "area_chart" ||
        block.type === "pie_chart"
    );
};

const getQueryBlockBase = (block: DashboardBlockSchema) => ({
    connection_id: isDashboardQueryBlock(block)
        ? block.connection_id
        : undefined,
    database: isDashboardQueryBlock(block) ? block.database : undefined,
    query: isDashboardQueryBlock(block) ? block.query : "select 1",
    name: isDashboardQueryBlock(block) ? block.name : "No name",
    description: isDashboardQueryBlock(block) ? block.description : undefined,
    custom_refresh_interval_enabled: isDashboardQueryBlock(block)
        ? block.custom_refresh_interval_enabled
        : false,
    custom_refresh_interval: isDashboardQueryBlock(block)
        ? block.custom_refresh_interval
        : 60,
    custom_row_limit_enabled: isDashboardQueryBlock(block)
        ? block.custom_row_limit_enabled
        : false,
    custom_row_limit: isDashboardQueryBlock(block)
        ? block.custom_row_limit
        : 1000,
});

export const replaceDashboardBlockType = (
    block: DashboardBlockSchema,
    nextType: DashboardBlockSchema["type"],
): DashboardBlockSchema => {
    if (block.type === nextType) {
        return block;
    }

    const baseBlock = {
        id: block.id,
        dashboard_id: block.dashboard_id,
        bounds: block.bounds,
    };

    switch (nextType) {
        case "text":
            return {
                ...baseBlock,
                type: "text",
                text: block.type === "text" ? block.text : "",
            };
        case "spacer":
            return {
                ...baseBlock,
                type: "spacer",
            };
        case "table":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "table",
                columns: block.type === "table" ? block.columns : [],
            };
        case "single_string":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "single_string",
                column: block.type === "single_string" ? block.column : "value",
                fallback_value:
                    block.type === "single_string"
                        ? block.fallback_value
                        : undefined,
            };
        case "bar_chart":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "bar_chart",
                variant: block.type === "bar_chart" ? block.variant : "vertical",
                category_key:
                    block.type === "bar_chart"
                        ? block.category_key
                        : "category",
                default_active_key:
                    block.type === "bar_chart"
                        ? block.default_active_key
                        : undefined,
                value_label: block.type === "bar_chart" ? block.value_label : "Value",
                is_stacked: block.type === "bar_chart" ? block.is_stacked : false,
                series:
                    block.type === "bar_chart"
                        ? block.series
                        : [
                              {
                                  key: "value",
                                  label: "Value",
                                  color: "var(--chart-1)",
                              },
                          ],
            };
        case "line_chart":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "line_chart",
                variant: block.type === "line_chart" ? block.variant : "default",
                x_axis_key:
                    block.type === "line_chart" ? block.x_axis_key : "category",
                default_active_key:
                    block.type === "line_chart"
                        ? block.default_active_key
                        : undefined,
                value_label: block.type === "line_chart" ? block.value_label : "Value",
                line_type: block.type === "line_chart" ? block.line_type : "natural",
                series:
                    block.type === "line_chart"
                        ? block.series
                        : [
                              {
                                  key: "value",
                                  label: "Value",
                                  color: "var(--chart-1)",
                              },
                          ],
            };
        case "area_chart":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "area_chart",
                variant: block.type === "area_chart" ? block.variant : "default",
                x_axis_key:
                    block.type === "area_chart" ? block.x_axis_key : "category",
                default_active_key:
                    block.type === "area_chart"
                        ? block.default_active_key
                        : undefined,
                value_label: block.type === "area_chart" ? block.value_label : "Value",
                line_type: block.type === "area_chart" ? block.line_type : "natural",
                series:
                    block.type === "area_chart"
                        ? block.series
                        : [
                              {
                                  key: "value",
                                  label: "Value",
                                  color: "var(--chart-1)",
                              },
                          ],
            };
        case "pie_chart":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "pie_chart",
                interactive: block.type === "pie_chart" ? block.interactive : false,
                variant: block.type === "pie_chart" ? block.variant : "pie",
                name_key: block.type === "pie_chart" ? block.name_key : "name",
                value_key: block.type === "pie_chart" ? block.value_key : "value",
                default_active_key:
                    block.type === "pie_chart"
                        ? block.default_active_key
                        : undefined,
                value_label: block.type === "pie_chart" ? block.value_label : "Value",
                color_key: block.type === "pie_chart" ? block.color_key : "fill",
                show_labels: block.type === "pie_chart" ? block.show_labels : false,
                show_center_label:
                    block.type === "pie_chart"
                        ? block.show_center_label
                        : false,
                center_value:
                    block.type === "pie_chart" ? block.center_value : undefined,
                center_label:
                    block.type === "pie_chart" ? block.center_label : undefined,
            };
    }
};
