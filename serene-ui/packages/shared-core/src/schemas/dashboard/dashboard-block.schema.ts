import z from "zod";

const DashboardSchemaBlockBase = z.object({
    id: z.number(),
    dashboard_id: z.number(),
    bounds: z.object({
        x: z.number(),
        y: z.number(),
        width: z.number(),
        height: z.number(),
    }),
});

const DashboardQueryBlockBaseSchema = DashboardSchemaBlockBase.extend({
    query: z.string(),
    name: z.string().max(255).optional().default("No name"),
    description: z.string().max(1024).optional(),
    custom_refresh_interval_enabled: z.boolean().default(false),
    custom_refresh_interval: z.number().default(60),
    custom_row_limit_enabled: z.boolean().default(false),
    custom_row_limit: z.boolean().default(false),
});

const DashboardChartColumns = z.array(
    z.object({
        axis: z.array(z.enum(["x", "y"])),
        name: z.string(),
        is_dimension: z.boolean(),
        color: z.string().optional(),
    }),
);

const DashboardTextBlockSchema = DashboardSchemaBlockBase.extend({
    type: z.literal("text"),
    text: z.string(),
});

const DashboardSpacerBlockSchema = DashboardSchemaBlockBase.extend({
    type: z.literal("spacer"),
});

const DashboardTableBlockSchema = DashboardQueryBlockBaseSchema.extend({
    type: z.literal("table"),
});

const DashboardSingleStringBlockSchema = DashboardQueryBlockBaseSchema.extend({
    type: z.literal("single_string"),
    column: z.string(),
});

const DashboardBarChartBlockSchema = DashboardQueryBlockBaseSchema.extend({
    type: z.literal("bar_chart"),
    variant: z.enum([
        "interactive",
        "default_vertical",
        "defaut_horizontal",
        "stacked_vertical",
        "stacked_horizontal",
    ]),
    columns: DashboardChartColumns,
});

export const DashboardBlockSchema = z.discriminatedUnion("type", [
    DashboardTextBlockSchema,
    DashboardSpacerBlockSchema,
    DashboardTableBlockSchema,
    DashboardSingleStringBlockSchema,
    DashboardBarChartBlockSchema,
]);
