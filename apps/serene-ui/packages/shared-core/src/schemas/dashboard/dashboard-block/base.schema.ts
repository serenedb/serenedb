import z from "zod";

export const DashboardBlockBoundsSchema = z.object({
    x: z.number(),
    y: z.number(),
    width: z.number(),
    height: z.number(),
});
export type DashboardBlockBoundsSchema = z.infer<
    typeof DashboardBlockBoundsSchema
>;

export const DashboardBlockBaseSchema = z.object({
    id: z.number(),
    dashboard_id: z.number(),
    bounds: DashboardBlockBoundsSchema,
});
export type DashboardBlockBaseSchema = z.infer<typeof DashboardBlockBaseSchema>;

export const DashboardQueryBlockBaseSchema = DashboardBlockBaseSchema.extend({
    connection_id: z.number().optional(),
    database: z.string().max(255).optional(),
    query: z.string().max(10000),
    name: z.string().max(255).optional().default("No name"),
    description: z.string().max(1024).optional(),
    custom_refresh_interval_enabled: z.boolean().default(false),
    custom_refresh_interval: z.number().default(60),
    custom_row_limit_enabled: z.boolean().default(false),
    custom_row_limit: z.number().default(1000),
});
export type DashboardQueryBlockBaseSchema = z.infer<
    typeof DashboardQueryBlockBaseSchema
>;
