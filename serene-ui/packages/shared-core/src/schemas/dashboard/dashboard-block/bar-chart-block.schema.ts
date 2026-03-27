import z from "zod";
import { DashboardQueryBlockBaseSchema } from "./base.schema";
import { DashboardChartSeriesSchema } from "./chart.schema";

export const DashboardBarChartBlockSchema =
    DashboardQueryBlockBaseSchema.extend({
        type: z.literal("bar_chart"),
        variant: z.enum(["interactive", "vertical", "horizontal"]),
        category_key: z.string().min(1).max(255),
        default_active_key: z.string().min(1).max(255).optional(),
        value_label: z.string().max(255).default("Value"),
        is_stacked: z.boolean().default(false),
        series: z.array(DashboardChartSeriesSchema).min(1),
    });
export type DashboardBarChartBlockSchema = z.infer<
    typeof DashboardBarChartBlockSchema
>;

export const DashboardBarChartBlockInputSchema =
    DashboardBarChartBlockSchema.omit({
        id: true,
        dashboard_id: true,
    });
export type DashboardBarChartBlockInputSchema = z.infer<
    typeof DashboardBarChartBlockInputSchema
>;

export const DashboardBarChartBlockUpdateSchema =
    DashboardBarChartBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardBarChartBlockUpdateSchema = z.infer<
    typeof DashboardBarChartBlockUpdateSchema
>;
