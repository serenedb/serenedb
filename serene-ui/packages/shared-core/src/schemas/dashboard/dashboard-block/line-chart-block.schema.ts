import z from "zod";
import { DashboardQueryBlockBaseSchema } from "./base.schema";
import {
    DashboardChartSeriesSchema,
    DashboardLineTypeSchema,
} from "./chart.schema";

export const DashboardLineChartBlockSchema =
    DashboardQueryBlockBaseSchema.extend({
        type: z.literal("line_chart"),
        variant: z.enum(["interactive", "default"]).default("default"),
        x_axis_key: z.string().min(1).max(255),
        default_active_key: z.string().min(1).max(255).optional(),
        value_label: z.string().max(255).default("Value"),
        line_type: DashboardLineTypeSchema.default("natural"),
        series: z.array(DashboardChartSeriesSchema).min(1),
    });
export type DashboardLineChartBlockSchema = z.infer<
    typeof DashboardLineChartBlockSchema
>;

export const DashboardLineChartBlockInputSchema =
    DashboardLineChartBlockSchema.omit({
        id: true,
        dashboard_id: true,
    });
export type DashboardLineChartBlockInputSchema = z.infer<
    typeof DashboardLineChartBlockInputSchema
>;

export const DashboardLineChartBlockUpdateSchema =
    DashboardLineChartBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardLineChartBlockUpdateSchema = z.infer<
    typeof DashboardLineChartBlockUpdateSchema
>;
