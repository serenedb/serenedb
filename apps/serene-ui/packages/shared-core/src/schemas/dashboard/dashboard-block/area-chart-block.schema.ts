import z from "zod";
import { DashboardQueryBlockBaseSchema } from "./base.schema";
import {
    DashboardChartSeriesSchema,
    DashboardLineTypeSchema,
} from "./chart.schema";

export const DashboardAreaChartBlockSchema =
    DashboardQueryBlockBaseSchema.extend({
        type: z.literal("area_chart"),
        variant: z.enum(["interactive", "default"]).default("default"),
        x_axis_key: z.string().min(1).max(255),
        default_active_key: z.string().min(1).max(255).optional(),
        value_label: z.string().max(255).default("Value"),
        line_type: DashboardLineTypeSchema.default("natural"),
        series: z.array(DashboardChartSeriesSchema).min(1),
    });
export type DashboardAreaChartBlockSchema = z.infer<
    typeof DashboardAreaChartBlockSchema
>;

export const DashboardAreaChartBlockInputSchema =
    DashboardAreaChartBlockSchema.omit({
        id: true,
        dashboard_id: true,
    });
export type DashboardAreaChartBlockInputSchema = z.infer<
    typeof DashboardAreaChartBlockInputSchema
>;

export const DashboardAreaChartBlockUpdateSchema =
    DashboardAreaChartBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardAreaChartBlockUpdateSchema = z.infer<
    typeof DashboardAreaChartBlockUpdateSchema
>;
