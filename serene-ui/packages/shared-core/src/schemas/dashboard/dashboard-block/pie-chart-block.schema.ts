import z from "zod";
import { DashboardQueryBlockBaseSchema } from "./base.schema";

export const DashboardPieChartBlockSchema =
    DashboardQueryBlockBaseSchema.extend({
        type: z.literal("pie_chart"),
        interactive: z.boolean().default(false),
        variant: z.enum(["pie", "donut"]).default("pie"),
        name_key: z.string().min(1).max(255),
        value_key: z.string().min(1).max(255),
        default_active_key: z.string().min(1).max(255).optional(),
        value_label: z.string().max(255).default("Value"),
        color_key: z.string().max(255).default("fill"),
        show_labels: z.boolean().default(false),
        show_center_label: z.boolean().default(false),
        center_value: z.union([z.string(), z.number()]).optional(),
        center_label: z.string().max(255).optional(),
    });
export type DashboardPieChartBlockSchema = z.infer<
    typeof DashboardPieChartBlockSchema
>;

export const DashboardPieChartBlockInputSchema =
    DashboardPieChartBlockSchema.omit({
        id: true,
        dashboard_id: true,
    });
export type DashboardPieChartBlockInputSchema = z.infer<
    typeof DashboardPieChartBlockInputSchema
>;

export const DashboardPieChartBlockUpdateSchema =
    DashboardPieChartBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardPieChartBlockUpdateSchema = z.infer<
    typeof DashboardPieChartBlockUpdateSchema
>;
