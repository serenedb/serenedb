import z from "zod";
import { DashboardQueryBlockBaseSchema } from "./base.schema";

export const DashboardSingleStringBlockSchema =
    DashboardQueryBlockBaseSchema.extend({
        type: z.literal("single_string"),
        column: z.string().min(1).max(255),
        fallback_value: z.string().max(255).optional(),
    });
export type DashboardSingleStringBlockSchema = z.infer<
    typeof DashboardSingleStringBlockSchema
>;

export const DashboardSingleStringBlockInputSchema =
    DashboardSingleStringBlockSchema.omit({
        id: true,
        dashboard_id: true,
    });
export type DashboardSingleStringBlockInputSchema = z.infer<
    typeof DashboardSingleStringBlockInputSchema
>;

export const DashboardSingleStringBlockUpdateSchema =
    DashboardSingleStringBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardSingleStringBlockUpdateSchema = z.infer<
    typeof DashboardSingleStringBlockUpdateSchema
>;
