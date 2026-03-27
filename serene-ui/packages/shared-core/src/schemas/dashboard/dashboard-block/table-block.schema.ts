import z from "zod";
import { DashboardQueryBlockBaseSchema } from "./base.schema";

export const DashboardTableBlockSchema = DashboardQueryBlockBaseSchema.extend({
    type: z.literal("table"),
    columns: z.array(z.string().min(1).max(255)).optional(),
});
export type DashboardTableBlockSchema = z.infer<typeof DashboardTableBlockSchema>;

export const DashboardTableBlockInputSchema = DashboardTableBlockSchema.omit({
    id: true,
    dashboard_id: true,
});
export type DashboardTableBlockInputSchema = z.infer<
    typeof DashboardTableBlockInputSchema
>;

export const DashboardTableBlockUpdateSchema =
    DashboardTableBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardTableBlockUpdateSchema = z.infer<
    typeof DashboardTableBlockUpdateSchema
>;
