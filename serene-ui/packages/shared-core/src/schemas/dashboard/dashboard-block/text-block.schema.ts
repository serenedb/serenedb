import z from "zod";
import { DashboardBlockBaseSchema } from "./base.schema";

export const DashboardTextBlockSchema = DashboardBlockBaseSchema.extend({
    type: z.literal("text"),
    text: z.string().max(10000),
});
export type DashboardTextBlockSchema = z.infer<typeof DashboardTextBlockSchema>;

export const DashboardTextBlockInputSchema = DashboardTextBlockSchema.omit({
    id: true,
    dashboard_id: true,
});
export type DashboardTextBlockInputSchema = z.infer<
    typeof DashboardTextBlockInputSchema
>;

export const DashboardTextBlockUpdateSchema =
    DashboardTextBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardTextBlockUpdateSchema = z.infer<
    typeof DashboardTextBlockUpdateSchema
>;
