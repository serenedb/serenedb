import z from "zod";
import { DashboardBlockBaseSchema } from "./base.schema";

export const DashboardSpacerBlockSchema = DashboardBlockBaseSchema.extend({
    type: z.literal("spacer"),
});
export type DashboardSpacerBlockSchema = z.infer<
    typeof DashboardSpacerBlockSchema
>;

export const DashboardSpacerBlockInputSchema = DashboardSpacerBlockSchema.omit({
    id: true,
    dashboard_id: true,
});
export type DashboardSpacerBlockInputSchema = z.infer<
    typeof DashboardSpacerBlockInputSchema
>;

export const DashboardSpacerBlockUpdateSchema =
    DashboardSpacerBlockInputSchema.extend({
        id: z.number().optional(),
    });
export type DashboardSpacerBlockUpdateSchema = z.infer<
    typeof DashboardSpacerBlockUpdateSchema
>;
