import z from "zod";
import { DashboardBlockSchema } from "./dashboard-block.schema";

export const DashboardBaseSchema = z.object({
    id: z.number(),
    name: z.string().max(255),
    auto_refresh: z.boolean().default(false),
    refresh_interval: z.number().default(60),
    row_limit: z.number().default(1000),
    created_at: z.string().datetime().optional(),
    updated_at: z.string().datetime().optional(),
});
export type DashboardBaseSchema = z.infer<typeof DashboardBaseSchema>;

export const DashboardSchema = DashboardBaseSchema.extend({
    blocks: z.array(DashboardBlockSchema).default([]),
});
export type DashboardSchema = z.infer<typeof DashboardSchema>;
