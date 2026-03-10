import z from "zod";

export const DashboardSchema = z.object({
    id: z.number(),
    name: z.string().max(255),
    auto_refresh: z.boolean().default(false),
    refresh_interval: z.number().default(60),
    row_limit: z.number().default(1000),
    created_at: z.string().datetime().optional(),
    updated_at: z.string().datetime().optional(),
});
