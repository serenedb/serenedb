import { z } from "zod";
import { BindVarSchema } from "../bind-var";

export const SavedQuerySchema = z.object({
    id: z.number(),
    name: z.string().max(255),
    query: z.string().max(10000),
    bind_vars: z.array(BindVarSchema).optional(),
    usage_count: z.number().default(0),
});
export type SavedQuerySchema = z.infer<typeof SavedQuerySchema>;
