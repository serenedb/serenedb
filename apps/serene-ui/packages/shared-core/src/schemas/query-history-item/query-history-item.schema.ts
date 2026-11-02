import { z } from "zod";
import { BindVarSchema } from "../bind-var";

export const QueryHistoryItemSchema = z.object({
    id: z.number(),
    name: z.string().max(255),
    query: z.string().max(10000),
    bind_vars: z.array(BindVarSchema).optional(),
    executed_at: z.iso.datetime(),
});

export type QueryHistoryItemSchema = z.infer<typeof QueryHistoryItemSchema>;
