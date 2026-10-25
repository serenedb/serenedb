import { z } from "zod";

export const BaseConnectionSchema = z.object({
    id: z.number(),
    name: z.string().max(255),
});
export type BaseConnectionSchema = z.infer<typeof BaseConnectionSchema>;
