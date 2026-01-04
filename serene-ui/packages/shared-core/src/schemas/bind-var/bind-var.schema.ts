import z from "zod";

export const BindVarSchema = z.object({
    name: z.string().min(1).max(255),
    default_value: z.string().max(255).optional(),
    description: z.string().max(1024).optional(),
    value: z.string().max(255).optional(),
});
export type BindVarSchema = z.infer<typeof BindVarSchema>;
