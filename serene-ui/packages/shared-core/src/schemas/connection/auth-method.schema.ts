import { z } from "zod";

export const PasswordAuthSchema = z.object({
    authMethod: z.literal("password"),
    user: z.string().min(1).max(20).optional().nullable(),
    password: z.string().max(20).optional().nullable(),
});
export type PasswordAuthSchema = z.infer<typeof PasswordAuthSchema>;

export const AuthMethodSchema = z.discriminatedUnion("authMethod", [
    PasswordAuthSchema,
]);
export type AuthMethodSchema = z.infer<typeof AuthMethodSchema>;
