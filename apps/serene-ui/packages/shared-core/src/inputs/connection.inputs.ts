import {
    PostgresHostPasswordSchema,
    PostgresSocketPasswordSchema,
} from "../schemas/connection/postgres-connection.schema";
import z from "zod";

export const AddConnectionInput = z.union([
    PostgresHostPasswordSchema.omit({ id: true }),
    PostgresSocketPasswordSchema.omit({ id: true }),
]);
export type AddConnectionInput = z.infer<typeof AddConnectionInput>;

export const UpdateConnectionInput = z.union([
    PostgresHostPasswordSchema.partial().required({ id: true }),
    PostgresSocketPasswordSchema.partial().required({ id: true }),
]);
export type UpdateConnectionInput = z.infer<typeof UpdateConnectionInput>;

export const DeleteConnectionInput = z.object({ id: z.number() });
export type DeleteConnectionInput = z.infer<typeof DeleteConnectionInput>;
