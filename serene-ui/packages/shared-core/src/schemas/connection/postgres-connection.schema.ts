import { z } from "zod";
import { AuthMethodSchema, PasswordAuthSchema } from "./auth-method.schema";
import { BaseConnectionSchema } from "./base-connection.schema";
import {
    ConnectionModesSchema,
    HostModeSchema,
    SocketModeSchema,
} from "./connection-mode.schema";

export const PostgresBaseConnectionSchema = BaseConnectionSchema.extend({
    type: z.literal("postgres"),
    ssl: z.boolean(),
    database: z.string().max(255).optional(),
});

export const PostgresConnectionSchema = PostgresBaseConnectionSchema.and(
    AuthMethodSchema,
).and(ConnectionModesSchema);

export const PostgresHostPasswordSchema = PostgresBaseConnectionSchema.extend({
    ...PasswordAuthSchema.shape,
    ...HostModeSchema.shape,
});

export const PostgresSocketPasswordSchema = PostgresBaseConnectionSchema.extend(
    {
        ...PasswordAuthSchema.shape,
        ...SocketModeSchema.shape,
    },
);

export const PostgresConnectionUnion = PostgresHostPasswordSchema.or(
    PostgresSocketPasswordSchema,
);

export type PostgresConnectionSchema = z.infer<typeof PostgresConnectionUnion>;
