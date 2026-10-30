import { BindVarSchema } from "../schemas";
import { ConnectionSchema } from "../schemas/connection";
import {
    PostgresHostPasswordSchema,
    PostgresSocketPasswordSchema,
} from "../schemas/connection/postgres-connection.schema";
import z from "zod";

const BaseExecuteQuerySchema = z.object({
    async: z.boolean().optional(),
    query: z.string(),
    database: z.string().max(255).optional(),
    bind_vars: z.array(z.string()).optional(),
    limit: z.number().min(1).optional(),
    user: z.string().max(255).optional(),
    password: z.string().max(255).optional(),
    host: z.string().max(255).optional(),
    socket: z.string().optional(),
    port: z.number().max(65535).optional(),
});

const ExecuteQueryWithConnectionIdSchema = BaseExecuteQuerySchema.extend({
    connectionId: z.number().optional(),
});

const ExecuteQueryWithConnectionHostSchema = BaseExecuteQuerySchema.merge(
    PostgresHostPasswordSchema.omit({
        id: true,
        name: true,
        database: true,
    }),
).extend({
    connectionId: z.literal(undefined),
});

const ExecuteQueryWithConnectionSocketSchema = BaseExecuteQuerySchema.merge(
    PostgresSocketPasswordSchema.omit({
        id: true,
        name: true,
        database: true,
    }),
).extend({
    connectionId: z.literal(undefined),
});

export const ExecuteQueryInput = z.union([
    ExecuteQueryWithConnectionIdSchema,
    ExecuteQueryWithConnectionHostSchema,
    ExecuteQueryWithConnectionSocketSchema,
]);
export type ExecuteQueryInput = z.infer<typeof ExecuteQueryInput>;

export const SubscribeQueryExecutionInput = z.object({
    jobId: z.number(),
});
export type SubscribeQueryExecutionInput = z.infer<
    typeof SubscribeQueryExecutionInput
>;
