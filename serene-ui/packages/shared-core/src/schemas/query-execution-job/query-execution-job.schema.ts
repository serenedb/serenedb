import { z } from "zod";
import { BindVarSchema } from "../bind-var";

const BaseQueryExecutionJobSchema = z.object({
    id: z.number(),
    query: z.string(),
    bind_vars: z.array(z.string()).optional(),
    created_at: z.string().datetime().optional(),
    execution_started_at: z.string().datetime().optional(),
    execution_finished_at: z.string().datetime().optional(),
});

const PendingQueryExecutionJobSchema = BaseQueryExecutionJobSchema.extend({
    status: z.literal("pending"),
});

const RunningQueryExecutionJobSchema = BaseQueryExecutionJobSchema.extend({
    status: z.literal("running"),
});

const SuccessQueryExecutionJobSchema = BaseQueryExecutionJobSchema.extend({
    status: z.literal("success"),
    action_type: z
        .enum(["SELECT", "INSERT", "UPDATE", "DELETE", "OTHER"])
        .optional(),
    result: z.array(z.record(z.string(), z.any())),
});

const FailedQueryExecutionJobSchema = BaseQueryExecutionJobSchema.extend({
    status: z.literal("failed"),
    action_type: z
        .enum(["SELECT", "INSERT", "UPDATE", "DELETE", "OTHER"])
        .optional(),
    error: z.string(),
});

export const QueryExecutionJobSchema = z.discriminatedUnion("status", [
    PendingQueryExecutionJobSchema,
    RunningQueryExecutionJobSchema,
    SuccessQueryExecutionJobSchema,
    FailedQueryExecutionJobSchema,
]);
export type QueryExecutionJobSchema = z.infer<typeof QueryExecutionJobSchema>;
