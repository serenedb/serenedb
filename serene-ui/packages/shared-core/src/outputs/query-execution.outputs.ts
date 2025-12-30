import { QueryExecutionJobSchema } from "../schemas";
import z from "zod";

export const ExecuteQueryOutput = z.object({
    jobId: z.number().optional(),
    result: z.array(z.record(z.string(), z.any())).optional(),
});
export type ExecuteQueryOutput = z.infer<typeof ExecuteQueryOutput>;

export const SubscribeQueryExecutionOutput = QueryExecutionJobSchema;
export type SubscribeQueryExecutionOutput = z.infer<
    typeof SubscribeQueryExecutionOutput
>;
