import { QueryExecutionJobSchema } from "../schemas";
import z from "zod";
import { QueryExecutionResultSchema } from "../schemas/query-execution-job/query-execution-job.schema";

export const ExecuteQueryOutput = z.object({
    jobId: z.number().optional(),
    results: z.array(QueryExecutionResultSchema).optional(),
});
export type ExecuteQueryOutput = z.infer<typeof ExecuteQueryOutput>;

export const SubscribeQueryExecutionOutput = QueryExecutionJobSchema;
export type SubscribeQueryExecutionOutput = z.infer<
    typeof SubscribeQueryExecutionOutput
>;
