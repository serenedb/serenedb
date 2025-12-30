import { QueryExecutionJobSchema } from "@serene-ui/shared-core";
import { QueryExecutionJobRow } from "./query-execution-job.types";

/**
 * Converts a Job entity to a flat database row format.
 * Serializes result and bind_vars to JSON strings for database storage.
 */
export function jobToRow(
    job: Omit<QueryExecutionJobSchema, "id">,
): Omit<QueryExecutionJobRow, "id"> {
    const bindVars = job.bind_vars ? JSON.stringify(job.bind_vars) : null;
    const result = "result" in job ? JSON.stringify(job.result) : null;
    const error = "error" in job ? job.error : null;
    const action_type = "action_type" in job ? job.action_type : null;
    const createdAt = job.created_at ?? null;
    const startedAt = job.execution_started_at ?? null;
    const finishedAt = job.execution_finished_at ?? null;

    return {
        ...job,
        bind_vars: bindVars,
        result: result,
        error: error,
        action_type,
        created_at: createdAt,
        execution_started_at: startedAt,
        execution_finished_at: finishedAt,
    };
}

/**
 * Converts a flat database row to a Job entity.
 * Deserializes JSON strings back to objects/arrays.
 */
export function rowToJob(row: QueryExecutionJobRow): QueryExecutionJobSchema {
    const r = row as any;

    return {
        id: r.id,
        query: r.query,
        status: r.status,
        bind_vars: r.bind_vars ? JSON.parse(r.bind_vars.toString()) : undefined,
        result: r.result ? JSON.parse(r.result.toString()) : undefined,
        error: r.error ?? undefined,
        action_type: r.action_type ?? undefined,
        created_at: r.created_at ?? undefined,
        execution_started_at: r.execution_started_at ?? undefined,
        execution_finished_at: r.execution_finished_at ?? undefined,
    };
}
