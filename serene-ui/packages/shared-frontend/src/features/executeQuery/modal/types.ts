import type { QueryExecutionJobSchema } from "@serene-ui/shared-core";

export type QueryResult = QueryExecutionJobSchema extends infer T
    ? T extends QueryExecutionJobSchema
        ? Omit<T, "id"> & { jobId: number }
        : never
    : never;
