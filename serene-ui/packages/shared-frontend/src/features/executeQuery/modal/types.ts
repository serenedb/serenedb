import type { QueryExecutionJobSchema } from "@serene-ui/shared-core";

export interface StatementRange {
    startOffset: number;
    endOffset: number;
}

export type QueryResult = QueryExecutionJobSchema extends infer T
    ? T extends QueryExecutionJobSchema
        ? Omit<T, "id"> & {
              jobId: number;
              statementIndex?: number;
              statementRange?: StatementRange;
              statementQuery?: string;
              sourceQuery?: string;
          }
        : never
    : never;
