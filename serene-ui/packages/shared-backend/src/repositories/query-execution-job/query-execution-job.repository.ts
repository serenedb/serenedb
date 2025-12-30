import { QueryExecutionJobSchema } from "@serene-ui/shared-core";
import { DBClient } from "../../database";
import {
    buildDelete,
    buildInsert,
    buildSelect,
    buildUpdate,
} from "../../utils/request-builder";
import { jobToRow, rowToJob } from "./query-execution-job.mappers";
import { QueryExecutionJobRow } from "./query-execution-job.types";

export const QueryExecutionJobRepository = {
    findById: (id: number): QueryExecutionJobSchema | null => {
        const { sql, values } = buildSelect("jobs", {
            where: { id },
        });
        const row = DBClient.prepare(sql).get(...values);
        return row ? rowToJob(row) : null;
    },

    findOne: (
        query?: Partial<QueryExecutionJobSchema>,
    ): QueryExecutionJobSchema | null => {
        const { sql, values } = buildSelect("jobs", {
            where: query,
            limit: 1,
        });
        const row = DBClient.prepare(sql).get(...values);
        return row ? rowToJob(row) : null;
    },

    findMany: (
        query?: Partial<QueryExecutionJobSchema>,
    ): QueryExecutionJobSchema[] => {
        const { sql, values } = buildSelect("jobs", {
            where: query,
        });
        const rows = DBClient.prepare(sql).all(...values);
        return rows.map((row) => rowToJob(row as QueryExecutionJobRow));
    },

    create: (
        job: Omit<QueryExecutionJobSchema, "id">,
    ): QueryExecutionJobSchema | null => {
        const jobRow = jobToRow(job);

        const { sql, values } = buildInsert("jobs", jobRow);
        const result = DBClient.prepare(sql).run(...values);

        const { sql: selectSql, values: selectValues } = buildSelect("jobs", {
            where: { id: result.lastInsertRowid as number },
        });
        const row = DBClient.prepare(selectSql).get(...selectValues);
        return row ? rowToJob(row) : null;
    },

    update: (
        id: string,
        updates: Partial<QueryExecutionJobRow>,
    ): QueryExecutionJobRow | null => {
        if (!Object.keys(updates).length) return null;

        const { sql, values } = buildUpdate("jobs", updates, {
            where: { id },
        });
        DBClient.prepare(sql).run(...values);

        const { sql: selectSql, values: selectValues } = buildSelect("jobs", {
            where: { id },
        });
        const row = DBClient.prepare(selectSql).get(...selectValues);
        return row ? rowToJob(row) : null;
    },

    delete: (id: string): QueryExecutionJobRow | null => {
        const { sql: selectSql, values: selectValues } = buildSelect("jobs", {
            where: { id },
        });
        const row = DBClient.prepare(selectSql).get(...selectValues);
        if (!row) return null;

        const { sql, values } = buildDelete("jobs", {
            where: { id },
        });
        DBClient.prepare(sql).run(...values);
        return rowToJob(row);
    },
};
