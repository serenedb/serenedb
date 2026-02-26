import { BindVarSchema } from "@serene-ui/shared-core";

export interface ConsoleTab {
    id: number;
    type: "query";
    value: string;
    bind_vars?: BindVarSchema[];
    results: {
        jobId: number;
        status: "running" | "pending" | "success" | "failed";
        rows: Record<string, any>[] | undefined;
        error?: string;
        message?: string;
        created_at?: string;
        execution_started_at?: string;
        execution_finished_at?: string;
    }[];
}
