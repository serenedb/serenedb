import { QueryHistoryItemSchema } from "@serene-ui/shared-core";

export type IAddQueryHistory = Omit<QueryHistoryItemSchema, "id">;
export type IDeleteQueryHistory = {
    id: number;
};
