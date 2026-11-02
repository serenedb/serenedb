import {
    useQuery,
    useMutation,
    UseQueryOptions,
    useQueryClient,
} from "@tanstack/react-query";
import type { IAddQueryHistory, IDeleteQueryHistory } from "./dto";
import { BaseApiResponse } from "../../../shared/api/api-client";
import { QueryHistoryItemSchema } from "@serene-ui/shared-core";

export const useGetQueryHistory = (
    props?: Partial<UseQueryOptions<QueryHistoryItemSchema[], Error>>,
) => {
    return useQuery<QueryHistoryItemSchema[]>({
        queryKey: ["query-history"],
        queryFn: async () => {
            const data = localStorage.getItem("system:query-history");
            if (!data) {
                return [];
            }
            return await JSON.parse(data).sort(
                (a: QueryHistoryItemSchema, b: QueryHistoryItemSchema) =>
                    new Date(b.executed_at).getTime() -
                    new Date(a.executed_at).getTime(),
            );
        },
        ...props,
    });
};

export const useAddQueryHistory = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async (data: IAddQueryHistory) => {
            const history = localStorage.getItem("system:query-history");
            if (!history) {
                localStorage.setItem(
                    "system:query-history",
                    JSON.stringify([data]),
                );
                return data;
            }
            const parsedHistory = JSON.parse(history);
            localStorage.setItem(
                "system:query-history",
                JSON.stringify([...parsedHistory, data]),
            );
            return data;
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["query-history"] });
        },
    });
};
export const useDeleteQueryHistory = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async (data: IDeleteQueryHistory) => {
            const history = localStorage.getItem("system:query-history");
            if (!history) {
                return data;
            }
            const parsedHistory = JSON.parse(history);
            localStorage.setItem(
                "system:query-history",
                JSON.stringify(
                    parsedHistory.filter(
                        (item: QueryHistoryItemSchema) => item.id !== data.id,
                    ),
                ),
            );
            return data;
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["query-history"] });
        },
    });
};

export const useClearQueryHistory = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async () => {
            localStorage.removeItem("system:query-history");
            return [];
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["query-history"] });
        },
    });
};
