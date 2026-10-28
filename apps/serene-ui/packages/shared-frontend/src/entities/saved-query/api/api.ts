import {
    useQuery,
    useMutation,
    UseQueryOptions,
    useQueryClient,
} from "@tanstack/react-query";
import { ListMySavedQueriesOutput } from "@serene-ui/shared-core";
import { orpc } from "../../../shared/api/orpc";

export const useGetSavedQueries = (
    props?: Partial<UseQueryOptions<ListMySavedQueriesOutput, Error>>,
) => {
    return useQuery(
        orpc.savedQuery.listMy.queryOptions({
            queryKey: ["saved-queries"],
            ...props,
        }),
    );
};

export const useAddSavedQuery = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.savedQuery.add.mutationOptions({
            onSuccess: () => {
                queryClient.invalidateQueries({ queryKey: ["saved-queries"] });
            },
        }),
    );
};

export const useUpdateSavedQuery = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.savedQuery.update.mutationOptions({
            onSuccess: () => {
                queryClient.invalidateQueries({ queryKey: ["saved-queries"] });
            },
        }),
    );
};

export const useDeleteSavedQuery = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.savedQuery.delete.mutationOptions({
            onSuccess: () => {
                queryClient.invalidateQueries({ queryKey: ["saved-queries"] });
            },
        }),
    );
};
