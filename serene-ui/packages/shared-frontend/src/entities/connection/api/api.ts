import {
    useQuery,
    useMutation,
    UseQueryOptions,
    useQueryClient,
} from "@tanstack/react-query";
import { ConnectionSchema } from "@serene-ui/shared-core";
import { orpc } from "../../../shared/api/orpc";

export const useGetConnections = (
    props?: Partial<UseQueryOptions<ConnectionSchema[], Error>>,
) => {
    return useQuery(
        orpc.connection.listMy.queryOptions({
            queryKey: ["connections"],
            ...props,
        }),
    );
};

export const useAddConnection = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.connection.add.mutationOptions({
            onSuccess: () => {
                queryClient.invalidateQueries({ queryKey: ["connections"] });
            },
        }),
    );
};

export const useUpdateConnection = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.connection.update.mutationOptions({
            onSuccess: () => {
                queryClient.invalidateQueries({ queryKey: ["connections"] });
            },
        }),
    );
};

export const useDeleteConnection = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.connection.delete.mutationOptions({
            onSuccess: () => {
                queryClient.invalidateQueries({ queryKey: ["connections"] });
            },
        }),
    );
};
