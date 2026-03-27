import {
    UseQueryOptions,
    useMutation,
    useQuery,
    useQueryClient,
} from "@tanstack/react-query";
import { DashboardSchema } from "@serene-ui/shared-core";
import { orpc } from "../../../shared/api/orpc";

export const DASHBOARDS_QUERY_KEY = ["dashboards"] as const;
export const FAVORITE_DASHBOARDS_QUERY_KEY = ["dashboards", "favorites"] as const;
export const dashboardQueryKey = (dashboardId: number) =>
    ["dashboard", dashboardId] as const;

const setDashboardCaches = (
    queryClient: ReturnType<typeof useQueryClient>,
    dashboard: DashboardSchema,
) => {
    queryClient.setQueryData(dashboardQueryKey(dashboard.id), dashboard);
    queryClient.setQueryData<DashboardSchema[] | undefined>(
        DASHBOARDS_QUERY_KEY,
        (currentDashboards) => {
            if (!currentDashboards) {
                return [dashboard];
            }

            const hasDashboard = currentDashboards.some(
                (currentDashboard) => currentDashboard.id === dashboard.id,
            );

            if (!hasDashboard) {
                return [dashboard, ...currentDashboards];
            }

            return currentDashboards.map((currentDashboard) =>
                currentDashboard.id === dashboard.id
                    ? dashboard
                    : currentDashboard,
            );
        },
    );
    queryClient.setQueryData<DashboardSchema[] | undefined>(
        FAVORITE_DASHBOARDS_QUERY_KEY,
        (currentDashboards) => {
            if (!dashboard.favorite) {
                return (currentDashboards ?? []).filter(
                    (currentDashboard) => currentDashboard.id !== dashboard.id,
                );
            }

            if (!currentDashboards) {
                return [dashboard];
            }

            const hasDashboard = currentDashboards.some(
                (currentDashboard) => currentDashboard.id === dashboard.id,
            );

            if (!hasDashboard) {
                return [dashboard, ...currentDashboards];
            }

            return currentDashboards.map((currentDashboard) =>
                currentDashboard.id === dashboard.id
                    ? dashboard
                    : currentDashboard,
            );
        },
    );
};

export const useGetDashboards = (
    props?: Partial<UseQueryOptions<DashboardSchema[], Error>>,
) => {
    return useQuery(
        orpc.dashboard.listMy.queryOptions({
            queryKey: DASHBOARDS_QUERY_KEY,
            ...props,
        }),
    );
};

export const useGetFavoriteDashboards = (
    props?: Partial<UseQueryOptions<DashboardSchema[], Error>>,
) => {
    return useQuery(
        orpc.dashboard.favorites.queryOptions({
            queryKey: FAVORITE_DASHBOARDS_QUERY_KEY,
            ...props,
        }),
    );
};

export const useGetDashboard = (
    dashboardId: number,
    props?: Partial<UseQueryOptions<DashboardSchema, Error>>,
) => {
    return useQuery<DashboardSchema, Error>({
        queryKey: dashboardQueryKey(dashboardId),
        queryFn: async () => {
            return await orpc.dashboard.get.call({
                id: dashboardId,
            });
        },
        enabled: dashboardId > 0,
        ...props,
    });
};

export const useAddDashboard = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.dashboard.add.mutationOptions({
            onSuccess: (dashboard) => {
                setDashboardCaches(queryClient, dashboard);
            },
        }),
    );
};

export const useUpdateDashboard = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.dashboard.update.mutationOptions({
            onSuccess: async (dashboard) => {
                setDashboardCaches(queryClient, dashboard);
            },
        }),
    );
};

export const useDeleteDashboard = () => {
    const queryClient = useQueryClient();

    return useMutation(
        orpc.dashboard.delete.mutationOptions({
            onSuccess: async (dashboard) => {
                await queryClient.invalidateQueries({
                    queryKey: DASHBOARDS_QUERY_KEY,
                });
                await queryClient.invalidateQueries({
                    queryKey: FAVORITE_DASHBOARDS_QUERY_KEY,
                });
                await queryClient.removeQueries({
                    queryKey: dashboardQueryKey(dashboard.id),
                });
            },
        }),
    );
};
