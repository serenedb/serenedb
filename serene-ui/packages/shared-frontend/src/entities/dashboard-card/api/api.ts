import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { DashboardSchema } from "@serene-ui/shared-core";
import { orpc } from "../../../shared/api/orpc";
import {
    DASHBOARDS_QUERY_KEY,
    dashboardQueryKey,
    useGetDashboard,
} from "../../dashboard";
import type { DashboardCardSchema } from "../model";
import {
    AddDashboardCardInput,
    UpdateDashboardCardInput,
    toDashboardCardUpdateInput,
} from "../model";

type ReplaceDashboardCardsInput = {
    dashboardId: number;
    cards: UpdateDashboardCardInput[];
};

type AddDashboardCardMutationInput = {
    dashboardId: number;
    card: AddDashboardCardInput;
    index?: number;
};

type UpdateDashboardCardMutationInput = {
    dashboardId: number;
    card: UpdateDashboardCardInput & {
        id: number;
    };
};

type DeleteDashboardCardMutationInput = {
    dashboardId: number;
    cardId: number;
};

type DashboardCacheSnapshot = {
    dashboard?: DashboardSchema;
    dashboards?: DashboardSchema[];
};

const getDashboardCacheSnapshot = (
    queryClient: ReturnType<typeof useQueryClient>,
    dashboardId: number,
) => {
    return {
        dashboard: queryClient.getQueryData<DashboardSchema>(
            dashboardQueryKey(dashboardId),
        ),
        dashboards: queryClient.getQueryData<DashboardSchema[]>(
            DASHBOARDS_QUERY_KEY,
        ),
    };
};

const restoreDashboardCacheSnapshot = (
    queryClient: ReturnType<typeof useQueryClient>,
    dashboardId: number,
    snapshot?: DashboardCacheSnapshot,
) => {
    if (!snapshot) {
        return;
    }

    queryClient.setQueryData(dashboardQueryKey(dashboardId), snapshot.dashboard);
    queryClient.setQueryData(DASHBOARDS_QUERY_KEY, snapshot.dashboards);
};

const setDashboardCaches = (
    queryClient: ReturnType<typeof useQueryClient>,
    dashboard: DashboardSchema,
) => {
    queryClient.setQueryData(dashboardQueryKey(dashboard.id), dashboard);
    queryClient.setQueryData<DashboardSchema[] | undefined>(
        DASHBOARDS_QUERY_KEY,
        (currentDashboards) =>
            currentDashboards?.map((currentDashboard) =>
                currentDashboard.id === dashboard.id
                    ? dashboard
                    : currentDashboard,
            ) ?? currentDashboards,
    );
};

const updateDashboardCaches = (
    queryClient: ReturnType<typeof useQueryClient>,
    dashboardId: number,
    updater: (dashboard: DashboardSchema) => DashboardSchema,
) => {
    queryClient.setQueryData<DashboardSchema | undefined>(
        dashboardQueryKey(dashboardId),
        (currentDashboard) =>
            currentDashboard ? updater(currentDashboard) : currentDashboard,
    );
    queryClient.setQueryData<DashboardSchema[] | undefined>(
        DASHBOARDS_QUERY_KEY,
        (currentDashboards) =>
            currentDashboards?.map((currentDashboard) =>
                currentDashboard.id === dashboardId
                    ? updater(currentDashboard)
                    : currentDashboard,
            ) ?? currentDashboards,
    );
};

const loadDashboardCards = async (dashboardId: number) => {
    const dashboard = await orpc.dashboard.get.call({
        id: dashboardId,
    });

    return dashboard.blocks.map(toDashboardCardUpdateInput);
};

export const useDashboardCards = (dashboardId: number) => {
    const dashboardQuery = useGetDashboard(dashboardId);

    return {
        ...dashboardQuery,
        data: dashboardQuery.data?.blocks ?? [],
    };
};

export const useReplaceDashboardCards = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async ({ dashboardId, cards }: ReplaceDashboardCardsInput) => {
            return await orpc.dashboard.update.call({
                id: dashboardId,
                blocks: cards,
            });
        },
        onSuccess: async (dashboard) => {
            setDashboardCaches(queryClient, dashboard);
        },
    });
};

export const useAddDashboardCard = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async ({
            dashboardId,
            card,
            index,
        }: AddDashboardCardMutationInput) => {
            const cards = await loadDashboardCards(dashboardId);
            const insertIndex =
                index === undefined
                    ? cards.length
                    : Math.min(Math.max(index, 0), cards.length);

            cards.splice(insertIndex, 0, card);

            return await orpc.dashboard.update.call({
                id: dashboardId,
                blocks: cards,
            });
        },
        onMutate: async ({ dashboardId, card, index }) => {
            await queryClient.cancelQueries({
                queryKey: DASHBOARDS_QUERY_KEY,
            });
            await queryClient.cancelQueries({
                queryKey: dashboardQueryKey(dashboardId),
            });

            const snapshot = getDashboardCacheSnapshot(queryClient, dashboardId);
            const optimisticCard = {
                ...card,
                id: -Date.now(),
                dashboard_id: dashboardId,
            } as DashboardCardSchema;

            updateDashboardCaches(queryClient, dashboardId, (dashboard) => {
                const nextBlocks = [...dashboard.blocks];
                const insertIndex =
                    index === undefined
                        ? nextBlocks.length
                        : Math.min(Math.max(index, 0), nextBlocks.length);

                nextBlocks.splice(insertIndex, 0, optimisticCard);

                return {
                    ...dashboard,
                    blocks: nextBlocks,
                };
            });

            return {
                dashboardId,
                snapshot,
            };
        },
        onError: (_error, _variables, context) => {
            restoreDashboardCacheSnapshot(
                queryClient,
                context?.dashboardId ?? 0,
                context?.snapshot,
            );
        },
        onSuccess: async (dashboard) => {
            setDashboardCaches(queryClient, dashboard);
        },
    });
};

export const useUpdateDashboardCard = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async ({
            dashboardId,
            card,
        }: UpdateDashboardCardMutationInput) => {
            const cards = await loadDashboardCards(dashboardId);
            const cardIndex = cards.findIndex(
                (currentCard) => currentCard.id === card.id,
            );

            if (cardIndex === -1) {
                throw new Error(`Dashboard card ${card.id} not found`);
            }

            cards[cardIndex] = card;

            return await orpc.dashboard.update.call({
                id: dashboardId,
                blocks: cards,
            });
        },
        onMutate: async ({ dashboardId, card }) => {
            await queryClient.cancelQueries({
                queryKey: DASHBOARDS_QUERY_KEY,
            });
            await queryClient.cancelQueries({
                queryKey: dashboardQueryKey(dashboardId),
            });

            const snapshot = getDashboardCacheSnapshot(queryClient, dashboardId);

            updateDashboardCaches(queryClient, dashboardId, (dashboard) => ({
                ...dashboard,
                blocks: dashboard.blocks.map((currentCard) =>
                    currentCard.id === card.id
                        ? ({
                              ...card,
                              dashboard_id: dashboardId,
                          } as DashboardCardSchema)
                        : currentCard,
                ),
            }));

            return {
                dashboardId,
                snapshot,
            };
        },
        onError: (_error, _variables, context) => {
            restoreDashboardCacheSnapshot(
                queryClient,
                context?.dashboardId ?? 0,
                context?.snapshot,
            );
        },
        onSuccess: async (dashboard) => {
            setDashboardCaches(queryClient, dashboard);
        },
    });
};

export const useDeleteDashboardCard = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: async ({
            dashboardId,
            cardId,
        }: DeleteDashboardCardMutationInput) => {
            const cards = await loadDashboardCards(dashboardId);
            const nextCards = cards.filter((card) => card.id !== cardId);

            return await orpc.dashboard.update.call({
                id: dashboardId,
                blocks: nextCards,
            });
        },
        onSuccess: async (dashboard) => {
            setDashboardCaches(queryClient, dashboard);
        },
    });
};
