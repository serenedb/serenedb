import React, { MouseEvent, SyntheticEvent } from "react";
import { toast } from "sonner";
import { DeleteDashboardIconButton } from "../../../../features";
import { useUpdateDashboard } from "../../../../entities/dashboard";
import {
    Button,
    DashboardsIcon,
    getErrorMessage,
    Skeleton,
    StarIcon,
} from "../../../../shared";
import { useDashboardsSidebarContext } from "./DashboardsSidebarContext";

interface DashboardsSidebarDashboard {
    id: number;
    name: string;
    favorite: boolean;
}

interface DashboardsSidebarDashboardListProps {
    dashboards?: DashboardsSidebarDashboard[];
    isDataFetched: boolean;
    isDataLoading: boolean;
    emptyState: string;
    hasAdditionalItems?: boolean;
    additionalItems?: React.ReactNode;
}

export const DashboardsSidebarDashboardList: React.FC<
    DashboardsSidebarDashboardListProps
> = ({
    dashboards,
    isDataFetched,
    isDataLoading,
    emptyState,
    hasAdditionalItems = false,
    additionalItems,
}) => {
    const { onCurrentDashboardChange } = useDashboardsSidebarContext();
    const { mutateAsync: updateDashboard, isPending } = useUpdateDashboard();
    const [updatingDashboardId, setUpdatingDashboardId] = React.useState<
        number | null
    >(null);

    const hasDashboards = (dashboards?.length ?? 0) > 0;
    const hasAnyItems = hasDashboards || hasAdditionalItems;
    const isInitialLoading = !isDataFetched && isDataLoading && !hasAnyItems;

    const stopPropagation = (event: SyntheticEvent) => {
        event.stopPropagation();
    };

    const handleToggleFavorite = async (
        event: MouseEvent<HTMLButtonElement>,
        dashboardId: number,
        isFavorite: boolean,
    ) => {
        event.stopPropagation();
        setUpdatingDashboardId(dashboardId);

        try {
            await updateDashboard({
                id: dashboardId,
                favorite: !isFavorite,
            });
        } catch (error) {
            toast.error("Failed to update favorite", {
                description: getErrorMessage(
                    error,
                    "Failed to update favorite",
                ),
            });
        } finally {
            setUpdatingDashboardId(null);
        }
    };

    return (
        <div className="flex h-full min-h-0 flex-col">
            {isInitialLoading && !hasDashboards ? (
                <div className="flex flex-col gap-1.5 p-1">
                    <Skeleton className="h-7 w-full rounded-md" />
                    <Skeleton className="h-7 w-full rounded-md" />
                    <Skeleton className="h-7 w-full rounded-md" />
                </div>
            ) : null}

            {!hasAnyItems && isDataFetched && !isDataLoading ? (
                <div className="flex flex-1 items-center justify-center p-2">
                    <p className="text-center text-xs text-foreground/70">
                        {emptyState}
                    </p>
                </div>
            ) : null}

            {hasAnyItems ? (
                <div className="flex min-h-0 flex-1 flex-col overflow-auto">
                    {(dashboards ?? []).map((dashboard) => (
                        <div
                            key={dashboard.id}
                            className="group/explorer-node flex h-7 items-center gap-1 pl-4 pr-1 hover:bg-accent"
                            title={dashboard.name}
                            onClick={() =>
                                onCurrentDashboardChange(dashboard.id)
                            }>
                            <DashboardsIcon className="size-3 ml-3 shrink-0 opacity-70" />
                            <p className="min-w-0 flex-1 truncate text-xs ml-1.5">
                                {dashboard.name}
                            </p>
                            <div className="flex items-center gap-0">
                                <div className="opacity-0 pointer-events-none group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto group-focus-within/explorer-node:opacity-100 group-focus-within/explorer-node:pointer-events-auto">
                                    <DeleteDashboardIconButton
                                        dashboardId={dashboard.id}
                                        dashboardName={dashboard.name}
                                    />
                                </div>
                                <Button
                                    type="button"
                                    size="xsIcon"
                                    variant="ghost"
                                    className={`text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0 ${
                                        dashboard.favorite
                                            ? "opacity-100 pointer-events-auto"
                                            : "opacity-0 pointer-events-none group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto group-focus-within/explorer-node:opacity-100 group-focus-within/explorer-node:pointer-events-auto"
                                    }`}
                                    title={
                                        dashboard.favorite
                                            ? "Remove from favorites"
                                            : "Add to favorites"
                                    }
                                    disabled={
                                        isPending &&
                                        updatingDashboardId === dashboard.id
                                    }
                                    onPointerDown={stopPropagation}
                                    onClick={(event) =>
                                        void handleToggleFavorite(
                                            event,
                                            dashboard.id,
                                            dashboard.favorite,
                                        )
                                    }>
                                    <StarIcon
                                        className={
                                            dashboard.favorite
                                                ? "size-3 text-foreground fill-current"
                                                : "size-3"
                                        }
                                    />
                                </Button>
                            </div>
                        </div>
                    ))}
                    {additionalItems}
                </div>
            ) : null}
        </div>
    );
};
