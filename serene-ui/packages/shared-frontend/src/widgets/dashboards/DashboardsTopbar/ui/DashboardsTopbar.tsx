import { SidebarIcon } from "lucide-react";
import type { DashboardSchema } from "@serene-ui/shared-core";
import React from "react";
import { toast } from "sonner";
import { useUpdateDashboard } from "../../../../entities/dashboard";
import { Button, getErrorMessage, RefreshIcon, StarIcon } from "../../../../shared";

interface DashboardsTopbarProps {
    currentDashboard?: DashboardSchema | null;
    isExplorerOpened: boolean;
    onToggleExplorer: () => void;
    onRefreshAllCharts?: () => void;
}

export const DashboardsTopbar: React.FC<DashboardsTopbarProps> = ({
    currentDashboard,
    isExplorerOpened,
    onToggleExplorer,
    onRefreshAllCharts,
}) => {
    const { mutateAsync: updateDashboard, isPending } = useUpdateDashboard();

    const handleToggleFavorite = async () => {
        if (!currentDashboard) {
            return;
        }

        try {
            await updateDashboard({
                id: currentDashboard.id,
                favorite: !currentDashboard.favorite,
            });
        } catch (error) {
            toast.error("Failed to update favorite", {
                description: getErrorMessage(error, "Failed to update favorite"),
            });
        }
    };

    return (
        <div className="w-full h-12 flex px-2.5 items-center justify-between border-b-1">
            <div className="flex gap-4 items-center">
                <Button
                    size="icon"
                    variant="secondary"
                    title={
                        isExplorerOpened
                            ? "Close dashboards explorer"
                            : "Open dashboards explorer"
                    }
                    onClick={onToggleExplorer}>
                    <SidebarIcon />
                </Button>
                <p className="text-xs text-primary-foreground">
                    {currentDashboard?.name ?? "Select dashboard"}
                </p>
            </div>
            <div className="flex gap-1">
                <Button
                    size="icon"
                    variant="secondary"
                    title={
                        currentDashboard?.favorite
                            ? "Remove from favorites"
                            : "Add to favorites"
                    }
                    aria-label={
                        currentDashboard?.favorite
                            ? "Remove from favorites"
                            : "Add to favorites"
                    }
                    disabled={!currentDashboard || isPending}
                    onClick={() => void handleToggleFavorite()}>
                    <StarIcon
                        className={
                            currentDashboard?.favorite
                                ? "fill-current"
                                : undefined
                        }
                    />
                </Button>
                <Button
                    size="icon"
                    variant="secondary"
                    title="Refresh all charts"
                    aria-label="Refresh all charts"
                    disabled={!currentDashboard}
                    onClick={onRefreshAllCharts}>
                    <RefreshIcon />
                </Button>
            </div>
        </div>
    );
};
