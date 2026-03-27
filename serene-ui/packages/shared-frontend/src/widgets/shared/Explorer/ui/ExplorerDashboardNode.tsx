import type { MouseEvent, SyntheticEvent } from "react";
import { DeleteDashboardIconButton } from "../../../../features";
import { useUpdateDashboard } from "../../../../entities/dashboard";
import { Button, getErrorMessage, StarIcon } from "../../../../shared";
import { toast } from "sonner";
import type { ExplorerNodeProps } from "../model";
import { nodeTemplates } from "../model/const";
import { ExplorerNodeButton } from "./ExplorerNodeButton";

export const ExplorerDashboardNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const { node, style } = nodeData;
    const dashboardId = node.data.context?.dashboardId;
    const isFavorite = node.data.context?.dashboardFavorite === true;
    const { mutateAsync: updateDashboard, isPending } = useUpdateDashboard();

    const stopPropagation = (event: SyntheticEvent) => {
        event.stopPropagation();
    };

    const handleToggleFavorite = async (
        event: MouseEvent<HTMLButtonElement>,
    ) => {
        event.stopPropagation();

        if (!dashboardId) {
            return;
        }

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
        }
    };

    return (
        <div style={style} className="group relative">
            <ExplorerNodeButton
                className="pl-[27px] pr-7"
                title={node.data.name}
                onClick={node.data.context?.action}
                open={false}
                icon={nodeTemplates.dashboard.icon}
                showArrow={false}
            />
            {dashboardId ? (
                <div className="absolute top-1/2 right-1 z-10 flex -translate-y-1/2 items-center gap-1 opacity-0 transition-opacity group-hover:opacity-100 group-focus-within:opacity-100">
                    <Button
                        type="button"
                        size="iconSmall"
                        variant="ghost"
                        title={
                            isFavorite
                                ? "Remove from favorites"
                                : "Add to favorites"
                        }
                        disabled={isPending}
                        onPointerDown={stopPropagation}
                        onClick={(event) => void handleToggleFavorite(event)}>
                        <StarIcon
                            className={
                                isFavorite
                                    ? "size-3 text-foreground/50 fill-current"
                                    : "size-3 text-foreground/50"
                            }
                        />
                    </Button>
                    <DeleteDashboardIconButton
                        dashboardId={dashboardId}
                        dashboardName={node.data.name}
                    />
                </div>
            ) : null}
        </div>
    );
};
