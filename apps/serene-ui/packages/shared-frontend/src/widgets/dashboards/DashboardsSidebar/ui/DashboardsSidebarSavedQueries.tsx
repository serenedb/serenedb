import React from "react";
import {
    Button,
    DeleteSavedQueryIconButton,
    EditIcon,
    focusSidebarElement,
    handleSidebarListItemKeyDown,
    StarIcon,
    TreeQueryIcon,
} from "@serene-ui/shared-frontend";
import { useGetSavedQueries } from "@serene-ui/shared-frontend/entities";
import type { SavedQuerySchema } from "@serene-ui/shared-core";
import {
    createDashboardSavedQueryDragPayload,
    setDashboardSavedQueryDragActive,
    setDashboardSavedQueryDragData,
} from "../../model/savedQueryDrag";
import { useSavedQueriesModal } from "../../../../features/openSavedQueriesModal";
import { ExportSavedQueryButton } from "../../../../features/openSavedQueriesModal/ui/buttons";
import { useDashboardsSidebarFavoriteSavedQueries } from "../model";

export const DashboardsSidebarSavedQueries: React.FC = () => {
    const { data: savedQueries, isLoading } = useGetSavedQueries();
    const { isSavedQueryFavorite, toggleFavoriteSavedQuery } =
        useDashboardsSidebarFavoriteSavedQueries();
    const { openEditModal } = useSavedQueriesModal();
    const dragPreviewRef = React.useRef<HTMLElement | null>(null);

    const clearDragPreview = React.useCallback(() => {
        if (!dragPreviewRef.current) {
            return;
        }

        dragPreviewRef.current.remove();
        dragPreviewRef.current = null;
    }, []);

    React.useEffect(() => {
        return () => {
            clearDragPreview();
        };
    }, [clearDragPreview]);

    return (
        <div className="flex h-full min-h-0 flex-col pt-0">
            <div className="flex min-h-0 flex-1 flex-col overflow-auto">
                {isLoading ? (
                    <div className="flex h-full items-center justify-center p-2">
                        <p className="text-xs text-foreground/70">
                            Loading saved queries...
                        </p>
                    </div>
                ) : !savedQueries?.length ? (
                    <div className="flex h-full items-center justify-center p-2">
                        <p className="text-center text-xs text-foreground/70">
                            No saved queries yet
                        </p>
                    </div>
                ) : (
                    savedQueries.map((savedQuery) => {
                        const isFavorite = isSavedQueryFavorite(savedQuery.id);

                        return (
                            <div
                                key={savedQuery.id}
                                className="group/explorer-node flex h-7 items-center gap-1 pl-4 pr-1 hover:bg-accent focus:bg-accent outline-none"
                                title={savedQuery.name}
                                tabIndex={0}
                                data-sidebar-primary-action="true"
                                data-sidebar-focus-id={`dashboards-sidebar-savedQueries-saved-query-${savedQuery.id}`}
                                data-sidebar-section-id="savedQueries"
                                draggable
                                onKeyDown={(event) =>
                                    handleSidebarListItemKeyDown(event, () => {})
                                }
                                onClick={(event) => {
                                    focusSidebarElement(event.currentTarget);
                                }}
                                onDragStart={(event) => {
                                    event.stopPropagation();
                                    clearDragPreview();

                                    const payload =
                                        createDashboardSavedQueryDragPayload(
                                            savedQuery,
                                        );

                                    setDashboardSavedQueryDragActive(true);
                                    setDashboardSavedQueryDragData(
                                        event.dataTransfer,
                                        payload,
                                    );
                                    event.dataTransfer.effectAllowed = "copy";

                                    const dragPreview =
                                        document.createElement("div");
                                    dragPreview.textContent = savedQuery.name;
                                    dragPreview.style.position = "fixed";
                                    dragPreview.style.top = "-9999px";
                                    dragPreview.style.left = "-9999px";
                                    dragPreview.style.padding = "4px 8px";
                                    dragPreview.style.borderRadius = "4px";
                                    dragPreview.style.fontSize = "12px";
                                    dragPreview.style.background =
                                        "rgb(34 34 34)";
                                    dragPreview.style.color = "white";
                                    dragPreview.style.pointerEvents = "none";

                                    document.body.appendChild(dragPreview);
                                    dragPreviewRef.current = dragPreview;
                                    event.dataTransfer.setDragImage(
                                        dragPreview,
                                        8,
                                        8,
                                    );
                                }}
                                onDragEnd={() => {
                                    clearDragPreview();
                                    setDashboardSavedQueryDragActive(false);
                                }}>
                                <TreeQueryIcon className="ml-3 size-3 shrink-0 opacity-70" />
                                <p className="min-w-0 flex-1 truncate text-xs ml-1.5">
                                    {savedQuery.name}
                                </p>
                                <div className="flex items-center gap-0">
                                    <Button
                                        variant="ghost"
                                        size="xsIcon"
                                        className="text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0 opacity-0 pointer-events-none group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto"
                                        title="Edit query"
                                        draggable={false}
                                        onClick={(event) => {
                                            event.preventDefault();
                                            event.stopPropagation();
                                            openEditModal(savedQuery);
                                        }}>
                                        <EditIcon className="size-3" />
                                    </Button>
                                    <ExportSavedQueryButton
                                        variant="ghost"
                                        size="xsIcon"
                                        savedQuery={savedQuery}
                                        className="opacity-0 pointer-events-none group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto"
                                    />
                                    <DeleteSavedQueryIconButton
                                        savedQuery={savedQuery}
                                        className="text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0 opacity-0 pointer-events-none group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto"
                                        onDeleteSuccess={(
                                            deletedSavedQuery: Pick<
                                                SavedQuerySchema,
                                                "id" | "name" | "query"
                                            >,
                                        ) => {
                                            if (
                                                isSavedQueryFavorite(
                                                    deletedSavedQuery.id,
                                                )
                                            ) {
                                                toggleFavoriteSavedQuery(
                                                    deletedSavedQuery,
                                                );
                                            }
                                        }}
                                    />
                                    <Button
                                        variant="ghost"
                                        size="xsIcon"
                                        className={`text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0 ${
                                            isFavorite
                                                ? "opacity-100 pointer-events-auto"
                                                : "opacity-0 pointer-events-none group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto"
                                        }`}
                                        title={
                                            isFavorite
                                                ? "Remove from favorites"
                                                : "Add to favorites"
                                        }
                                        draggable={false}
                                        onClick={(event) => {
                                            event.preventDefault();
                                            event.stopPropagation();
                                            toggleFavoriteSavedQuery(
                                                savedQuery,
                                            );
                                        }}>
                                        <StarIcon
                                            className={
                                                isFavorite
                                                    ? "size-3 text-foreground fill-current"
                                                    : "size-3"
                                            }
                                        />
                                    </Button>
                                </div>
                            </div>
                        );
                    })
                )}
            </div>
        </div>
    );
};
