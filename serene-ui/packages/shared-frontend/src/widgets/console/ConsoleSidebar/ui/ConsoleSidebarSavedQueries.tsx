import React from "react";
import {
    createSavedQueryDragPayload,
    setSavedQueryDragData,
    useDeleteSavedQuery,
    useGetSavedQueries,
} from "@serene-ui/shared-frontend/entities";
import {
    Button,
    PinIcon,
    TrashIcon,
    TreeQueryIcon,
} from "@serene-ui/shared-frontend";
import { toast } from "sonner";

export const ConsoleSidebarSavedQueries: React.FC = () => {
    const { data: savedQueries, isLoading } = useGetSavedQueries();
    const { mutateAsync: deleteSavedQuery } = useDeleteSavedQuery();
    const [deletingSavedQueryId, setDeletingSavedQueryId] = React.useState<
        number | null
    >(null);
    const dragPreviewRef = React.useRef<HTMLElement | null>(null);

    const clearDragPreview = React.useCallback(() => {
        if (!dragPreviewRef.current) {
            return;
        }

        dragPreviewRef.current.remove();
        dragPreviewRef.current = null;
    }, []);

    const handleDeleteSavedQuery = React.useCallback(
        async (event: React.MouseEvent<HTMLButtonElement>, id: number) => {
            event.preventDefault();
            event.stopPropagation();

            try {
                setDeletingSavedQueryId(id);
                await deleteSavedQuery({ id });
            } catch (error) {
                console.error(error);
                toast.error("Failed to delete saved query");
            } finally {
                setDeletingSavedQueryId((currentId) =>
                    currentId === id ? null : currentId,
                );
            }
        },
        [deleteSavedQuery],
    );

    React.useEffect(() => {
        return () => {
            clearDragPreview();
        };
    }, [clearDragPreview]);

    if (isLoading) {
        return (
            <div className="flex h-full items-center justify-center p-2">
                <p className="text-xs text-foreground/70">Loading saved queries...</p>
            </div>
        );
    }

    if (!savedQueries?.length) {
        return (
            <div className="flex h-full items-center justify-center p-2">
                <p className="text-center text-xs text-foreground/70">
                    No saved queries yet
                </p>
            </div>
        );
    }

    return (
        <div className="flex h-full min-h-0 flex-col pt-0">
            <div className="flex min-h-0 flex-1 flex-col overflow-auto">
                {savedQueries.map((savedQuery) => (
                    <div
                        key={savedQuery.id}
                        className="flex items-center gap-1 p-1 pl-3.5 hover:bg-accent"
                        title={savedQuery.name}
                        draggable
                        onDragStart={(event) => {
                            event.stopPropagation();
                            clearDragPreview();
                            const payload = createSavedQueryDragPayload(savedQuery);

                            setSavedQueryDragData(event.dataTransfer, payload);
                            event.dataTransfer.effectAllowed = "copy";

                            const dragPreview = document.createElement("div");
                            dragPreview.textContent = savedQuery.name;
                            dragPreview.style.position = "fixed";
                            dragPreview.style.top = "-9999px";
                            dragPreview.style.left = "-9999px";
                            dragPreview.style.padding = "4px 8px";
                            dragPreview.style.borderRadius = "4px";
                            dragPreview.style.fontSize = "12px";
                            dragPreview.style.background = "rgb(34 34 34)";
                            dragPreview.style.color = "white";
                            dragPreview.style.pointerEvents = "none";

                            document.body.appendChild(dragPreview);
                            dragPreviewRef.current = dragPreview;
                            event.dataTransfer.setDragImage(dragPreview, 8, 8);
                        }}
                        onDragEnd={() => {
                            clearDragPreview();
                        }}>
                        <TreeQueryIcon className="size-4 shrink-0 opacity-70" />
                        <p className="min-w-0 flex-1 truncate text-xs ml-1">
                            {savedQuery.name}
                        </p>
                        <Button
                            variant="ghost"
                            size="iconSmall"
                            className="text-foreground/50 hover:text-foreground bg-transparent hover:bg-white/5"
                            title="Pin query (coming soon)"
                            draggable={false}
                            onClick={(event) => {
                                event.preventDefault();
                                event.stopPropagation();
                            }}>
                            <PinIcon className="size-3" />
                        </Button>
                        <Button
                            variant="ghost"
                            size="iconSmall"
                            className="text-foreground/50 hover:text-foreground bg-transparent hover:bg-white/5"
                            title="Delete query"
                            disabled={deletingSavedQueryId === savedQuery.id}
                            draggable={false}
                            onClick={(event) =>
                                handleDeleteSavedQuery(event, savedQuery.id)
                            }>
                            <TrashIcon className="size-3" />
                        </Button>
                    </div>
                ))}
            </div>
        </div>
    );
};
