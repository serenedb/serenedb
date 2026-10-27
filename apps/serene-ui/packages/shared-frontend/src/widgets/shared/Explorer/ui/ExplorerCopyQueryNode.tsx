import {
    createSavedQueryDragPayload,
    setSavedQueryDragData,
} from "@serene-ui/shared-frontend/entities";
import {
    ContextMenu,
    ContextMenuContent,
    ContextMenuItem,
    ContextMenuTrigger,
    TreeQueryIcon,
} from "@serene-ui/shared-frontend/shared";
import { ExplorerNodeButton } from "./ExplorerNodeButton";
import { type DragEvent } from "react";
import { type ExplorerNodeProps, useExplorer } from "../model";

export const ExplorerCopyQueryNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const { node } = nodeData;
    const { enablePinning, isNodePinned, togglePinnedNode } = useExplorer();
    const savedQueryId = node.data.context?.savedQueryId;
    const query = node.data.context?.query;
    const canDrag =
        typeof savedQueryId === "number" && typeof query === "string";
    const pinned = enablePinning && isNodePinned(node.data);

    const handleClick = async () => {
        if (!node.isOpen) {
            node.open();
            node.data.context?.action?.();
        } else {
            node.close();
        }
    };

    const handleDragStart = (event: DragEvent<HTMLDivElement>) => {
        if (!canDrag || typeof savedQueryId !== "number" || !query) {
            return;
        }

        event.stopPropagation();

        setSavedQueryDragData(
            event.dataTransfer,
            createSavedQueryDragPayload({
                id: savedQueryId,
                name: node.data.name,
                query,
            }),
        );
        event.dataTransfer.effectAllowed = "copy";
    };

    return (
        <ContextMenu>
            <ContextMenuTrigger className="h-full block">
                <div
                    className="h-full"
                    draggable={canDrag}
                    onDragStart={handleDragStart}>
                    <ExplorerNodeButton
                        title={node.data.name}
                        onClick={handleClick}
                        open={node.isOpen}
                        icon={<TreeQueryIcon />}
                        showArrow={false}
                        isPinned={pinned}
                        onTogglePin={
                            enablePinning
                                ? () => {
                                      togglePinnedNode(node.data);
                                  }
                                : undefined
                        }
                    />
                </div>
            </ContextMenuTrigger>
            <ContextMenuContent className="w-52">
                {enablePinning ? (
                    <ContextMenuItem
                        onClick={() => {
                            togglePinnedNode(node.data);
                        }}>
                        {pinned ? "Unpin" : "Pin"}
                    </ContextMenuItem>
                ) : null}
            </ContextMenuContent>
        </ContextMenu>
    );
};
