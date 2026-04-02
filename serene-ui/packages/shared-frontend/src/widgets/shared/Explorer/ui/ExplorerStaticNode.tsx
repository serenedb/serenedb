import { useExplorer, type ExplorerNodeProps } from "../model";
import { nodeTemplates } from "../model/const";
import { ExplorerNodeButton } from "./ExplorerNodeButton";
import {
    ContextMenu,
    ContextMenuContent,
    ContextMenuItem,
    ContextMenuTrigger,
} from "@serene-ui/shared-frontend/shared";
import { useState } from "react";
import { useConnection } from "@serene-ui/shared-frontend/entities";

export const ExplorerStaticNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const [isLoading, setIsLoading] = useState(false);
    const { node, style } = nodeData;
    const {
        addNodes,
        refreshNode,
        enablePinning,
        isNodePinned,
        togglePinnedNode,
    } = useExplorer();
    const { currentConnection } = useConnection();

    const nodeType = node.data.type as keyof typeof nodeTemplates;
    const isCurrentDatabase =
        nodeType === "database" &&
        node.data.context?.connectionId === currentConnection.connectionId &&
        node.data.context?.database === currentConnection.database;
    const pinned = enablePinning && isNodePinned(node.data);

    const handleClick = async () => {
        if (!node.isOpen) {
            node.open();
            addNodes(node.data.id, nodeTemplates[nodeType].getNodes(node.data));
        } else {
            node.close();
        }
    };

    const handleRefresh = async () => {
        if (node.isOpen) {
            node.close();
        }

        refreshNode(node.data.id);

        setIsLoading(true);
        await new Promise((resolve) => setTimeout(resolve, 500));

        node.open();
        addNodes(
            node.data.id,
            nodeTemplates[nodeType].getNodes(node.data),
            true,
        );
        setIsLoading(false);
    };

    return (
        <ContextMenu>
            <ContextMenuTrigger className="h-full block">
                <ExplorerNodeButton
                    title={node.data.name}
                    onClick={handleClick}
                    open={node.isOpen}
                    icon={nodeTemplates[nodeType]?.icon}
                    style={style}
                    showArrow={nodeType !== "column" && nodeType !== "index"}
                    isLoading={isLoading}
                    rightText={
                        nodeType === "column"
                            ? node.data.context?.column_data_type
                            : ""
                    }
                    rightNode={
                        isCurrentDatabase ? (
                            <div className="rounded-full border border-emerald-500/40 bg-emerald-500/10 px-2 py-1 text-[10px] pointer-events-none font-medium leading-none text-emerald-700 dark:text-emerald-300">
                                current
                            </div>
                        ) : undefined
                    }
                    isPinned={pinned}
                    onTogglePin={
                        enablePinning
                            ? () => {
                                  togglePinnedNode(node.data);
                              }
                            : undefined
                    }
                />
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
                <ContextMenuItem onClick={handleRefresh}>
                    Refresh
                </ContextMenuItem>
            </ContextMenuContent>
        </ContextMenu>
    );
};
