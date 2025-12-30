import { toast } from "sonner";
import { useExecuteQuery } from "@serene-ui/shared-frontend/entities";
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

export const ExplorerQueryNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const [isError, setIsError] = useState(false);
    const { node, style } = nodeData;
    const { treeRef, addNodes, refreshNode, updateNodeData } = useExplorer();

    const nodeType = node.data.type as keyof typeof nodeTemplates;
    const { mutateAsync: executeQuery, isPending: isLoading } =
        useExecuteQuery();

    const handleClick = async () => {
        const rootNodeId = node.id.split("/")[0];
        setIsError(false);
        updateNodeData(rootNodeId, { isError: false });
        if (node.isOpen) {
            node.close();
            return;
        }
        const connectionId = node.data.context?.connectionId;

        const queryInput =
            typeof nodeTemplates[nodeType].getChildrenQuery === "function"
                ? nodeTemplates[nodeType].getChildrenQuery(node.data)
                : nodeTemplates[nodeType].getChildrenQuery;
        if (!queryInput) {
            node.close();
            return;
        }

        try {
            const { result } = await executeQuery({
                query: queryInput?.query || "",
                database: queryInput?.database || "",
                connectionId,
            });

            const connectionNode = treeRef.current?.get(node.data.id);
            if (!connectionNode) return;

            addNodes(
                node.data.id,
                nodeTemplates[nodeType].formatToNodes(
                    result as any[],
                    node.data,
                ),
            );
            node.open();
        } catch (error) {
            const message = (error as Error).message;
            if (message.includes("AggregateError")) {
                const rootNodeId = node.id.split("/")[0];
                const rootNode = treeRef.current?.get(rootNodeId);

                toast.error("Failed to establish connection", {
                    duration: 5000,
                    description: "Please check your connection settings",
                });

                updateNodeData(rootNodeId, { isError: true });

                if (rootNode && rootNodeId !== node.data.id) {
                    rootNode.close();
                }
            } else {
                toast.error("Failed to load entities");
                setIsError(true);
            }
            return;
        }
    };

    const handleRefresh = async () => {
        const rootNodeId = node.id.split("/")[0];

        updateNodeData(rootNodeId, { isError: false });
        setIsError(false);
        if (node.isOpen) {
            node.close();
        }

        refreshNode(node.data.id);
        await new Promise((resolve) => setTimeout(resolve, 500));

        node.open();
        const connectionId = node.data.context?.connectionId;

        const queryInput =
            typeof nodeTemplates[nodeType].getChildrenQuery === "function"
                ? nodeTemplates[nodeType].getChildrenQuery(node.data)
                : nodeTemplates[nodeType].getChildrenQuery;

        if (!queryInput) {
            node.close();
            return;
        }

        try {
            const { result } = await executeQuery({
                query: queryInput?.query || "",
                database: queryInput?.database || "",
                connectionId,
            });

            const connectionNode = treeRef.current?.get(node.data.id);
            if (!connectionNode) return;

            addNodes(
                node.data.id,
                nodeTemplates[nodeType].formatToNodes(
                    result as any[],
                    node.data,
                ),
                true,
            );
        } catch (error) {
            const message = (error as Error).message;
            if (message.includes("AggregateError")) {
                const rootNodeId = node.id.split("/")[0];
                const rootNode = treeRef.current?.get(rootNodeId);

                toast.error("Failed to establish connection", {
                    duration: 5000,
                    description: "Please check your connection settings",
                });

                updateNodeData(rootNodeId, { isError: true });

                if (rootNode && rootNodeId !== node.data.id) {
                    rootNode.close();
                }
            } else {
                toast.error("Failed to load entities");
            }
            node.close();
            return;
        }
    };

    return (
        <ContextMenu>
            <ContextMenuTrigger>
                <ExplorerNodeButton
                    title={node.data.name}
                    onClick={handleClick}
                    open={node.isOpen}
                    icon={nodeTemplates[nodeType]?.icon}
                    style={style}
                    isLoading={isLoading}
                    isError={node.data.isError || isError}
                />
            </ContextMenuTrigger>
            <ContextMenuContent className="w-52">
                <ContextMenuItem onClick={handleRefresh}>
                    Refresh
                </ContextMenuItem>
            </ContextMenuContent>
        </ContextMenu>
    );
};
