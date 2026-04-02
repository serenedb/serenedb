import { toast } from "sonner";
import {
    useConnection,
    useDeleteConnection,
    useExecuteQuery,
} from "@serene-ui/shared-frontend/entities";
import { useExplorer, type ExplorerNodeProps } from "../model";
import { nodeTemplates } from "../model/const";
import { ExplorerNodeButton } from "./ExplorerNodeButton";
import {
    Button,
    ContextMenu,
    ContextMenuContent,
    ContextMenuItem,
    ContextMenuTrigger,
} from "@serene-ui/shared-frontend/shared";
import { useState, type MouseEvent } from "react";
import type { QueryExecutionResultSchema } from "@serene-ui/shared-core";
import { DeleteIcon } from "../../../../shared/ui/icons/index";

export const ExplorerQueryNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const [isError, setIsError] = useState(false);
    const [deletingConnectionId, setDeletingConnectionId] = useState<
        number | null
    >(null);
    const { node, style } = nodeData;
    const {
        treeRef,
        addNodes,
        refreshNode,
        updateNodeData,
        enablePinning,
        isNodePinned,
        togglePinnedNode,
    } = useExplorer();

    const nodeType = node.data.type as keyof typeof nodeTemplates;
    const pinned = enablePinning && isNodePinned(node.data);
    const isConnectionNode = nodeType === "connection";
    const { mutateAsync: executeQuery, isPending: isLoading } =
        useExecuteQuery<QueryExecutionResultSchema[]>();
    const { mutateAsync: deleteConnection } = useDeleteConnection();
    const { currentConnection, setCurrentConnection } = useConnection();

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
            const { results } = await executeQuery({
                query: queryInput?.query || "",
                database: queryInput?.database || "",
                connectionId,
            });
            const rows = (results?.[0]?.rows || []) as any[];

            const connectionNode = treeRef.current?.get(node.data.id);
            if (!connectionNode) return;

            addNodes(
                node.data.id,
                nodeTemplates[nodeType].formatToNodes(rows, node.data),
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
            const { results } = await executeQuery({
                query: queryInput?.query || "",
                database: queryInput?.database || "",
                connectionId,
            });
            const rows = (results?.[0]?.rows || []) as any[];

            const connectionNode = treeRef.current?.get(node.data.id);
            if (!connectionNode) return;

            addNodes(
                node.data.id,
                nodeTemplates[nodeType].formatToNodes(rows, node.data),
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

    const handleDeleteConnection = async (
        event: MouseEvent<HTMLButtonElement>,
    ) => {
        event.preventDefault();
        event.stopPropagation();

        const connectionId = node.data.context?.connectionId;

        if (!isConnectionNode || typeof connectionId !== "number") {
            return;
        }

        try {
            setDeletingConnectionId(connectionId);
            await deleteConnection({ id: connectionId });

            if (pinned) {
                togglePinnedNode(node.data);
            }

            if (currentConnection.connectionId === connectionId) {
                setCurrentConnection({
                    connectionId: -1,
                    database: "",
                });
            }
        } catch (error) {
            console.error(error);
            toast.error("Failed to delete connection");
        } finally {
            setDeletingConnectionId((currentId) =>
                currentId === connectionId ? null : currentId,
            );
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
                    afterPinNode={
                        isConnectionNode ? (
                            <Button
                                variant="ghost"
                                size="xsIcon"
                                className="text-foreground/50 hover:text-foreground bg-transparent hover:bg-white/5 transition-none duration-0 opacity-0 pointer-events-none group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto group-focus-within/explorer-node:opacity-100 group-focus-within/explorer-node:pointer-events-auto mr-1"
                                title="Delete connection"
                                disabled={
                                    deletingConnectionId ===
                                    node.data.context?.connectionId
                                }
                                onClick={handleDeleteConnection}>
                                <DeleteIcon className="size-3" />
                            </Button>
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
