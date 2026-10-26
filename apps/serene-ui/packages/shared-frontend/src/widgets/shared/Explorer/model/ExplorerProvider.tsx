import React, { createContext, useContext, useState } from "react";
import type { TreeApi } from "react-arborist";
import type { ExplorerNodeData, ExplorerPinningOptions } from "./types";

interface ExplorerContextType {
    currentTree: ExplorerNodeData[];
    setCurrentTree: React.Dispatch<React.SetStateAction<ExplorerNodeData[]>>;
    treeRef: React.RefObject<TreeApi<ExplorerNodeData> | null>;
    addNodes: (
        parentId: string,
        newChildren: ExplorerNodeData[],
        forceReplace?: boolean,
    ) => void;
    removeNode: (parentPath: string) => void;
    refreshNode: (nodeId: string) => void;
    updateNodeData: (
        nodeId: string,
        updates: Partial<ExplorerNodeData>,
    ) => void;
    enablePinning: boolean;
    isNodePinned: (node: ExplorerNodeData) => boolean;
    togglePinnedNode: (node: ExplorerNodeData) => void;
}

const ExplorerContext = createContext<ExplorerContextType | undefined>(
    undefined,
);

const getNodeIdentity = (node: Pick<ExplorerNodeData, "type" | "name">) =>
    `${node.type}:${node.name}`;

export const ExplorerProvider = ({
    children,
    enablePinning = false,
    isNodePinned,
    onTogglePinned,
}: {
    children: React.ReactNode;
} & ExplorerPinningOptions) => {
    const [tree, setTree] = useState<ExplorerNodeData[]>([]);
    const treeRef = React.useRef<TreeApi<ExplorerNodeData> | null>(null);

    const addNodes = (
        parentId: string,
        newChildren: ExplorerNodeData[],
        forceReplace: boolean = false,
    ) => {
        const parentNode = treeRef.current?.get(parentId);
        let maxId = 1;
        const existingChildrenByIdentity = new Map<
            string,
            ExplorerNodeData
        >();

        for (const child of parentNode?.children || []) {
            const childIdParts = child.id.split("/");
            const childId = childIdParts[childIdParts.length - 1];
            const childIndex = parseInt(childId.split("-")[1]);

            if (maxId < childIndex) maxId = childIndex;
            existingChildrenByIdentity.set(
                getNodeIdentity(child.data),
                child.data,
            );
        }

        const nextNodes = newChildren.map((child) => {
            const existingChild = existingChildrenByIdentity.get(
                getNodeIdentity(child),
            );

            if (!existingChild) {
                const childIdParts = child.id.split("/");
                const childId = childIdParts[childIdParts.length - 1];
                const path = childIdParts
                    .slice(0, childIdParts.length - 1)
                    .join("/");
                const childPrefix = parseInt(childId.split("-")[0]);
                const nextId = path + "/" + childPrefix + "-" + (maxId + 1);

                maxId++;

                return {
                    ...child,
                    id: nextId,
                };
            }

            return {
                ...existingChild,
                ...child,
                id: existingChild.id,
                children:
                    forceReplace || child.children
                        ? child.children ?? existingChild.children
                        : existingChild.children,
            };
        });

        setTree((prevTree) => {
            const pathParts = parentId.split("/");

            const updateChildren = (
                nodes: ExplorerNodeData[],
                depth: number,
            ): ExplorerNodeData[] => {
                return nodes.map((node) => {
                    if (node.id.split("/")[depth] === pathParts[depth]) {
                        if (depth === pathParts.length - 1) {
                            return {
                                ...node,
                                children: nextNodes,
                            };
                        }
                        return {
                            ...node,
                            children: updateChildren(
                                node.children || [],
                                depth + 1,
                            ),
                        };
                    }
                    return node;
                });
            };

            return updateChildren(prevTree, 0);
        });
    };

    const removeNode = (parentId: string) => {
        const node = treeRef.current?.get(parentId);
        if (!node) return;
        treeRef.current?.delete(node.id);
    };

    const refreshNode = (nodeId: string) => {
        setTree((prevTree) => {
            const pathParts = nodeId.split("/");

            const clearChildren = (
                nodes: ExplorerNodeData[],
                depth: number,
            ): ExplorerNodeData[] => {
                return nodes.map((node) => {
                    if (node.id.split("/")[depth] === pathParts[depth]) {
                        if (depth === pathParts.length - 1) {
                            return {
                                ...node,
                                children: [],
                            };
                        }
                        return {
                            ...node,
                            children: clearChildren(
                                node.children || [],
                                depth + 1,
                            ),
                        };
                    }
                    return node;
                });
            };

            return clearChildren(prevTree, 0);
        });
    };

    const updateNodeData = (
        nodeId: string,
        updates: Partial<ExplorerNodeData>,
    ) => {
        setTree((prevTree) => {
            const pathParts = nodeId.split("/");

            const updateNode = (
                nodes: ExplorerNodeData[],
                depth: number,
            ): ExplorerNodeData[] => {
                return nodes.map((node) => {
                    if (node.id.split("/")[depth] === pathParts[depth]) {
                        if (depth === pathParts.length - 1) {
                            return {
                                ...node,
                                ...updates,
                            };
                        }
                        return {
                            ...node,
                            children: updateNode(
                                node.children || [],
                                depth + 1,
                            ),
                        };
                    }
                    return node;
                });
            };

            return updateNode(prevTree, 0);
        });
    };

    return (
        <ExplorerContext.Provider
            value={{
                treeRef,
                currentTree: tree,
                setCurrentTree: setTree,
                addNodes,
                removeNode,
                refreshNode,
                updateNodeData,
                enablePinning,
                isNodePinned: isNodePinned || (() => false),
                togglePinnedNode: onTogglePinned || (() => {}),
            }}>
            {children}
        </ExplorerContext.Provider>
    );
};

export const useExplorer = () => {
    const context = useContext(ExplorerContext);
    if (!context) {
        throw new Error("useExplorer must be used within a ExplorerProvider");
    }
    return context;
};
