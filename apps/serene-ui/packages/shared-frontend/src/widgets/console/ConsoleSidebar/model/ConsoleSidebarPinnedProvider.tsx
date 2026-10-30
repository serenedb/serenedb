import type { SavedQuerySchema } from "@serene-ui/shared-core";
import React, {
    createContext,
    useContext,
    useEffect,
    useMemo,
    useState,
} from "react";
import type {
    ExplorerNodeContext,
    ExplorerNodeData,
} from "../../../shared/Explorer";

const CONSOLE_PINNED_ITEMS_STORAGE_KEY = "console:sidebar:pinned-items:v1";

type SerializableExplorerNodeContext = Omit<ExplorerNodeContext, "action">;

type ConsolePinnedExplorerNode = {
    kind: "explorer-node";
    key: string;
    name: string;
    type: string;
    context?: SerializableExplorerNodeContext;
};

type ConsolePinnedSavedQuery = {
    kind: "saved-query";
    key: string;
    id: SavedQuerySchema["id"];
    name: SavedQuerySchema["name"];
    query: SavedQuerySchema["query"];
};

type ConsolePinnedItem = ConsolePinnedExplorerNode | ConsolePinnedSavedQuery;

type ConsoleSidebarPinnedContextValue = {
    pinnedItems: ConsolePinnedItem[];
    pinnedExplorerNodes: ExplorerNodeData[];
    isNodePinned: (node: ExplorerNodeData) => boolean;
    togglePinnedNode: (node: ExplorerNodeData) => void;
    isSavedQueryPinned: (savedQueryId: SavedQuerySchema["id"]) => boolean;
    togglePinnedSavedQuery: (
        savedQuery: Pick<SavedQuerySchema, "id" | "name" | "query">,
    ) => void;
};

const ConsoleSidebarPinnedContext = createContext<
    ConsoleSidebarPinnedContextValue | undefined
>(undefined);

const buildContextIdentity = (context?: SerializableExplorerNodeContext) => {
    return JSON.stringify({
        connectionId: context?.connectionId ?? null,
        database: context?.database ?? null,
        dashboardId: context?.dashboardId ?? null,
        dashboardFavorite: context?.dashboardFavorite ?? null,
        savedQueryId: context?.savedQueryId ?? null,
        schemaId: context?.schemaId ?? null,
        catalogId: context?.catalogId ?? null,
        tableId: context?.tableId ?? null,
        query: context?.query ?? null,
        viewId: context?.viewId ?? null,
        column_data_type: context?.column_data_type ?? null,
        column_is_array: context?.column_is_array ?? null,
    });
};

const normalizeExplorerNodeContext = (
    context?: ExplorerNodeContext,
): SerializableExplorerNodeContext | undefined => {
    if (!context) {
        return undefined;
    }

    const normalizedContext: SerializableExplorerNodeContext = {};

    if (typeof context.connectionId === "number") {
        normalizedContext.connectionId = context.connectionId;
    }
    if (typeof context.database === "string") {
        normalizedContext.database = context.database;
    }
    if (typeof context.dashboardId === "number") {
        normalizedContext.dashboardId = context.dashboardId;
    }
    if (typeof context.dashboardFavorite === "boolean") {
        normalizedContext.dashboardFavorite = context.dashboardFavorite;
    }
    if (typeof context.savedQueryId === "number") {
        normalizedContext.savedQueryId = context.savedQueryId;
    }
    if (typeof context.schemaId === "number") {
        normalizedContext.schemaId = context.schemaId;
    }
    if (typeof context.catalogId === "number") {
        normalizedContext.catalogId = context.catalogId;
    }
    if (typeof context.tableId === "number") {
        normalizedContext.tableId = context.tableId;
    }
    if (typeof context.query === "string") {
        normalizedContext.query = context.query;
    }
    if (typeof context.viewId === "number") {
        normalizedContext.viewId = context.viewId;
    }
    if (typeof context.column_data_type === "string") {
        normalizedContext.column_data_type = context.column_data_type;
    }
    if (typeof context.column_is_array === "string") {
        normalizedContext.column_is_array = context.column_is_array;
    }

    return Object.keys(normalizedContext).length ? normalizedContext : undefined;
};

const getSavedQueryPinnedKey = (id: SavedQuerySchema["id"]) =>
    `saved-query:${id}`;

const getExplorerNodePinnedKey = (
    node: Pick<ExplorerNodeData, "name" | "type" | "context">,
) => {
    const normalizedContext = normalizeExplorerNodeContext(node.context);
    return `explorer-node:${node.type}:${node.name}:${buildContextIdentity(normalizedContext)}`;
};

const createPinnedExplorerItem = (
    node: Pick<ExplorerNodeData, "name" | "type" | "context">,
): ConsolePinnedExplorerNode => ({
    kind: "explorer-node",
    key: getExplorerNodePinnedKey(node),
    name: node.name,
    type: node.type,
    context: normalizeExplorerNodeContext(node.context),
});

const createPinnedSavedQueryItem = (
    savedQuery: Pick<SavedQuerySchema, "id" | "name" | "query">,
): ConsolePinnedSavedQuery => ({
    kind: "saved-query",
    key: getSavedQueryPinnedKey(savedQuery.id),
    id: savedQuery.id,
    name: savedQuery.name,
    query: savedQuery.query,
});

const getPinnedNodeId = (key: string, index: number) => {
    const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 80);
    return `pinned-${index}-${safeKey}`;
};

const toPinnedExplorerNodes = (
    items: ConsolePinnedItem[],
): ExplorerNodeData[] => {
    return items.map((item, index) => {
        const baseNode = {
            id: getPinnedNodeId(item.key, index),
            name: item.name,
            parentId: null,
        };

        if (item.kind === "saved-query") {
            return {
                ...baseNode,
                type: "saved-query",
                context: {
                    savedQueryId: item.id,
                    query: item.query,
                },
            };
        }

        return {
            ...baseNode,
            type: item.type,
            context: item.context,
        };
    });
};

const isObject = (value: unknown): value is Record<string, unknown> =>
    typeof value === "object" && value !== null;

const readPinnedItemsFromStorage = (): ConsolePinnedItem[] => {
    if (typeof window === "undefined") {
        return [];
    }

    try {
        const rawValue = window.localStorage.getItem(
            CONSOLE_PINNED_ITEMS_STORAGE_KEY,
        );
        if (!rawValue) {
            return [];
        }

        const parsedValue = JSON.parse(rawValue) as unknown;
        if (!Array.isArray(parsedValue)) {
            return [];
        }

        return parsedValue.reduce<ConsolePinnedItem[]>((accumulator, item) => {
            if (!isObject(item) || typeof item.kind !== "string") {
                return accumulator;
            }

            if (
                item.kind === "explorer-node" &&
                typeof item.key === "string" &&
                typeof item.name === "string" &&
                typeof item.type === "string"
            ) {
                accumulator.push({
                    kind: "explorer-node",
                    key: item.key,
                    name: item.name,
                    type: item.type,
                    context: normalizeExplorerNodeContext(
                        isObject(item.context)
                            ? (item.context as ExplorerNodeContext)
                            : undefined,
                    ),
                });
                return accumulator;
            }

            if (
                item.kind === "saved-query" &&
                typeof item.key === "string" &&
                typeof item.id === "number" &&
                typeof item.name === "string" &&
                typeof item.query === "string"
            ) {
                accumulator.push({
                    kind: "saved-query",
                    key: item.key,
                    id: item.id,
                    name: item.name,
                    query: item.query,
                });
            }

            return accumulator;
        }, []);
    } catch (error) {
        console.warn("Failed to restore pinned console items:", error);
        return [];
    }
};

export const ConsoleSidebarPinnedProvider: React.FC<{
    children: React.ReactNode;
}> = ({ children }) => {
    const [pinnedItems, setPinnedItems] = useState<ConsolePinnedItem[]>(() =>
        readPinnedItemsFromStorage(),
    );

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        try {
            window.localStorage.setItem(
                CONSOLE_PINNED_ITEMS_STORAGE_KEY,
                JSON.stringify(pinnedItems),
            );
        } catch (error) {
            console.warn("Failed to persist pinned console items:", error);
        }
    }, [pinnedItems]);

    const togglePinnedSavedQuery = (
        savedQuery: Pick<SavedQuerySchema, "id" | "name" | "query">,
    ) => {
        const pinnedItem = createPinnedSavedQueryItem(savedQuery);

        setPinnedItems((currentItems) => {
            if (currentItems.some((item) => item.key === pinnedItem.key)) {
                return currentItems.filter((item) => item.key !== pinnedItem.key);
            }

            return [pinnedItem, ...currentItems];
        });
    };

    const togglePinnedNode = (node: ExplorerNodeData) => {
        const savedQueryId = node.context?.savedQueryId;
        const query = node.context?.query;

        if (
            node.type === "saved-query" &&
            typeof savedQueryId === "number" &&
            typeof query === "string"
        ) {
            togglePinnedSavedQuery({
                id: savedQueryId,
                name: node.name,
                query,
            });
            return;
        }

        const pinnedItem = createPinnedExplorerItem(node);

        setPinnedItems((currentItems) => {
            if (currentItems.some((item) => item.key === pinnedItem.key)) {
                return currentItems.filter((item) => item.key !== pinnedItem.key);
            }

            return [pinnedItem, ...currentItems];
        });
    };

    const isSavedQueryPinned = (savedQueryId: SavedQuerySchema["id"]) =>
        pinnedItems.some(
            (item) => item.key === getSavedQueryPinnedKey(savedQueryId),
        );

    const isNodePinned = (node: ExplorerNodeData) => {
        const savedQueryId = node.context?.savedQueryId;
        if (node.type === "saved-query" && typeof savedQueryId === "number") {
            return isSavedQueryPinned(savedQueryId);
        }

        return pinnedItems.some(
            (item) => item.key === getExplorerNodePinnedKey(node),
        );
    };

    const pinnedExplorerNodes = useMemo(
        () => toPinnedExplorerNodes(pinnedItems),
        [pinnedItems],
    );

    const value: ConsoleSidebarPinnedContextValue = {
        pinnedItems,
        pinnedExplorerNodes,
        isNodePinned,
        togglePinnedNode,
        isSavedQueryPinned,
        togglePinnedSavedQuery,
    };

    return (
        <ConsoleSidebarPinnedContext.Provider value={value}>
            {children}
        </ConsoleSidebarPinnedContext.Provider>
    );
};

export const useConsoleSidebarPinned = () => {
    const context = useContext(ConsoleSidebarPinnedContext);
    if (!context) {
        throw new Error(
            "useConsoleSidebarPinned must be used within ConsoleSidebarPinnedProvider",
        );
    }
    return context;
};
