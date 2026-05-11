import {
    useEffect,
    forwardRef,
    useCallback,
    useRef,
    type HTMLAttributes,
    type KeyboardEvent,
    type ReactElement,
} from "react";
import { NodeApi, Tree } from "react-arborist";
import { createDragDropManager } from "dnd-core";
import { HTML5Backend } from "react-dnd-html5-backend";
import type {
    ExplorerNodeData,
    ExplorerNodeProps,
    ExplorerPinningOptions,
} from "../model";
import { ExplorerNode } from "./ExplorerNode";
import { ExplorerProvider, useExplorer } from "../model/ExplorerProvider";
import {
    activateSidebarPrimaryAction,
    focusSidebarElement,
    focusSidebarItemByFocusId,
    focusSidebarRelativeItem,
    handleSidebarSectionHotkey,
    restoreSidebarFocusById,
    focusSidebarSectionHeader,
    LoaderIcon,
    useResizeObserver,
} from "@serene-ui/shared-frontend/shared";

interface ExplorerProps extends ExplorerPinningOptions {
    initialData: ExplorerNodeData[];
    searchTerm?: string;
    isDataFetched?: boolean;
    sidebarSectionId?: string;
    stateStorageKey?: string;
}

const EXPLORER_DND_MANAGER_KEY = "__SERENE_EXPLORER_DND_MANAGER__";
const EXPLORER_STATE_VERSION = 1;

type ExplorerOpenState = Record<string, boolean>;
type PersistedExplorerState = {
    version: typeof EXPLORER_STATE_VERSION;
    tree: ExplorerNodeData[];
    openState: ExplorerOpenState;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
    typeof value === "object" && value !== null;

const isOpenState = (value: unknown): value is ExplorerOpenState => {
    return (
        isRecord(value) &&
        Object.values(value).every((entry) => typeof entry === "boolean")
    );
};

const sanitizeExplorerNodeContext = (
    context: unknown,
): ExplorerNodeData["context"] | undefined => {
    if (!isRecord(context)) {
        return undefined;
    }

    const sanitized: ExplorerNodeData["context"] = {};

    Object.entries(context).forEach(([key, value]) => {
        if (key === "action") {
            return;
        }

        if (
            typeof value === "string" ||
            typeof value === "number" ||
            typeof value === "boolean"
        ) {
            sanitized[key as keyof NonNullable<ExplorerNodeData["context"]>] =
                value as never;
        }
    });

    return Object.keys(sanitized).length ? sanitized : undefined;
};

const sanitizeExplorerNode = (value: unknown): ExplorerNodeData | undefined => {
    if (
        !isRecord(value) ||
        typeof value.id !== "string" ||
        typeof value.name !== "string" ||
        typeof value.type !== "string" ||
        !(typeof value.parentId === "string" || value.parentId === null)
    ) {
        return undefined;
    }

    const children = Array.isArray(value.children)
        ? value.children.reduce<ExplorerNodeData[]>((accumulator, child) => {
              const sanitizedChild = sanitizeExplorerNode(child);

              if (sanitizedChild) {
                  accumulator.push(sanitizedChild);
              }

              return accumulator;
          }, [])
        : undefined;

    return {
        id: value.id,
        name: value.name,
        type: value.type,
        parentId: value.parentId,
        context: sanitizeExplorerNodeContext(value.context),
        isError: typeof value.isError === "boolean" ? value.isError : undefined,
        children,
    };
};

const readPersistedExplorerState = (
    storageKey?: string,
): PersistedExplorerState | null => {
    if (!storageKey || typeof window === "undefined") {
        return null;
    }

    try {
        const rawValue = window.localStorage.getItem(storageKey);
        if (!rawValue) {
            return null;
        }

        const parsedValue = JSON.parse(rawValue) as unknown;
        if (
            !isRecord(parsedValue) ||
            parsedValue.version !== EXPLORER_STATE_VERSION ||
            !Array.isArray(parsedValue.tree) ||
            !isOpenState(parsedValue.openState)
        ) {
            return null;
        }

        return {
            version: EXPLORER_STATE_VERSION,
            tree: parsedValue.tree.reduce<ExplorerNodeData[]>(
                (accumulator, node) => {
                    const sanitizedNode = sanitizeExplorerNode(node);

                    if (sanitizedNode) {
                        accumulator.push(sanitizedNode);
                    }

                    return accumulator;
                },
                [],
            ),
            openState: parsedValue.openState,
        };
    } catch (error) {
        console.warn("Failed to restore explorer state:", error);
        return null;
    }
};

const getNodeIdentity = (node: ExplorerNodeData) =>
    JSON.stringify({
        name: node.name,
        type: node.type,
        context: sanitizeExplorerNodeContext(node.context) ?? null,
    });

const mergePersistedTreeWithInitialData = (
    initialData: ExplorerNodeData[],
    persistedTree: ExplorerNodeData[],
) => {
    const persistedById = new Map(persistedTree.map((node) => [node.id, node]));
    const persistedByIdentity = new Map(
        persistedTree.map((node) => [getNodeIdentity(node), node]),
    );

    return initialData.map((node) => {
        const persistedNode =
            persistedById.get(node.id) ??
            persistedByIdentity.get(getNodeIdentity(node));

        if (!persistedNode) {
            return node;
        }

        return {
            ...node,
            children: persistedNode.children,
            isError: persistedNode.isError,
        };
    });
};

type GlobalWithExplorerDndManager = typeof globalThis & {
    [EXPLORER_DND_MANAGER_KEY]?: ReturnType<typeof createDragDropManager>;
};
const globalWithExplorerDndManager = globalThis as GlobalWithExplorerDndManager;
const explorerDndManager =
    globalWithExplorerDndManager[EXPLORER_DND_MANAGER_KEY] ||
    (globalWithExplorerDndManager[EXPLORER_DND_MANAGER_KEY] =
        createDragDropManager(HTML5Backend));

const WrappedExplorer = forwardRef<HTMLDivElement, ExplorerProps>(
    (
        {
            initialData,
            searchTerm,
            isDataFetched,
            sidebarSectionId,
            stateStorageKey,
        },
        ref,
    ) => {
        const { ref: resizeRef, size } = useResizeObserver();
        const { treeRef, currentTree, setCurrentTree } = useExplorer();
        const persistedStateRef = useRef<PersistedExplorerState | null>(
            readPersistedExplorerState(stateStorageKey),
        );

        useEffect(() => {
            if (stateStorageKey && !isDataFetched) {
                return;
            }

            setCurrentTree((currentTree) => {
                if (!stateStorageKey) {
                    return initialData;
                }

                const sourceTree = currentTree.length
                    ? currentTree
                    : persistedStateRef.current?.tree;

                if (!sourceTree?.length) {
                    return initialData;
                }

                return mergePersistedTreeWithInitialData(
                    initialData,
                    sourceTree,
                );
            });
        }, [initialData, isDataFetched, setCurrentTree, stateStorageKey]);

        const persistExplorerState = useCallback(
            (tree: ExplorerNodeData[]) => {
                if (
                    !stateStorageKey ||
                    !isDataFetched ||
                    typeof window === "undefined"
                ) {
                    return;
                }

                try {
                    const nextState: PersistedExplorerState = {
                        version: EXPLORER_STATE_VERSION,
                        tree,
                        openState:
                            treeRef.current?.openState ??
                            persistedStateRef.current?.openState ??
                            {},
                    };

                    window.localStorage.setItem(
                        stateStorageKey,
                        JSON.stringify(nextState),
                    );
                    persistedStateRef.current = nextState;
                } catch (error) {
                    console.warn("Failed to persist explorer state:", error);
                }
            },
            [isDataFetched, stateStorageKey, treeRef],
        );

        useEffect(() => {
            persistExplorerState(currentTree);
        }, [currentTree, persistExplorerState]);

        const handleRef = useCallback(
            (el: HTMLDivElement | null) => {
                resizeRef(el);
                if (typeof ref === "function") {
                    ref(el);
                } else if (ref) {
                    ref.current = el;
                }
            },
            [resizeRef, ref],
        );

        return (
            <div className="h-full w-full" ref={handleRef}>
                {!isDataFetched && (
                    <div className="flex h-full items-center justify-center p-2">
                        <LoaderIcon className="size-4 animate-spin opacity-60" />
                    </div>
                )}
                {size.width > 0 && size.height > 0 && isDataFetched && (
                    <Tree
                        className="scrollbar"
                        searchTerm={searchTerm}
                        indent={8}
                        renderRow={(props) => (
                            <Row
                                {...props}
                                sidebarSectionId={sidebarSectionId}
                            />
                        )}
                        width={size.width}
                        height={size.height}
                        rowHeight={28}
                        ref={treeRef}
                        dndManager={explorerDndManager}
                        initialOpenState={persistedStateRef.current?.openState}
                        onToggle={() => persistExplorerState(currentTree)}
                        data={currentTree}>
                        {Node}
                    </Tree>
                )}
            </div>
        );
    },
);

export const Explorer = forwardRef<HTMLDivElement, ExplorerProps>(
    (
        {
            initialData,
            searchTerm,
            isDataFetched,
            sidebarSectionId,
            enablePinning,
            isNodePinned,
            onTogglePinned,
            stateStorageKey,
        },
        ref,
    ) => {
        return (
            <ExplorerProvider
                enablePinning={enablePinning}
                isNodePinned={isNodePinned}
                onTogglePinned={onTogglePinned}>
                <WrappedExplorer
                    ref={ref}
                    initialData={initialData}
                    searchTerm={searchTerm}
                    isDataFetched={isDataFetched}
                    sidebarSectionId={sidebarSectionId}
                    stateStorageKey={stateStorageKey}
                />
            </ExplorerProvider>
        );
    },
);

export const Node = (props: ExplorerNodeProps) => {
    return <ExplorerNode nodeData={props} />;
};

export const Row = ({
    children,
    node,
    innerRef,
    attrs,
    sidebarSectionId,
}: {
    node: NodeApi<ExplorerNodeData>;
    innerRef: (el: HTMLDivElement | null) => void;
    attrs: HTMLAttributes<any>;
    children: ReactElement;
    sidebarSectionId?: string;
}) => {
    const handleKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
        if (handleSidebarSectionHotkey(e)) {
            return;
        }

        if (e.target !== e.currentTarget) {
            return;
        }

        const currentElement = e.currentTarget;
        const currentFocusId = currentElement.dataset.sidebarFocusId;
        const isNextItemKey =
            e.key === "ArrowDown" || e.key.toLowerCase() === "j";
        const isPreviousItemKey =
            e.key === "ArrowUp" || e.key.toLowerCase() === "k";
        const isExpandKey =
            e.key === "ArrowRight" || e.key.toLowerCase() === "l";
        const isCollapseKey =
            e.key === "ArrowLeft" || e.key.toLowerCase() === "h";

        if (e.key === "Escape") {
            currentElement.blur();
            return;
        }

        if (isNextItemKey || isPreviousItemKey) {
            e.preventDefault();
            e.stopPropagation();
            focusSidebarRelativeItem(
                currentElement,
                isNextItemKey ? "next" : "previous",
            );
            return;
        }

        if (e.key === "Enter") {
            e.preventDefault();
            e.stopPropagation();
            activateSidebarPrimaryAction(currentElement);

            if (currentFocusId) {
                restoreSidebarFocusById(currentFocusId);
            }
            return;
        }

        if (isExpandKey) {
            e.preventDefault();
            e.stopPropagation();

            if (node.isOpen) {
                focusSidebarRelativeItem(currentElement, "next");
                return;
            }

            activateSidebarPrimaryAction(currentElement);

            if (currentFocusId) {
                restoreSidebarFocusById(currentFocusId);
            }
            return;
        }

        if (!isCollapseKey) {
            return;
        }

        e.preventDefault();
        e.stopPropagation();

        if (node.isOpen) {
            activateSidebarPrimaryAction(currentElement);

            if (currentFocusId) {
                restoreSidebarFocusById(currentFocusId);
            }
            return;
        }

        if (
            sidebarSectionId &&
            node.data.parentId &&
            focusSidebarItemByFocusId(
                `${sidebarSectionId}:${node.data.parentId}`,
                { shouldScrollIntoView: true },
            )
        ) {
            return;
        }

        focusSidebarSectionHeader(currentElement);
    };

    return (
        <div
            {...attrs}
            className="focus:outline-none focus:bg-accent hover:bg-accent/100"
            tabIndex={0}
            ref={innerRef}
            data-sidebar-focus-id={
                sidebarSectionId ? `${sidebarSectionId}:${node.id}` : undefined
            }
            data-sidebar-section-id={sidebarSectionId}
            onKeyDown={handleKeyDown}
            onClick={(event) => {
                const currentFocusId =
                    event.currentTarget.dataset.sidebarFocusId;
                focusSidebarElement(event.currentTarget);
                node.handleClick(event);

                if (currentFocusId) {
                    restoreSidebarFocusById(currentFocusId);
                }
            }}>
            {children}
        </div>
    );
};
