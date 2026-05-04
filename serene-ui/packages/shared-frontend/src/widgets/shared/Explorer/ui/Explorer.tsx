import {
    useEffect,
    forwardRef,
    useCallback,
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
    Skeleton,
    useResizeObserver,
} from "@serene-ui/shared-frontend/shared";

interface ExplorerProps extends ExplorerPinningOptions {
    initialData: ExplorerNodeData[];
    searchTerm?: string;
    isDataFetched?: boolean;
    sidebarSectionId?: string;
}

const EXPLORER_DND_MANAGER_KEY = "__SERENE_EXPLORER_DND_MANAGER__";
type GlobalWithExplorerDndManager = typeof globalThis & {
    [EXPLORER_DND_MANAGER_KEY]?: ReturnType<typeof createDragDropManager>;
};
const globalWithExplorerDndManager = globalThis as GlobalWithExplorerDndManager;
const explorerDndManager =
    globalWithExplorerDndManager[EXPLORER_DND_MANAGER_KEY] ||
    (globalWithExplorerDndManager[EXPLORER_DND_MANAGER_KEY] =
        createDragDropManager(HTML5Backend));

const WrappedExplorer = forwardRef<HTMLDivElement, ExplorerProps>(
    ({ initialData, searchTerm, isDataFetched, sidebarSectionId }, ref) => {
        const { ref: resizeRef, size } = useResizeObserver();
        const { treeRef, currentTree, setCurrentTree } = useExplorer();

        useEffect(() => {
            setCurrentTree(initialData);
        }, [initialData]);

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
                    <div className="flex flex-col items-center justify-center gap-3 mt-1.5 ">
                        <div className="flex gap-1.5 mr-auto ml-2 items-center">
                            <Skeleton className="h-3 w-3" />
                            <Skeleton className="h-4 w-4 rounded-xs" />
                            <Skeleton className="h-4 w-15" />
                        </div>
                        <div className="flex gap-1.5 mr-auto ml-2 items-center">
                            <Skeleton className="h-3 w-3" />
                            <Skeleton className="h-4 w-4 rounded-xs" />
                            <Skeleton className="h-4 w-20" />
                        </div>
                        <div className="flex gap-1.5 mr-auto ml-7 items-center">
                            <Skeleton className="h-3 w-3" />
                            <Skeleton className="h-4 w-4 rounded-xs" />
                            <Skeleton className="h-4 w-30" />
                        </div>
                        <div className="flex gap-1.5 mr-auto ml-7 items-center">
                            <Skeleton className="h-3 w-3" />
                            <Skeleton className="h-4 w-4 rounded-xs" />
                            <Skeleton className="h-4 w-20" />
                        </div>
                    </div>
                )}
                {size.width > 0 && size.height > 0 && isDataFetched && (
                    <Tree
                        className="scrollbar fade-in"
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
                const currentFocusId = event.currentTarget.dataset.sidebarFocusId;
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
