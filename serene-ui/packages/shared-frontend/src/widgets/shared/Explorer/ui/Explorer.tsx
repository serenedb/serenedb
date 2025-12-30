import React, {
    useEffect,
    forwardRef,
    useCallback,
    type HTMLAttributes,
    type ReactElement,
} from "react";
import { NodeApi, Tree } from "react-arborist";
import type { ExplorerNodeData, ExplorerNodeProps } from "../model";
import { ExplorerNode } from "./ExplorerNode";
import { ExplorerProvider, useExplorer } from "../model/ExplorerProvider";
import { Skeleton, useResizeObserver } from "@serene-ui/shared-frontend/shared";

interface ExplorerProps {
    initialData: ExplorerNodeData[];
    searchTerm?: string;
    isDataFetched?: boolean;
}

const WrappedExplorer = forwardRef<HTMLDivElement, ExplorerProps>(
    ({ initialData, searchTerm, isDataFetched }, ref) => {
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
            <div className="h-full" ref={handleRef}>
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
                        renderRow={Row}
                        width={size.width}
                        height={size.height}
                        rowHeight={28}
                        ref={treeRef}
                        data={currentTree}>
                        {Node}
                    </Tree>
                )}
            </div>
        );
    },
);

export const Explorer = forwardRef<HTMLDivElement, ExplorerProps>(
    ({ initialData, searchTerm, isDataFetched }, ref) => {
        return (
            <ExplorerProvider>
                <WrappedExplorer
                    ref={ref}
                    initialData={initialData}
                    searchTerm={searchTerm}
                    isDataFetched={isDataFetched}
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
}: {
    node: NodeApi<ExplorerNodeData>;
    innerRef: (el: HTMLDivElement | null) => void;
    attrs: HTMLAttributes<any>;
    children: ReactElement;
}) => {
    return (
        <div
            {...attrs}
            className="focus:outline-none focus:bg-secondary hover:bg-secondary/100"
            tabIndex={0}
            ref={innerRef}
            onKeyDown={(e) => {
                if (e.key === "Escape") {
                    e.currentTarget.blur();
                } else if (["Enter", "ArrowRight"].includes(e.key)) {
                    e.preventDefault();
                    e.stopPropagation();
                    const button = e.currentTarget.querySelector("button");
                    if (button) {
                        button.click();
                    }
                }
            }}
            onClick={node.handleClick}>
            {children}
        </div>
    );
};
