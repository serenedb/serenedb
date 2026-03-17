import React from "react";

import { useGetSavedQueries } from "@serene-ui/shared-frontend/entities";
import { TreeQueryIcon } from "@serene-ui/shared-frontend/shared";
import {
    createDashboardSavedQueryDragPayload,
    setDashboardSavedQueryDragActive,
    setDashboardSavedQueryDragData,
} from "../../model/savedQueryDrag";
import { ExplorerNodeButton } from "../../../shared/Explorer/ui/ExplorerNodeButton";
import { DashboardsMenuSection } from "./DashboardsMenuSection";

type SavedQueriesListProps = {
    bodyHeight: number;
    showResizeHandle?: boolean;
    onResizePointerDown?: (event: React.PointerEvent<HTMLDivElement>) => void;
};

export function SavedQueriesList({
    bodyHeight,
    showResizeHandle = false,
    onResizePointerDown,
}: SavedQueriesListProps) {
    const {
        data: savedQueries,
        isFetched: isDataFetched,
        isLoading: isDataLoading,
    } = useGetSavedQueries();

    const hasQueries = (savedQueries?.length ?? 0) > 0;

    return (
        <DashboardsMenuSection
            sectionId="savedQueries"
            title="Saved Queries"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}>
            <div className="flex h-full min-h-0 flex-col p-1">
                {!hasQueries && isDataFetched && !isDataLoading ? (
                    <div className="flex flex-1 items-center justify-center p-2">
                        <p className="text-center text-xs text-foreground/70">
                            No saved queries yet
                        </p>
                    </div>
                ) : (
                    <div className="flex min-h-0 flex-1 flex-col gap-1 overflow-auto pr-1">
                        {(savedQueries ?? []).map((savedQuery) => (
                            <div
                                key={savedQuery.id}
                                className="focus:bg-secondary hover:bg-secondary/100"
                                draggable
                                onDragStart={(event) => {
                                    event.stopPropagation();
                                    const payload =
                                        createDashboardSavedQueryDragPayload(
                                            savedQuery,
                                        );

                                    setDashboardSavedQueryDragActive(true);
                                    setDashboardSavedQueryDragData(
                                        event.dataTransfer,
                                        payload,
                                    );
                                    event.dataTransfer.effectAllowed = "copy";
                                }}
                                onDragEnd={() => {
                                    setDashboardSavedQueryDragActive(false);
                                }}>
                                <ExplorerNodeButton
                                    className="pl-[23px]"
                                    title={savedQuery.name}
                                    onClick={() => {}}
                                    open={false}
                                    icon={<TreeQueryIcon />}
                                    showArrow={false}
                                />
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </DashboardsMenuSection>
    );
}
