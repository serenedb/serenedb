import React, { useEffect } from "react";

import { DashboardsMenuSection } from "./DashboardsMenuSection";
import { DashboardExplorer } from "./DashboardExplorer";
import { useGetSavedQueries } from "@serene-ui/shared-frontend/entities";
import { ExplorerNodeData } from "@serene-ui/shared-frontend/widgets";
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

    const initialData: ExplorerNodeData[] = (savedQueries ?? []).map(
        (savedQuery) => ({
            id: `sq-${savedQuery.id}`,
            name: savedQuery.name,
            type: "saved-query",
            parentId: null,
        }),
    );

    return (
        <DashboardsMenuSection
            sectionId="savedQueries"
            title="Saved Queries"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}>
            <DashboardExplorer
                initialData={initialData}
                isDataFetched={isDataFetched && !isDataLoading}
                emptyState="No saved queries yet"
            />
        </DashboardsMenuSection>
    );
}
