import React from "react";

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
    return (
        <DashboardsMenuSection
            sectionId="savedQueries"
            title="Saved Queries"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}
        />
    );
}
