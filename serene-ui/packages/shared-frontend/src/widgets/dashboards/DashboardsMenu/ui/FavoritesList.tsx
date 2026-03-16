import React from "react";

import { DashboardsMenuSection } from "./DashboardsMenuSection";
import { DashboardExplorer } from "./DashboardExplorer";
type FavoritesListProps = {
    bodyHeight: number;
    showResizeHandle?: boolean;
    onResizePointerDown?: (event: React.PointerEvent<HTMLDivElement>) => void;
};

export function FavoritesList({
    bodyHeight,
    showResizeHandle = false,
    onResizePointerDown,
}: FavoritesListProps) {
    return (
        <DashboardsMenuSection
            sectionId="favorites"
            title="Favorites"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}>
            <DashboardExplorer
                initialData={[]}
                isDataFetched
                emptyState="No favorites yet"
            />
        </DashboardsMenuSection>
    );
}
