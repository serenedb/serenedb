import React from "react";

import { DashboardsMenuSection } from "./DashboardsMenuSection";
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
            onResizePointerDown={onResizePointerDown}
        />
    );
}
