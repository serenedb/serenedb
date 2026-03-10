import React from "react";

import { DashboardsMenuSection } from "./DashboardsMenuSection";
type DashboardsListProps = {
    bodyHeight: number;
    showResizeHandle?: boolean;
    onResizePointerDown?: (event: React.PointerEvent<HTMLDivElement>) => void;
};

export function DashboardsList({
    bodyHeight,
    showResizeHandle = false,
    onResizePointerDown,
}: DashboardsListProps) {
    return (
        <DashboardsMenuSection
            sectionId="dashboards"
            title="Dashboards"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}
        />
    );
}
