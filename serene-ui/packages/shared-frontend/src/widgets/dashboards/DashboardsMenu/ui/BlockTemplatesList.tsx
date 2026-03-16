import React from "react";

import { DashboardsMenuSection } from "./DashboardsMenuSection";
import { DashboardExplorer } from "./DashboardExplorer";
type BlockTemplatesListProps = {
    bodyHeight: number;
    showResizeHandle?: boolean;
    onResizePointerDown?: (event: React.PointerEvent<HTMLDivElement>) => void;
};

export function BlockTemplatesList({
    bodyHeight,
    showResizeHandle = false,
    onResizePointerDown,
}: BlockTemplatesListProps) {
    return (
        <DashboardsMenuSection
            sectionId="blockTemplates"
            title="Block Templates"
            bodyHeight={bodyHeight}
            showResizeHandle={showResizeHandle}
            onResizePointerDown={onResizePointerDown}>
            <DashboardExplorer
                initialData={[]}
                isDataFetched
                emptyState="No block templates yet"
            />
        </DashboardsMenuSection>
    );
}
