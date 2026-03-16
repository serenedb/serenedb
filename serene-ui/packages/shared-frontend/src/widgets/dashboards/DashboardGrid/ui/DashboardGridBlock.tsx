import React from "react";
import type {
    DashboardBlockSchema,
    DashboardSchema,
} from "@serene-ui/shared-core";

import { DashboardQueryBlockCard } from "./DashboardQueryBlockCard";
import { DashboardTextCard } from "./cards/DashboardTextCard";

interface DashboardGridBlockProps {
    block: DashboardBlockSchema;
    dashboardId: number;
    dashboard?: DashboardSchema | null;
    isMoving: boolean;
    onDeleteBlock?: (block: DashboardBlockSchema) => void | Promise<void>;
    onDuplicateBlock?: (block: DashboardBlockSchema) => void | Promise<void>;
    onEditBlock?: (block: DashboardBlockSchema) => void;
}

export const DashboardGridBlock: React.FC<DashboardGridBlockProps> = ({
    block,
    dashboardId,
    dashboard,
    isMoving,
    onDeleteBlock,
    onDuplicateBlock,
    onEditBlock,
}) => {
    const handleDelete = () => onDeleteBlock?.(block);
    const handleDuplicate = () => onDuplicateBlock?.(block);
    const handleEdit = () => onEditBlock?.(block);

    switch (block.type) {
        case "text":
            return (
                <DashboardTextCard
                    block={block}
                    dashboardId={dashboardId}
                    onDelete={handleDelete}
                    onDuplicate={handleDuplicate}
                />
            );
        case "spacer":
            return <div className="flex-1" />;
        case "table":
        case "single_string":
        case "bar_chart":
        case "line_chart":
        case "pie_chart":
            return (
                <DashboardQueryBlockCard
                    block={block}
                    dashboard={dashboard}
                    isMoving={isMoving}
                    onDelete={handleDelete}
                    onDuplicate={handleDuplicate}
                    onEdit={handleEdit}
                />
            );
    }

    return null;
};
