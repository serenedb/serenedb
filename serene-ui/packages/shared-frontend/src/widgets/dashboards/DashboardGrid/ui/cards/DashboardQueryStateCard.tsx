import React from "react";
import { Button } from "../../../../../shared/ui";
import { DashboardChartCardBase } from "./DashboardChartCardBase";

interface DashboardQueryStateCardProps {
    name?: string;
    description?: string;
    title: string;
    details?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

export const DashboardQueryStateCard: React.FC<DashboardQueryStateCardProps> = ({
    name,
    description,
    title,
    details,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
    return (
        <DashboardChartCardBase
            name={name}
            description={description}
            onDelete={onDelete}
            onDuplicate={onDuplicate}
            onEdit={onEdit}>
            <div className="flex min-h-0 flex-1 items-center justify-center p-4">
                <div className="flex max-w-64 flex-col items-center gap-3 text-center">
                    <p className="text-sm font-medium text-primary-foreground">{title}</p>
                    {details ? (
                        <p className="text-xs text-muted-foreground">{details}</p>
                    ) : null}
                    {onEdit ? (
                        <Button
                            type="button"
                            size="small"
                            variant="secondary"
                            data-testid="dashboardQueryStateCard-editButton"
                            onMouseDown={(event) => {
                                event.stopPropagation();
                            }}
                            onClick={onEdit}>
                            Edit
                        </Button>
                    ) : null}
                </div>
            </div>
        </DashboardChartCardBase>
    );
};
