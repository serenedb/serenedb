import React from "react";
import { DashboardCardActions } from "./DashboardCardActions";

interface DashboardChartCardBaseProps {
    children: React.ReactNode;
    name?: string;
    description?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

export const DashboardChartCardBase: React.FC<DashboardChartCardBaseProps> = ({
    children,
    name,
    description,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
    return (
        <div className="bg-background border-1 rounded-xs flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="flex min-h-0 flex-1 flex-col">{children}</div>
            <div className="flex justify-between items-center border-t-1  p-3">
                <div className="flex flex-col ">
                    <p className="uppercase text-xs font-extrabold text-primary-foreground">
                        {name}
                    </p>
                    <p className="text-xs text-muted-foreground">
                        {description}
                    </p>
                </div>
                <DashboardCardActions
                    onDelete={onDelete}
                    onDuplicate={onDuplicate}
                    onEdit={onEdit}
                />
            </div>
        </div>
    );
};
