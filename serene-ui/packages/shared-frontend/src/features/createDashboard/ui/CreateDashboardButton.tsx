import React from "react";
import { toast } from "sonner";

import { useAddDashboard } from "../../../entities/dashboard";
import { Button, getErrorMessage, PlusIcon } from "../../../shared";

const DEFAULT_DASHBOARD_NAME = "Untitled";

interface CreateDashboardButtonProps {
    onCreateDashboard?: (dashboardId: number) => void;
}

export const CreateDashboardButton: React.FC<CreateDashboardButtonProps> = ({
    onCreateDashboard,
}) => {
    const { mutateAsync: addDashboard, isPending } = useAddDashboard();

    const handleCreateDashboard = async () => {
        try {
            const dashboard = await addDashboard({
                name: DEFAULT_DASHBOARD_NAME,
                auto_refresh: false,
                refresh_interval: 60,
                row_limit: 1000,
                blocks: [],
            });

            toast.success("Dashboard created", {
                description: dashboard.name,
            });
            onCreateDashboard?.(dashboard.id);
        } catch (error) {
            const message = getErrorMessage(
                error,
                "Failed to create dashboard",
            );

            toast.error("Failed to create dashboard", {
                description:
                    message === "Failed to create dashboard"
                        ? undefined
                        : message,
                action: {
                    label: "Close",
                    onClick: (event) => {
                        event.stopPropagation();
                    },
                },
            });
        }
    };

    return (
        <Button
            type="button"
            size="iconSmall"
            variant="ghost"
            title="Create dashboard"
            disabled={isPending}
            onClick={handleCreateDashboard}>
            <PlusIcon className="size-2.5 text-foreground/50" />
        </Button>
    );
};
