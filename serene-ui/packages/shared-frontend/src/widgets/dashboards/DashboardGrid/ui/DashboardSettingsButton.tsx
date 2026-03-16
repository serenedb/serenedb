import React from "react";
import type { DashboardSchema } from "@serene-ui/shared-core";
import {
    Button,
    Checkbox,
    DashboardSettingsIcon,
    Input,
    Label,
    Popover,
    PopoverContent,
    PopoverTrigger,
} from "@serene-ui/shared-frontend";
import { useUpdateDashboard } from "../../../../entities/dashboard";
import { getErrorMessage } from "../../../../shared";
import { toast } from "sonner";

interface DashboardSettingsButtonProps {
    currentDashboard?: DashboardSchema | null;
}

const DASHBOARD_SETTINGS_DEBOUNCE_MS = 400;

export const DashboardSettingsButton: React.FC<
    DashboardSettingsButtonProps
> = ({ currentDashboard }) => {
    const [open, setOpen] = React.useState(false);
    const [draft, setDraft] = React.useState({
        auto_refresh: false,
        refresh_interval: "60",
        row_limit: "1000",
    });
    const { mutateAsync: updateDashboard } = useUpdateDashboard();

    React.useEffect(() => {
        if (!currentDashboard) {
            return;
        }

        setDraft({
            auto_refresh: currentDashboard.auto_refresh,
            refresh_interval: String(currentDashboard.refresh_interval ?? 60),
            row_limit: String(currentDashboard.row_limit ?? 1000),
        });
    }, [
        currentDashboard?.id,
        currentDashboard?.auto_refresh,
        currentDashboard?.refresh_interval,
        currentDashboard?.row_limit,
    ]);

    React.useEffect(() => {
        if (!currentDashboard) {
            return;
        }

        const nextRefreshInterval = Number(draft.refresh_interval);
        const nextRowLimit = Number(draft.row_limit);

        if (
            !Number.isFinite(nextRefreshInterval) ||
            !Number.isInteger(nextRefreshInterval) ||
            nextRefreshInterval <= 0 ||
            !Number.isFinite(nextRowLimit) ||
            !Number.isInteger(nextRowLimit) ||
            nextRowLimit <= 0
        ) {
            return;
        }

        if (
            currentDashboard.auto_refresh === draft.auto_refresh &&
            currentDashboard.refresh_interval === nextRefreshInterval &&
            currentDashboard.row_limit === nextRowLimit
        ) {
            return;
        }

        const timeoutId = window.setTimeout(() => {
            void updateDashboard({
                id: currentDashboard.id,
                auto_refresh: draft.auto_refresh,
                refresh_interval: nextRefreshInterval,
                row_limit: nextRowLimit,
            }).catch((error) => {
                toast.error("Failed to update dashboard settings", {
                    description: getErrorMessage(
                        error,
                        "Failed to update dashboard settings.",
                    ),
                });
            });
        }, DASHBOARD_SETTINGS_DEBOUNCE_MS);

        return () => {
            window.clearTimeout(timeoutId);
        };
    }, [currentDashboard, draft, updateDashboard]);

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="secondary"
                    size="icon"
                    className="size-9"
                    title="Dashboard settings">
                    <DashboardSettingsIcon />
                </Button>
            </PopoverTrigger>
            <PopoverContent
                align="end"
                side="top"
                variant="secondary"
                className="min-w-80 p-4">
                <div className="flex flex-col gap-4">
                    <div className="flex items-center justify-between gap-3">
                        <div className="flex flex-col gap-1">
                            <Label htmlFor="dashboard-auto-refresh">
                                Auto refresh
                            </Label>
                            <p className="text-xs text-muted-foreground">
                                Refresh all query cards automatically.
                            </p>
                        </div>
                        <Checkbox
                            id="dashboard-auto-refresh"
                            checked={draft.auto_refresh}
                            onCheckedChange={(checked) => {
                                setDraft((currentValue) => ({
                                    ...currentValue,
                                    auto_refresh: checked === true,
                                }));
                            }}
                        />
                    </div>
                    <div className="flex flex-col gap-2">
                        <Label htmlFor="dashboard-refresh-interval">
                            Refresh interval, sec
                        </Label>
                        <Input
                            id="dashboard-refresh-interval"
                            type="number"
                            min={1}
                            step={1}
                            value={draft.refresh_interval}
                            onChange={(event) => {
                                setDraft((currentValue) => ({
                                    ...currentValue,
                                    refresh_interval: event.target.value,
                                }));
                            }}
                        />
                    </div>
                    <div className="flex flex-col gap-2">
                        <Label htmlFor="dashboard-row-limit">Row limit</Label>
                        <Input
                            id="dashboard-row-limit"
                            type="number"
                            min={1}
                            step={1}
                            value={draft.row_limit}
                            onChange={(event) => {
                                setDraft((currentValue) => ({
                                    ...currentValue,
                                    row_limit: event.target.value,
                                }));
                            }}
                        />
                    </div>
                </div>
            </PopoverContent>
        </Popover>
    );
};
