import React from "react";
import { ChevronDownIcon } from "lucide-react";

import {
    Button,
    Popover,
    PopoverContent,
    PopoverTrigger,
} from "../../../../shared/ui";

const SCALE_OPTIONS = [0.5, 0.75, 1] as const;
type DashboardScaleOption = (typeof SCALE_OPTIONS)[number];

interface DashboardScaleButtonProps {
    scale: DashboardScaleOption;
    onScaleChange: (scale: DashboardScaleOption) => void;
}

export const DashboardScaleButton: React.FC<DashboardScaleButtonProps> = ({
    scale,
    onScaleChange,
}) => {
    const [open, setOpen] = React.useState(false);

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="secondary"
                    className="h-9 min-w-20 justify-between rounded-lg px-3 bg-[#282828] hover:bg-[#282828] border-1"
                    data-testid="dashboardScaleButton-trigger"
                    title="Dashboard scale">
                    <span>{Math.round(scale * 100)}%</span>
                    <ChevronDownIcon className="size-4" />
                </Button>
            </PopoverTrigger>
            <PopoverContent
                align="start"
                side="top"
                variant="secondary"
                className="w-24 p-1 shadow-none">
                <div className="flex flex-col gap-1">
                    {SCALE_OPTIONS.map((option) => {
                        const isActive = option === scale;

                        return (
                            <Button
                                key={option}
                                variant={isActive ? "default" : "ghost"}
                                data-testid={`dashboardScaleButton-option-${Math.round(option * 100)}`}
                                className="justify-start rounded-md px-2"
                                onClick={() => {
                                    onScaleChange(option);
                                    setOpen(false);
                                }}>
                                {Math.round(option * 100)}%
                            </Button>
                        );
                    })}
                </div>
            </PopoverContent>
        </Popover>
    );
};
