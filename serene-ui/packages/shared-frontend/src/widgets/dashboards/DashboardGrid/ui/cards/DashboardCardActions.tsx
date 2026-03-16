import { useState } from "react";
import {
    Button,
    CopyIcon,
    EditIcon,
    Popover,
    PopoverContent,
    PopoverTrigger,
    ThreeDotsIcon,
    TrashIcon,
} from "@serene-ui/shared-frontend";
import { cn } from "../../../../../shared/lib/utils";

interface DashboardCardActionsProps {
    className?: string;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
    onEdit?: () => void;
}

export const DashboardCardActions: React.FC<DashboardCardActionsProps> = ({
    className,
    onDelete,
    onDuplicate,
    onEdit,
}) => {
    const [open, setOpen] = useState(false);

    const handleAction = async (action?: () => void | Promise<void>) => {
        if (!action) {
            return;
        }

        setOpen(false);
        await action();
    };

    return (
        <div className={cn("dashboard-card-actions", className)}>
            <Popover open={open} onOpenChange={setOpen}>
                <PopoverTrigger asChild>
                    <Button
                        size="icon"
                        variant="ghost"
                        title="Card actions"
                        onMouseDown={(event) => {
                            event.stopPropagation();
                        }}>
                        <ThreeDotsIcon />
                    </Button>
                </PopoverTrigger>
                <PopoverContent
                    align="end"
                    side="top"
                    variant="secondary"
                    className="min-w-44 p-1 shadow-none">
                    <div className="flex flex-col gap-1">
                        <Button
                            type="button"
                            variant="ghost"
                            className="h-8 w-full justify-start px-2.5 text-xs"
                            disabled={!onEdit}
                            onClick={() => void handleAction(onEdit)}>
                            <EditIcon className="size-3.5" />
                            Edit
                        </Button>
                        <Button
                            type="button"
                            variant="ghost"
                            className="h-8 w-full justify-start px-2.5 text-xs"
                            disabled={!onDuplicate}
                            onClick={() => void handleAction(onDuplicate)}>
                            <CopyIcon className="size-3.5" />
                            Duplicate
                        </Button>
                        <Button
                            type="button"
                            variant="ghost"
                            className="h-8 w-full justify-start px-2.5 text-xs text-destructive hover:text-destructive"
                            disabled={!onDelete}
                            onClick={() => void handleAction(onDelete)}>
                            <TrashIcon className="size-3.5" />
                            Delete
                        </Button>
                    </div>
                </PopoverContent>
            </Popover>
        </div>
    );
};
