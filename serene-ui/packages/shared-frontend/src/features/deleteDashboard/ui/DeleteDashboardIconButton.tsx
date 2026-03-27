import React from "react";

import { useDeleteDashboard } from "../../../entities/dashboard";
import {
    Button,
    Dialog,
    DialogClose,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
    TrashIcon,
} from "../../../shared";

interface DeleteDashboardIconButtonProps {
    dashboardId: number;
    dashboardName: string;
}

export const DeleteDashboardIconButton: React.FC<
    DeleteDashboardIconButtonProps
> = ({ dashboardId, dashboardName }) => {
    const [open, setOpen] = React.useState(false);
    const { mutateAsync: deleteDashboard, isPending } = useDeleteDashboard();

    const stopPropagation = (event: React.SyntheticEvent) => {
        event.stopPropagation();
    };

    const handleDeleteDashboard = async (
        event: React.MouseEvent<HTMLButtonElement>,
    ) => {
        event.stopPropagation();

        await deleteDashboard({
            id: dashboardId,
        });

        setOpen(false);
    };

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
                <Button
                    type="button"
                    size="iconSmall"
                    variant="ghost"
                    title="Delete dashboard"
                    onPointerDown={stopPropagation}
                    onClick={stopPropagation}>
                    <TrashIcon className="size-3 text-foreground/50" />
                </Button>
            </DialogTrigger>
            <DialogContent
                className="max-w-sm"
                onPointerDown={stopPropagation}
                onClick={stopPropagation}>
                <DialogHeader>
                    <DialogTitle>Delete dashboard</DialogTitle>
                    <DialogDescription>
                        Are you sure that you want to delete "
                        {dashboardName}"?
                    </DialogDescription>
                </DialogHeader>
                <DialogFooter>
                    <DialogClose asChild>
                        <Button
                            type="button"
                            variant="outline"
                            disabled={isPending}>
                            Cancel
                        </Button>
                    </DialogClose>
                    <Button
                        type="button"
                        variant="destructive"
                        disabled={isPending}
                        onClick={(event) => void handleDeleteDashboard(event)}>
                        Delete
                    </Button>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
};
