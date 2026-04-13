import React from "react";
import { toast } from "sonner";
import type { SavedQuerySchema } from "@serene-ui/shared-core";
import { useDeleteSavedQuery } from "../../../entities";
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
    cn,
} from "../../../shared";

interface DeleteSavedQueryIconButtonProps {
    savedQuery: Pick<SavedQuerySchema, "id" | "name" | "query">;
    className?: React.ComponentProps<typeof Button>["className"];
    onDeleteSuccess?: (
        savedQuery: Pick<SavedQuerySchema, "id" | "name" | "query">,
    ) => void;
}

export const DeleteSavedQueryIconButton: React.FC<
    DeleteSavedQueryIconButtonProps
> = ({ savedQuery, className, onDeleteSuccess }) => {
    const [open, setOpen] = React.useState(false);
    const { mutateAsync: deleteSavedQuery, isPending } = useDeleteSavedQuery();

    const stopPropagation = (event: React.SyntheticEvent) => {
        event.stopPropagation();
    };

    const handleDeleteSavedQuery = async (
        event: React.MouseEvent<HTMLButtonElement>,
    ) => {
        event.preventDefault();
        event.stopPropagation();

        try {
            await deleteSavedQuery({ id: savedQuery.id });
            onDeleteSuccess?.(savedQuery);
            setOpen(false);
        } catch (error) {
            console.error(error);
            toast.error("Failed to delete saved query");
        }
    };

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
                <Button
                    type="button"
                    size="xsIcon"
                    variant="ghost"
                    className={cn(
                        "text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0",
                        className,
                    )}
                    title="Delete query"
                    disabled={isPending}
                    draggable={false}
                    onPointerDown={stopPropagation}
                    onClick={stopPropagation}>
                    <TrashIcon className="size-3" />
                </Button>
            </DialogTrigger>
            <DialogContent
                className="max-w-sm"
                onPointerDown={stopPropagation}
                onClick={stopPropagation}>
                <DialogHeader>
                    <DialogTitle>Delete saved query</DialogTitle>
                    <DialogDescription>
                        Are you sure that you want to delete "{savedQuery.name}
                        "?
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
                        onClick={(event) => void handleDeleteSavedQuery(event)}>
                        Delete
                    </Button>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
};
