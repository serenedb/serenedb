import React from "react";
import { toast } from "sonner";

import { useAddDashboard } from "../../../entities/dashboard";
import {
    Button,
    Dialog,
    DialogClose,
    DialogContent,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
    getErrorMessage,
    Input,
    PlusIcon,
} from "../../../shared";

const DEFAULT_DASHBOARD_NAME = "Untitled";

interface CreateDashboardButtonProps {
    onCreateDashboard?: (dashboardId: number) => void;
}

export const CreateDashboardButton: React.FC<CreateDashboardButtonProps> = ({
    onCreateDashboard,
}) => {
    const [open, setOpen] = React.useState(false);
    const [name, setName] = React.useState(DEFAULT_DASHBOARD_NAME);
    const { mutateAsync: addDashboard, isPending } = useAddDashboard();

    React.useEffect(() => {
        if (!open) {
            return;
        }

        setName(DEFAULT_DASHBOARD_NAME);
    }, [open]);

    const handleCreateDashboard = async () => {
        const dashboardName = name.trim();

        if (!dashboardName) {
            return;
        }

        try {
            const dashboard = await addDashboard({
                name: dashboardName,
                favorite: false,
                auto_refresh: false,
                refresh_interval: 60,
                row_limit: 1000,
                blocks: [],
            });

            toast.success("Dashboard created", {
                description: dashboard.name,
            });
            onCreateDashboard?.(dashboard.id);
            setOpen(false);
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
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
                <Button
                    type="button"
                    size="xsIcon"
                    className="text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0"
                    variant="ghost"
                    title="Create dashboard"
                    disabled={isPending}>
                    <PlusIcon className="size-2.5 text-foreground/50" />
                </Button>
            </DialogTrigger>
            <DialogContent className="max-w-sm">
                <form
                    className="grid gap-4"
                    onSubmit={(event) => {
                        event.preventDefault();
                        void handleCreateDashboard();
                    }}>
                    <DialogHeader>
                        <DialogTitle>Create dashboard</DialogTitle>
                    </DialogHeader>
                    <Input
                        value={name}
                        onChange={(event) => setName(event.target.value)}
                        placeholder="Name"
                        disabled={isPending}
                        autoFocus
                    />
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
                            type="submit"
                            disabled={isPending || !name.trim()}>
                            Create
                        </Button>
                    </DialogFooter>
                </form>
            </DialogContent>
        </Dialog>
    );
};
