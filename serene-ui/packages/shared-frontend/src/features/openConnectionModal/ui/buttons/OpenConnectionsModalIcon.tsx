import { Button, EditIcon } from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";

interface OpenConnectionsModalIconProps {
    className?: React.ComponentProps<typeof Button>["className"];
}

export const OpenConnectionsModalIcon: React.FC<
    OpenConnectionsModalIconProps
> = ({ className, ...props }) => {
    const { setOpen } = useConnectionsModal();
    return (
        <Button
            variant={"secondary"}
            onClick={() => setOpen(true)}
            size={"icon"}
            className={className}
            aria-label="Edit connections"
            {...props}>
            <EditIcon />
        </Button>
    );
};
