import { Button } from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";

interface OpenConnectionsModalButtonProps {
    className?: React.ComponentProps<typeof Button>["className"];
}

export const OpenConnectionsModalButton: React.FC<
    OpenConnectionsModalButtonProps
> = ({ className, ...props }) => {
    const { setOpen } = useConnectionsModal();

    return (
        <Button
            title="add-connection"
            onClick={() => {
                setOpen(true);
            }}
            className={className}
            {...props}>
            Add server
        </Button>
    );
};
