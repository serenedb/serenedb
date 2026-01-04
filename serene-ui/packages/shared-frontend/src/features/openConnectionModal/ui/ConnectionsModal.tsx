import { Dialog, DialogContent } from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../model/ConnectionsModalContext";
import { ConnectionData } from "./forms/ConnectionData";
import { ConnectionsSidebar } from "./ConnectionsSidebar";

export const ConnectionsModal: React.FC = () => {
    const { open, setOpen } = useConnectionsModal();

    return (
        <Dialog open={open} onOpenChange={(value) => setOpen(value)}>
            <DialogContent
                showCloseButton={false}
                className="sm:max-w-[800px] w-full min-h-[560px] flex gap-0 p-0">
                <ConnectionsSidebar />
                <ConnectionData />
            </DialogContent>
        </Dialog>
    );
};
