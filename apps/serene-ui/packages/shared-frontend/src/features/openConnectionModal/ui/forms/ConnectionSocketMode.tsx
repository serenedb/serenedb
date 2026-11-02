import { Input, Label } from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";

export const ConnectionSocketMode = () => {
    const { handleInputChange, currentConnection } = useConnectionsModal();

    if (currentConnection.mode !== "socket") {
        return null;
    }

    return (
        <>
            <div className="flex flex-col gap-2">
                <Label htmlFor="socket">Socket</Label>
                <Input
                    value={currentConnection.socket}
                    onChange={handleInputChange}
                    id="socket"
                    autoComplete={"off"}
                />
            </div>
        </>
    );
};
