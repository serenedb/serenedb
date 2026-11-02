import { Input, Label } from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";

export const ConnectionHostMode = () => {
    const { handleInputChange, currentConnection } = useConnectionsModal();

    if (currentConnection.mode !== "host") {
        return null;
    }

    return (
        <>
            <div className="flex flex-col gap-2">
                <Label htmlFor="host">Host</Label>
                <Input
                    value={currentConnection.host}
                    onChange={handleInputChange}
                    id="host"
                    autoComplete={"off"}
                    placeholder="localhost"
                />
            </div>
            <div className="flex flex-col gap-2">
                <Label htmlFor="port">Port</Label>
                <Input
                    value={currentConnection.port}
                    onChange={handleInputChange}
                    id="port"
                    autoComplete={"off"}
                    placeholder="5432"
                />
            </div>
        </>
    );
};
