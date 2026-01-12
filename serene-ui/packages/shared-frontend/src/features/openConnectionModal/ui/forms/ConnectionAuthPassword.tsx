import { Input, Label } from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";

export const ConnectionAuthPassword = () => {
    const { currentConnection, handleInputChange } = useConnectionsModal();

    if (currentConnection.authMethod !== "password") {
        return null;
    }

    return (
        <>
            <div className="flex flex-col gap-2">
                <Label htmlFor="user">User</Label>
                <Input
                    value={currentConnection.user || ""}
                    id="user"
                    placeholder="postgres"
                    onChange={handleInputChange}
                    autoComplete={"off"}
                />
            </div>
            <div className="flex flex-col gap-2">
                <Label htmlFor="password">Password</Label>
                <Input
                    onChange={handleInputChange}
                    value={currentConnection.password || ""}
                    placeholder=""
                    className="txtPassword"
                    id="password"
                    autoComplete={"off"}
                />
            </div>
        </>
    );
};
