import {
    Label,
    Select,
    SelectTrigger,
    KeyIcon,
    SelectValue,
    SelectContent,
    SelectGroup,
    SelectItem,
} from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";
import { ConnectionHostMode } from "./ConnectionHostMode";
import { ConnectionSocketMode } from "./ConnectionSocketMode";

export const ConnectionConnectMode = () => {
    const { handleSelectChange, currentConnection } = useConnectionsModal();
    return (
        <div className="flex-1 flex flex-col gap-4">
            <div className="flex flex-col gap-2">
                <Label htmlFor="mode">Connection mode</Label>
                <Select
                    value={currentConnection.mode}
                    onValueChange={(value) => {
                        handleSelectChange("mode", value);
                    }}>
                    <SelectTrigger className="w-full">
                        <div className="flex gap-2.5 items-center">
                            <KeyIcon />
                            <SelectValue placeholder="Select mode" />
                        </div>
                    </SelectTrigger>
                    <SelectContent>
                        <SelectGroup>
                            <SelectItem value="host">Host and Port</SelectItem>
                            <SelectItem value="socket">Socket </SelectItem>
                        </SelectGroup>
                    </SelectContent>
                </Select>
            </div>
            {currentConnection.mode === "host" && <ConnectionHostMode />}
            {currentConnection.mode === "socket" && <ConnectionSocketMode />}
        </div>
    );
};
