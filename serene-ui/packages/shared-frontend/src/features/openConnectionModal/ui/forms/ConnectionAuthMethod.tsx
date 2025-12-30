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
import { ConnectionAuthPassword } from "./ConnectionAuthPassword";

export const ConnectionAuthMethod = () => {
    const { currentConnection, handleSelectChange } = useConnectionsModal();

    return (
        <div className="flex-1 flex flex-col gap-4">
            <div className="flex flex-col gap-2">
                <Label htmlFor="">Authentication method</Label>
                <Select
                    value={currentConnection.authMethod}
                    onValueChange={(value) => {
                        handleSelectChange("authMethod", value);
                    }}>
                    <SelectTrigger className="w-full">
                        <div className="flex gap-2.5 items-center">
                            <KeyIcon />
                            <SelectValue placeholder="Select method" />
                        </div>
                    </SelectTrigger>
                    <SelectContent>
                        <SelectGroup defaultValue={"password"}>
                            <SelectItem value="password">
                                Username / Password
                            </SelectItem>
                        </SelectGroup>
                    </SelectContent>
                </Select>
            </div>
            {currentConnection.authMethod === "password" && (
                <ConnectionAuthPassword />
            )}
        </div>
    );
};
