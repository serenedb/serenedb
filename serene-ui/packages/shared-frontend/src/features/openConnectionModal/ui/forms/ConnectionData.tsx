import {
    Button,
    cn,
    CrossIcon,
    Input,
    Label,
} from "@serene-ui/shared-frontend/shared";
import { TestConnectionButton } from "@serene-ui/shared-frontend/features";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";
import { AddConnectionButton } from "../buttons/ConnectionActionButton";
import { ConnectionAuthMethod } from "./ConnectionAuthMethod";
import { ConnectionConnectMode } from "./ConnectionConnectMode";
import { ConnectionTypeSelector } from "./ConnectionTypeSelector";

export const ConnectionData = () => {
    const { currentConnection, handleInputChange, setOpen, isGlobalDisabled } =
        useConnectionsModal();

    return (
        <div
            className={cn(
                "flex flex-col flex-1",
                isGlobalDisabled ? "opacity-50 pointer-events-none" : "",
            )}>
            <div className="pl-4 pr-2 py-2 border-b flex items-center justify-between">
                <p className="text-sm font-medium">Connection Data</p>
                <Button
                    variant="secondary"
                    size="iconSmall"
                    title="Close connection modal"
                    onClick={() => setOpen(false)}>
                    <CrossIcon className="size-3" />
                </Button>
            </div>
            <ConnectionTypeSelector />
            <div className="flex flex-col flex-1 gap-4 p-4">
                <div className="flex flex-col gap-2">
                    <Label htmlFor="name">Connection Name</Label>
                    <Input
                        value={currentConnection.name}
                        onChange={handleInputChange}
                        id="name"
                        autoComplete={"off"}
                        placeholder="localhost"
                    />
                </div>
                <div className="flex flex-col gap-2">
                    <Label htmlFor="database">Default database</Label>
                    <Input
                        value={currentConnection.database}
                        onChange={handleInputChange}
                        id="database"
                        autoComplete={"off"}
                        placeholder="postgres"
                    />
                </div>
                <div className="flex gap-2">
                    <ConnectionAuthMethod />
                    <ConnectionConnectMode />
                </div>
                <div className="flex gap-2 mt-auto ml-auto">
                    <TestConnectionButton connection={currentConnection} />
                    <AddConnectionButton />
                </div>
            </div>
        </div>
    );
};
