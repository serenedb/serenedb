import React from "react";
import { useConnection } from "@serene-ui/shared-frontend/entities";
import {
    OpenConnectionsModalIcon,
    RefreshDatabasesButton,
} from "@serene-ui/shared-frontend/features";
import { ConnectionsCombobox } from "../../../shared/ConnectionsCombobox";
import { DatabasesCombobox } from "../../../shared/DatabasesCombobox";

export const ConsoleConnectionSelect: React.FC = () => {
    const { currentConnection, setCurrentConnection } = useConnection();

    const handleChangeConnectionId = (newConnectionId: number) => {
        setCurrentConnection({
            connectionId: newConnectionId,
            database: "",
        });
    };

    const handleChangeDatabase = (newDatabase: string) => {
        setCurrentConnection({
            ...currentConnection,
            database: newDatabase,
        });
    };

    return (
        <div className="px-2 flex flex-col gap-[5px]">
            <div className="flex gap-[5px]">
                <ConnectionsCombobox
                    currentConnectionId={currentConnection.connectionId}
                    setCurrentConnectionId={handleChangeConnectionId}
                />
                <OpenConnectionsModalIcon />
            </div>
            <div className="flex gap-[5px]">
                <DatabasesCombobox
                    currentDatabase={currentConnection.database || ""}
                    setCurrentDatabase={handleChangeDatabase}
                />
                {currentConnection.connectionId !== -1 && (
                    <RefreshDatabasesButton />
                )}
            </div>
        </div>
    );
};
