import { useConnection } from "@serene-ui/shared-frontend/entities";
import { ConnectionIcon } from "@serene-ui/shared-frontend/shared";
import { CommandSection } from "../../model";
import { CommandItemBase } from "./CommandItemBase";

export interface ChangeConnectionCommandProps {
    isSection?: boolean;
    connectionId?: number;
    title?: string;
}

export const ChangeConnectionCommand = ({
    isSection = false,
    connectionId,
    title,
}: ChangeConnectionCommandProps) => {
    const { setCurrentConnection } = useConnection();

    const handleChangeConnectionId = (newConnectionId: number) => {
        setCurrentConnection((prev) => ({
            ...prev,
            connectionId: newConnectionId,
            database: "",
        }));
    };

    return (
        <CommandItemBase
            isSection={isSection}
            title={title}
            icon={<ConnectionIcon />}
            section={CommandSection.Connections}
            sectionLabel="Change connection"
            onSelect={
                connectionId !== undefined
                    ? () => handleChangeConnectionId(connectionId)
                    : undefined
            }
        />
    );
};
