import { useConnection } from "@serene-ui/shared-frontend/entities";
import { DatabaseIcon } from "@serene-ui/shared-frontend/shared";
import { CommandSection } from "../../model";
import { CommandItemBase } from "./CommandItemBase";

export interface ChangeDatabaseCommandProps {
    isSection?: boolean;
    databaseName?: string;
    title?: string;
}

export const ChangeDatabaseCommand = ({
    isSection = false,
    databaseName,
    title,
}: ChangeDatabaseCommandProps) => {
    const { setCurrentConnection } = useConnection();

    const handleChangeDatabase = () => {
        setCurrentConnection((prev) => ({
            ...prev,
            database: databaseName,
        }));
    };

    return (
        <CommandItemBase
            isSection={isSection}
            title={title}
            icon={<DatabaseIcon />}
            section={CommandSection.Databases}
            sectionLabel="Change database"
            onSelect={
                databaseName !== undefined ? handleChangeDatabase : undefined
            }
        />
    );
};
