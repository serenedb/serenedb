import { useDatabases } from "@serene-ui/shared-frontend/entities";
import { CommandGroup } from "@serene-ui/shared-frontend/shared";
import { CommandSection, useCommandModal } from "../../model";
import { shouldShowCommandGroup } from "../../model/utils";
import { ChangeDatabaseCommand } from "../commands";
import { CommandLoadingItem, CommandErrorItem } from "../commands";

export const DatabaseSelectorCommandGroup = () => {
    const { currentSection, inputValue } = useCommandModal();
    const { databases, isLoading, error } = useDatabases();

    if (!shouldShowCommandGroup(currentSection, CommandSection.Databases)) {
        return null;
    }

    if (!inputValue && currentSection !== CommandSection.Databases) {
        return (
            <CommandGroup className="p-0">
                <ChangeDatabaseCommand isSection={true} />
            </CommandGroup>
        );
    }

    return (
        <CommandGroup
            className="p-0"
            heading={
                currentSection === CommandSection.Home ? "Databases" : undefined
            }>
            {isLoading && <CommandLoadingItem />}
            {error && (
                <CommandErrorItem
                    error={error}
                    message="Failed to load databases"
                />
            )}
            {!isLoading &&
                !error &&
                databases.map((database) => (
                    <ChangeDatabaseCommand
                        key={database}
                        title={database}
                        databaseName={database}
                    />
                ))}
        </CommandGroup>
    );
};
