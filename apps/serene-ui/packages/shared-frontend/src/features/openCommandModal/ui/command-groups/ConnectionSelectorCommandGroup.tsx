import { useGetConnections } from "@serene-ui/shared-frontend/entities";
import { CommandGroup } from "@serene-ui/shared-frontend/shared";
import { ChangeConnectionCommand } from "../commands/ChangeConnectionCommand";
import { CommandSection, useCommandModal } from "../../model";
import { shouldShowCommandGroup } from "../../model/utils";
import { CommandLoadingItem, CommandErrorItem } from "../commands";

export const ConnectionSelectorCommandGroup = () => {
    const { currentSection, inputValue } = useCommandModal();
    const {
        data: connections,
        isLoading,
        isError,
        error,
    } = useGetConnections();

    if (!shouldShowCommandGroup(currentSection, CommandSection.Connections)) {
        return null;
    }

    if (!inputValue && currentSection !== CommandSection.Connections) {
        return (
            <CommandGroup className="p-0">
                <ChangeConnectionCommand isSection={true} />
            </CommandGroup>
        );
    }

    return (
        <CommandGroup
            className="p-0"
            heading={
                currentSection === CommandSection.Home
                    ? "Connections"
                    : undefined
            }>
            {isLoading && <CommandLoadingItem />}
            {isError && (
                <CommandErrorItem
                    error={error}
                    message="Failed to load connections"
                />
            )}
            {!isLoading &&
                !isError &&
                connections?.map((connection) => (
                    <ChangeConnectionCommand
                        key={connection.id}
                        title={connection.name}
                        connectionId={connection.id}
                    />
                ))}
        </CommandGroup>
    );
};
