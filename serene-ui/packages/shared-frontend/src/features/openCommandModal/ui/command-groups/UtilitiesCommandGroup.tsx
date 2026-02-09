import { CommandGroup } from "@serene-ui/shared-frontend/shared";
import { CommandSection, useCommandModal } from "../../model";
import { shouldShowCommandGroup } from "../../model/utils";
import { CreateIssueCommand, ToggleThemeCommand } from "../commands";

export const UtilitiesCommandGroup = () => {
    const { currentSection, inputValue } = useCommandModal();

    if (!shouldShowCommandGroup(currentSection, CommandSection.Utilities)) {
        return null;
    }

    return (
        <CommandGroup className="p-0" heading={undefined}>
            <ToggleThemeCommand />
            <CreateIssueCommand />
        </CommandGroup>
    );
};
