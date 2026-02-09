import {
    CommandGroup,
    ConsoleIcon,
    FramesIcon,
    navigationMap,
    ReplicationIcon,
} from "@serene-ui/shared-frontend/shared";
import {
    ChangePageCommand,
    ChangePageCommandProps,
} from "../commands/ChangePageCommand";
import { CommandSection, useCommandModal } from "../../model";
import { shouldShowCommandGroup } from "../../model/utils";
import { FileIcon } from "lucide-react";

export const PageSelectorCommandGroup = () => {
    const { currentSection, inputValue } = useCommandModal();

    const PAGES: ChangePageCommandProps[] = [
        {
            title: "Console",
            route: navigationMap.console,
            icon: <ConsoleIcon />,
        },
        {
            title: "Frames",
            route: navigationMap.frames,
            icon: <FramesIcon />,
        },
        {
            title: "Replication",
            route: navigationMap.replication,
            icon: <ReplicationIcon />,
        },
    ];

    if (!shouldShowCommandGroup(currentSection, CommandSection.Pages)) {
        return null;
    }

    if (!inputValue && currentSection === CommandSection.Home) {
        return (
            <CommandGroup className="p-0">
                <ChangePageCommand isSection={true} icon={<FileIcon />} />
            </CommandGroup>
        );
    }

    return (
        <CommandGroup
            className="p-0"
            heading={
                currentSection === CommandSection.Home ? "Page" : undefined
            }>
            {PAGES.map((page) => (
                <ChangePageCommand
                    key={page.title}
                    title={page.title}
                    route={page.route}
                    icon={page.icon}
                />
            ))}
        </CommandGroup>
    );
};
