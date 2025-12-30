import {
    CommandDialog,
    CommandEmpty,
    CommandInput,
    CommandList,
    DialogContent,
    DialogTitle,
} from "@serene-ui/shared-frontend/shared";
import { useMemo } from "react";
import { CommandSection, useCommandModal } from "../model";
import {
    ConnectionSelectorCommandGroup,
    DatabaseSelectorCommandGroup,
    PageSelectorCommandGroup,
} from "./command-groups";
import { getSearchPlaceholder } from "../model/utils";

export const CommandModal: React.FC = () => {
    const {
        open,
        setOpen,
        inputValue,
        setInputValue,
        handleEscape,
        currentSection,
        setCurrentSection,
    } = useCommandModal();

    const handleInputChange = (value: string) => {
        setInputValue(value);
    };

    const goHome = () => {
        setCurrentSection(CommandSection.Home);
    };

    const searchPlaceholder = useMemo(
        () => getSearchPlaceholder(currentSection),
        [currentSection],
    );

    return (
        <CommandDialog open={open} onOpenChange={setOpen}>
            <DialogTitle />
            <DialogContent
                showCloseButton={false}
                onEscapeKeyDown={handleEscape}
                className="p-0 gap-0">
                <CommandInput
                    isSection={currentSection !== CommandSection.Home}
                    onBackButtonClick={goHome}
                    wrapperClassName="h-10"
                    value={inputValue}
                    onValueChange={handleInputChange}
                    placeholder={searchPlaceholder}
                />
                <CommandList className="p-1.5">
                    <CommandEmpty>No results found.</CommandEmpty>
                    <PageSelectorCommandGroup />
                    <ConnectionSelectorCommandGroup />
                    <DatabaseSelectorCommandGroup />
                </CommandList>
            </DialogContent>
        </CommandDialog>
    );
};
