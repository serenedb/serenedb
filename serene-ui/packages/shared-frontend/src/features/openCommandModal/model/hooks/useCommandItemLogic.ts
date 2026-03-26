import { useMemo } from "react";
import { CommandSection, useCommandModal } from "../../model";

export interface UseCommandItemLogicProps {
    isSection: boolean;
    title?: string;
    section: CommandSection;
    sectionLabel: string;
    onSelect?: () => void;
}

export interface CommandItemDisplayText {
    primary: string;
    secondary?: string;
    showSeparator: boolean;
}

export const useCommandItemLogic = ({
    isSection,
    title,
    section,
    sectionLabel,
    onSelect,
}: UseCommandItemLogicProps) => {
    const { setOpen, setCurrentSection, currentSection, inputValue } =
        useCommandModal();

    const handleSelect = () => {
        if (isSection) {
            setCurrentSection(section);
            return;
        }

        if (onSelect) {
            onSelect();
        }
        setOpen(false);
    };

    const displayText = useMemo((): CommandItemDisplayText => {
        // Home view
        if (isSection) {
            return {
                primary: sectionLabel,
                showSeparator: false,
            };
        }

        // Section selecte
        if (currentSection === section) {
            return {
                primary: title || "",
                showSeparator: false,
            };
        }

        // Search something on home view
        if (currentSection === CommandSection.Home && inputValue) {
            return {
                primary: title || "",
                secondary: sectionLabel,
                showSeparator: true,
            };
        }

        // Single commands
        return {
            primary: title || "",
            showSeparator: false,
        };
    }, [isSection, title, section, sectionLabel, currentSection, inputValue]);

    return {
        handleSelect,
        displayText,
    };
};
