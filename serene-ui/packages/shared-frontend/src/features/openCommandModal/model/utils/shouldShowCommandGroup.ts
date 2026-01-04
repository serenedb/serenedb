import { CommandSection } from "../types";

/**
 * Determines if a command group should be visible based on the current section
 * @param currentSection - The current active section
 * @param groupSection - The section this command group belongs to
 * @returns true if the group should be visible, false otherwise
 */
export const shouldShowCommandGroup = (
    currentSection: CommandSection,
    groupSection: CommandSection,
): boolean => {
    // Always show on home section
    if (currentSection === CommandSection.Home) {
        return true;
    }

    // Show only the matching section when not on home
    return currentSection === groupSection;
};
