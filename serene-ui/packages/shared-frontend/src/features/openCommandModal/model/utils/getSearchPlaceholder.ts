import { CommandSection } from "../types";

export const getSearchPlaceholder = (section: CommandSection) => {
    switch (section) {
        case CommandSection.Pages:
            return "Search pages...";
        case CommandSection.Connections:
            return "Search connections...";
        case CommandSection.Databases:
            return "Search databases...";
        default:
            return "Type a command or search...";
    }
};
