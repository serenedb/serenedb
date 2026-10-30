import { createContext, useContext } from "react";
import { CommandSection } from "./types";

interface CommandModalContextType {
    open: boolean;
    setOpen: React.Dispatch<React.SetStateAction<boolean>>;
    inputValue: string;
    setInputValue: React.Dispatch<React.SetStateAction<string>>;
    currentSection: CommandSection;
    setCurrentSection: React.Dispatch<React.SetStateAction<CommandSection>>;
    handleEscape: (e: KeyboardEvent) => void;
}

export const CommandModalContext = createContext<
    CommandModalContextType | undefined
>(undefined);

export const useCommandModal = () => {
    const context = useContext(CommandModalContext);
    if (!context) {
        throw new Error(
            "useCommandModal must be used within a CommandModalProvider",
        );
    }
    return context;
};
