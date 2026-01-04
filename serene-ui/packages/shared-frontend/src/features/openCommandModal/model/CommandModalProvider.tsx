import {
    DEFAULT_HOTKEYS,
    useAppHotkey,
} from "@serene-ui/shared-frontend/shared";
import { useEffect, useState } from "react";
import { CommandModal } from "../ui";
import { CommandModalContext } from "./CommandModalContext";
import { CommandSection } from "./types";

export const CommandModalProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [inputValue, setInputValue] = useState("");

    const [currentSection, setCurrentSection] = useState<CommandSection>(
        CommandSection.Home,
    );
    const [open, setOpen] = useState(false);

    /*
    TODO: implement recent commands functionality
    const [recent, setRecent] = useState<string[]>(
        JSON.parse(localStorage.getItem("system:recent-commands") || "[]"),
    );

    useEffect(() => {
        localStorage.setItem("system:recent-commands", JSON.stringify(recent));
    }, [recent]);
    */

    useAppHotkey(DEFAULT_HOTKEYS.COMMAND_TOGGLE, () => {
        setOpen(true);
    });

    const handleEscape = (e: KeyboardEvent) => {
        e.preventDefault();
        if (currentSection !== CommandSection.Home) {
            setCurrentSection(CommandSection.Home);
        } else {
            setOpen(false);
        }
    };

    useEffect(() => {
        if (!open) {
            setInputValue("");
            setCurrentSection(CommandSection.Home);
        }
    }, [open]);

    return (
        <CommandModalContext.Provider
            value={{
                open,
                setOpen,
                inputValue,
                setInputValue,
                currentSection,
                setCurrentSection,
                handleEscape,
            }}>
            {children}
            <CommandModal />
        </CommandModalContext.Provider>
    );
};
