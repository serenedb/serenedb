import { cn, CommandItem } from "@serene-ui/shared-frontend/shared";
import { CommandSection, useCommandItemLogic } from "../../model";

export interface CommandItemBaseProps {
    isSection?: boolean;
    title?: string;
    section: CommandSection;
    icon: React.ReactNode;
    sectionLabel: string;
    onSelect?: () => void;
    className?: string;
}

export const CommandItemBase = ({
    isSection = false,
    title,
    section,
    sectionLabel,
    icon,
    onSelect,
    className,
}: CommandItemBaseProps) => {
    const { handleSelect, displayText } = useCommandItemLogic({
        isSection,
        title,
        section,
        sectionLabel,
        onSelect,
    });

    const renderText = () => {
        const { primary, secondary, showSeparator } = displayText;

        // Home view
        if (!showSeparator) {
            return <p className="text-secondary-foreground">{primary}</p>;
        }

        // Section view
        return (
            <p className="text-secondary-foreground/50 dark:text-secondary-foreground/70">
                {secondary}
                {" > "}
                <span className="text-secondary-foreground dark:text-secondary-foreground">
                    {primary}
                </span>
            </p>
        );
    };

    return (
        <CommandItem
            className={cn("py-2 flex items-center", className)}
            onSelect={handleSelect}>
            {icon}
            {renderText()}
        </CommandItem>
    );
};
