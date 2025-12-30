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
            return <p className="text-primary-foreground">{primary}</p>;
        }

        // Section view
        return (
            <p className="text-secondary-foreground">
                {secondary}
                {" > "}
                <span className="text-primary-foreground">{primary}</span>
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
