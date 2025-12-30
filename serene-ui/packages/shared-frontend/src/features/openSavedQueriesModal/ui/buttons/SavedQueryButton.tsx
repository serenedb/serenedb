import { Button, cn, TreeQueryIcon } from "@serene-ui/shared-frontend/shared";

interface SavedQueryButtonProps {
    isSelected?: boolean;
    isEditing?: boolean;
    onClick: () => void;
    name?: string;
}

export const SavedQueryButton: React.FC<SavedQueryButtonProps> = ({
    isSelected,
    isEditing,
    onClick,
    name,
}) => {
    return (
        <Button
            className={cn(
                "flex justify-start items-start h-auto w-full rounded-none",
                {
                    "bg-secondary text-accent-foreground/50": isSelected,
                },
            )}
            variant="ghost"
            onClick={onClick}>
            <TreeQueryIcon className="mt-0.5" />
            <div className="flex flex-col">
                <p className="text-md font-medium w-max">
                    {name}{" "}
                    {isEditing && (
                        <span className="text-xs text-red-900">(unsaved)</span>
                    )}
                </p>
                <p className="text-xs text-secondary-foreground/50 w-max">
                    TODO
                </p>
            </div>
        </Button>
    );
};
