import {
    Button,
    cn,
    Tooltip,
    TooltipContent,
    TooltipTrigger,
} from "@serene-ui/shared-frontend/shared";

interface ConnectionTypeButtonProps {
    type: "serenedb" | "postgres";
    isActive: boolean;
    onSelect: (type: "serenedb" | "postgres") => void;
    tooltipContent: React.ReactNode;
    icon: React.ReactNode;
}

export const ConnectionTypeButton = ({
    type,
    isActive,
    onSelect,
    tooltipContent,
    icon,
}: ConnectionTypeButtonProps) => {
    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <Button
                    className={cn(
                        "opacity-50 bg-primary/30 hover:bg-primary/30",
                        isActive && "opacity-100 border border-primary/50",
                    )}
                    size="icon"
                    aria-label={`Select ${type} connection type`}
                    onClick={() => onSelect(type)}>
                    {icon}
                </Button>
            </TooltipTrigger>
            <TooltipContent
                className="flex flex-col items-center"
                side="top"
                align="center">
                {tooltipContent}
            </TooltipContent>
        </Tooltip>
    );
};
