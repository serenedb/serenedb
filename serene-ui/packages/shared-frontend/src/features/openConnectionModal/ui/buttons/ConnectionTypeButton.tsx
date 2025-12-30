import {
    Button,
    cn,
    Tooltip,
    TooltipContent,
    TooltipTrigger,
} from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";
import { ConnectionSchema } from "@serene-ui/shared-core";

interface ConnectionTypeButtonProps {
    type: ConnectionSchema["type"];
    tooltipContent: React.ReactNode;
    icon: React.ReactNode;
}

export const ConnectionTypeButton = ({
    type,
    tooltipContent,
    icon,
}: ConnectionTypeButtonProps) => {
    const { handleSelectChange, currentConnection } = useConnectionsModal();

    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <Button
                    className={cn(
                        "opacity-50 bg-primary/30 hover:bg-primary/30",
                        currentConnection.type === type &&
                            "opacity-100 border border-primary/50",
                    )}
                    size="icon"
                    onClick={() => handleSelectChange("type", type)}>
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
