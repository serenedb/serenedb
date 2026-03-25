import {
    ArrowDownIcon,
    Button,
    cn,
    LoaderIcon,
    Tooltip,
    TooltipContent,
    TooltipTrigger,
} from "@serene-ui/shared-frontend/shared";
import { AlertCircle } from "lucide-react";

interface ExplorerNodeButtonProps {
    title: string;
    icon: React.ReactNode;
    onClick?: () => void;
    open: boolean;
    className?: string;
    style?: React.CSSProperties;
    showArrow?: boolean;
    isLoading?: boolean;
    isError?: boolean;
    rightText?: string;
}

export const ExplorerNodeButton = ({
    title,
    icon,
    onClick,
    open,
    className,
    style,
    showArrow = true,
    isLoading = false,
    isError = false,
    rightText,
}: ExplorerNodeButtonProps) => {
    return (
        <div
            style={{
                height: "100%",
                ...style,
            }}>
            <div
                className={cn(
                    className,
                    "pl-4 flex w-full h-full items-center justify-start border-none text-foreground dark:hover:bg-accent",
                )}
                onClick={onClick}>
                {showArrow && onClick && (
                    <ArrowDownIcon className={!open ? "-rotate-90" : ""} />
                )}
                <div className="ml-2">{icon && icon}</div>
                <p className="text-xs ml-1.5">{title}</p>
                {isLoading && (
                    <LoaderIcon className="size-3.5 ml-1 animate-spin" />
                )}
                {isError && (
                    <Tooltip>
                        <TooltipTrigger>
                            <AlertCircle className="text-red-900" />
                        </TooltipTrigger>

                        <TooltipContent
                            arrowClassName="bg-red-900"
                            className="bg-red-900 fill-red-900">
                            <span className="text-xs">
                                Failed to establish connection
                            </span>
                        </TooltipContent>
                    </Tooltip>
                )}
                {rightText && (
                    <span className="ml-auto text-xs text-secondary-foreground/50">
                        {rightText}
                    </span>
                )}
            </div>
        </div>
    );
};
