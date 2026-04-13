import {
    ArrowDownIcon,
    Button,
    cn,
    LoaderIcon,
    PinIcon,
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
    rightNode?: React.ReactNode;
    titleBadge?: React.ReactNode;
    afterPinNode?: React.ReactNode;
    isPinned?: boolean;
    onTogglePin?: () => void;
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
    rightNode,
    titleBadge,
    afterPinNode,
    isPinned = false,
    onTogglePin,
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
                    "group/explorer-node pl-4 flex w-full h-full items-center justify-start border-none text-foreground dark:hover:bg-accent pr-1",
                )}
                onClick={onClick}>
                {showArrow && onClick && (
                    <ArrowDownIcon className={!open ? "-rotate-90" : ""} />
                )}
                <div className="ml-2">{icon && icon}</div>
                <p className="text-xs ml-1.5 text-foreground/80 dark:text-foreground">
                    {title}
                </p>
                {titleBadge ? (
                    <div className="ml-1 shrink-0">{titleBadge}</div>
                ) : null}
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
                {rightNode && <div className="ml-auto pr-1">{rightNode}</div>}
                {!rightNode && rightText && (
                    <span className="ml-auto text-xs text-secondary-foreground/50">
                        {rightText}
                    </span>
                )}
                {onTogglePin && (
                    <Button
                        variant="ghost"
                        size="xsIcon"
                        className={cn(
                            "text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0",
                            "opacity-0 pointer-events-none",
                            "group-hover/explorer-node:opacity-100 group-hover/explorer-node:pointer-events-auto",
                            "group-focus-within/explorer-node:opacity-100 group-focus-within/explorer-node:pointer-events-auto",
                            !rightText && !rightNode && "ml-auto",
                            isPinned && "opacity-100 pointer-events-auto",
                        )}
                        title={isPinned ? "Unpin" : "Pin"}
                        onClick={(event) => {
                            event.preventDefault();
                            event.stopPropagation();
                            onTogglePin();
                        }}>
                        <PinIcon
                            className={
                                isPinned
                                    ? "size-3 text-foreground fill-current"
                                    : "size-3"
                            }
                        />
                    </Button>
                )}
                {afterPinNode}
            </div>
        </div>
    );
};
