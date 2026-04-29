import { Button, CrossIcon } from "@serene-ui/shared-frontend";

interface ConsoleExecutionHistoryTopbarProps {
    onClose: () => void;
}

export const ConsoleExecutionHistoryTopbar = ({
    onClose,
}: ConsoleExecutionHistoryTopbarProps) => {
    return (
        <div className="electron-drag-region flex min-h-[48.5px] items-center justify-between border-b-[0.5px] py-2.5 pl-4 pr-2.5">
            <p className="uppercase dark:text-secondary-foreground font-black text-xs">
                Execution History
            </p>
            <div className="electron-no-drag">
                <Button
                    size="icon"
                    variant="ghost"
                    title="Close"
                    onClick={onClose}>
                    <CrossIcon className="size-3" />
                </Button>
            </div>
        </div>
    );
};
