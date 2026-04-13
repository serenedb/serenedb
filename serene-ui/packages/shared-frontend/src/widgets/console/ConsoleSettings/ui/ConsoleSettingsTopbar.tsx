import { Button, CrossIcon } from "@serene-ui/shared-frontend";

interface ConsoleSettingsTopbarProps {
    onClose: () => void;
}

export const ConsoleSettingsTopbar = ({
    onClose,
}: ConsoleSettingsTopbarProps) => {
    return (
        <div className="min-h-[48.5px] pl-4 pr-2.5 py-2.5 justify-between items-center flex border-b-[0.5px]">
            <p className="uppercase text-foreground dark:text-secondary-foreground font-black text-xs">
                Settings
            </p>
            <Button size="icon" variant="ghost" title="Close" onClick={onClose}>
                <CrossIcon className="size-3" />
            </Button>
        </div>
    );
};
