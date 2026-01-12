import { useConsoleLayout } from "@serene-ui/shared-frontend/features";
import {
    Button,
    MaximizeIcon,
    MinimizeIcon,
} from "@serene-ui/shared-frontend/shared";

interface ConsoleEditorMaximizeButtonProps {}

export const ConsoleEditorMaximizeButton: React.FC<
    ConsoleEditorMaximizeButtonProps
> = () => {
    const { isMaximized, toggleMaximized } = useConsoleLayout();
    return (
        <Button
            variant="secondary"
            size="icon"
            title={isMaximized ? "Exit maximized layout" : "Maximize console"}
            onClick={() => {
                toggleMaximized();
            }}>
            {isMaximized ? (
                <MinimizeIcon className="size-4 cursor-pointer" />
            ) : (
                <MaximizeIcon className="size-4 cursor-pointer" />
            )}
        </Button>
    );
};
