import {
    Button,
    HorizontalIcon,
    VerticalIcon,
} from "@serene-ui/shared-frontend/shared";
import { useConsoleLayout } from "../model";

export const ChangeConsoleLayoutIcon = () => {
    const { layout, toggleLayout } = useConsoleLayout();
    return (
        <Button
            variant="secondary"
            size="icon"
            title={
                layout === "horizontal"
                    ? "Switch to vertical layout"
                    : "Switch to horizontal layout"
            }
            onClick={toggleLayout}>
            {layout === "vertical" ? <VerticalIcon /> : <HorizontalIcon />}
        </Button>
    );
};
