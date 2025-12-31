import { Button, PlusIcon } from "@serene-ui/shared-frontend/shared";
import { type ConsoleTab } from "../model/types";

interface ConsoleEditorAddTabButtonProps {
    addTab: (tabType: ConsoleTab["type"]) => void;
}

export const ConsoleEditorAddTabButton: React.FC<
    ConsoleEditorAddTabButtonProps
> = ({ addTab }) => {
    return (
        <Button
            variant="secondary"
            size="icon"
            title="Add query tab"
            onClick={() => addTab("query")}>
            <PlusIcon className="size-3 bg-transparent cursor-pointer" />
        </Button>
    );
};
