import {
    ResizableHandle,
    ResizablePanel,
} from "@serene-ui/shared-frontend/shared";
import { ConsoleMenu } from "@serene-ui/shared-frontend/widgets";
import { useConsole } from "../model";

export const ConsoleLeftPanel = () => {
    const { selectedTabId, updateTab, explorerRef } = useConsole();
    return (
        <>
            <ResizablePanel
                className="flex flex-col"
                minSize={20}
                defaultSize={30}
                maxSize={50}>
                <ConsoleMenu
                    updateTab={updateTab}
                    selectedTabId={selectedTabId}
                    explorerRef={explorerRef}
                />
            </ResizablePanel>
            <ResizableHandle className="bg-border" tabIndex={-1} />
        </>
    );
};
