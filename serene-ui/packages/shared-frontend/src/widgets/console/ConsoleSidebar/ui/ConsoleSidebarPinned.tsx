import React from "react";
import { Explorer } from "../../../shared/Explorer";
import { useConsoleSidebarPinned } from "../model";

export const ConsoleSidebarPinned: React.FC = () => {
    const { pinnedExplorerNodes, isNodePinned, togglePinnedNode } =
        useConsoleSidebarPinned();

    if (!pinnedExplorerNodes.length) {
        return (
            <div className="flex h-full items-center justify-center p-2">
                <p className="text-center text-xs text-foreground/70">
                    No pinned items yet
                </p>
            </div>
        );
    }

    return (
        <Explorer
            initialData={pinnedExplorerNodes}
            isDataFetched={true}
            enablePinning
            isNodePinned={isNodePinned}
            onTogglePinned={togglePinnedNode}
        />
    );
};
