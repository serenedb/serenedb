import { Explorer } from "@serene-ui/shared-frontend/widgets";
import type { ExplorerNodeData } from "../../../shared/Explorer";
import { useEffect, useState } from "react";
import { useGetConnections } from "@serene-ui/shared-frontend/entities";
import { useConsoleSidebarPinned } from "../../ConsoleSidebar/model";

interface ConsoleExplorerProps {
    explorerRef?: React.RefObject<HTMLDivElement | null>;
}

export const ConsoleExplorer = ({ explorerRef }: ConsoleExplorerProps) => {
    const [searchTerm] = useState<string>();
    const [initialData, setInitialData] = useState<ExplorerNodeData[]>();
    const { isNodePinned, togglePinnedNode } = useConsoleSidebarPinned();
    const {
        data: connections,
        isFetched: isDataFetched,
        isLoading: isDataLoading,
    } = useGetConnections({
        refetchInterval: 30000,
    });

    useEffect(() => {
        if (connections?.length) {
            setInitialData(
                connections.map((connection) => ({
                    id: "c-" + connection.id,
                    name: connection.name,
                    type: "connection",
                    parentId: null,
                    context: { connectionId: connection.id },
                })),
            );
        }
    }, [connections]);
    return (
        <>
            <Explorer
                ref={explorerRef}
                searchTerm={searchTerm}
                initialData={initialData || []}
                isDataFetched={isDataFetched && !isDataLoading}
                sidebarSectionId="entities"
                enablePinning
                isNodePinned={isNodePinned}
                onTogglePinned={togglePinnedNode}
            />
        </>
    );
};
