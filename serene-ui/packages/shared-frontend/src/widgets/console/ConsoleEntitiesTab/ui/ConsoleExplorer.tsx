import { Explorer } from "@serene-ui/shared-frontend/widgets";
import type { ExplorerNodeData } from "../../../shared/Explorer";
import { useEffect, useState } from "react";
import { useGetConnections } from "@serene-ui/shared-frontend/entities";
import { Input } from "@serene-ui/shared-frontend/shared";

interface ConsoleExplorerProps {
    explorerRef?: React.RefObject<HTMLDivElement | null>;
}

export const ConsoleExplorer = ({ explorerRef }: ConsoleExplorerProps) => {
    const [searchTerm, setSearchTerm] = useState<string>();
    const [initialData, setInitialData] = useState<ExplorerNodeData[]>();
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
            <div className="mt-1 px-2">
                <Input
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.currentTarget.value)}
                    placeholder="Search"
                    variant="secondary"
                />
            </div>

            <div className="mt-3 px-3 flex items-center justify-between">
                <p className="text-sm">Explorer</p>
                <div className="flex gap-1"></div>
            </div>

            <div className="pt-1 flex-1">
                <Explorer
                    ref={explorerRef}
                    searchTerm={searchTerm}
                    initialData={initialData || []}
                    isDataFetched={isDataFetched && !isDataLoading}
                />
            </div>
        </>
    );
};
