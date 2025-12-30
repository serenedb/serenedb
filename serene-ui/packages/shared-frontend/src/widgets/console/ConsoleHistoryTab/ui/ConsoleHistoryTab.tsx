import React, { useCallback, useEffect, useState } from "react";
import {
    useClearQueryHistory,
    useGetQueryHistory,
} from "@serene-ui/shared-frontend/entities";
import { Button, CrossIcon, Input } from "@serene-ui/shared-frontend/shared";
import {
    type ExplorerNodeData,
    Explorer,
} from "@serene-ui/shared-frontend/widgets";
import { type ConsoleTab } from "../../ConsoleEditorTabsSelector";
import { type QueryHistoryItemSchema } from "@serene-ui/shared-core";
interface ConsoleHistoryTabProps {
    updateTab: (id: number, tabUpdate: Partial<ConsoleTab>) => void;
    selectedTabId: number;
    explorerRef?: React.RefObject<HTMLDivElement | null>;
}

export const ConsoleHistoryTab: React.FC<ConsoleHistoryTabProps> = ({
    updateTab,
    selectedTabId,
    explorerRef,
}) => {
    const [searchTerm, setSearchTerm] = useState<string>();
    const [initialData, setInitialData] = useState<ExplorerNodeData[]>();
    const {
        data: queryHistory,
        isFetched: isDataFetched,
        isLoading: isDataLoading,
    } = useGetQueryHistory({
        refetchInterval: 30000,
    });
    const { mutateAsync: clearQueryHistory } = useClearQueryHistory();

    const handleSetQueryToTab = useCallback(
        (query: string) => {
            updateTab(selectedTabId, { value: query });
        },
        [selectedTabId, updateTab],
    );

    const handleClearQueryHistory = async () => {
        await clearQueryHistory();
    };

    useEffect(() => {
        if (queryHistory) {
            setInitialData(
                queryHistory.map(
                    (
                        queryHistory: QueryHistoryItemSchema,
                        queryHistoryIndex: number,
                    ) => ({
                        id: "qh-" + queryHistoryIndex,
                        name: queryHistory.name,
                        type: "query-history",
                        parentId: null,
                        context: {
                            query: queryHistory.query,
                            action: () =>
                                handleSetQueryToTab(queryHistory.query),
                        },
                    }),
                ),
            );
        }
    }, [queryHistory, handleSetQueryToTab]);

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
            {initialData?.length ? (
                <>
                    <div className="mt-3 px-3 flex items-center justify-between">
                        <p className="text-sm">Query history</p>
                        <div className="flex gap-1">
                            <Button
                                onClick={handleClearQueryHistory}
                                size={"iconSmall"}
                                variant={"ghost"}>
                                <CrossIcon className="size-2.5" />
                            </Button>
                        </div>
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
            ) : (
                <div className="p-2 pt-1 flex-1 flex items-center justify-center">
                    <div className="flex items-center justify-center bg-secondary p-2 rounded">
                        <p className="text-sm text-center">
                            Query history is empty
                        </p>
                    </div>
                </div>
            )}
        </>
    );
};
