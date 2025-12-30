import { Explorer } from "@serene-ui/shared-frontend/widgets";
import type { ExplorerNodeData } from "../../../shared/Explorer";
import { useCallback, useEffect, useState } from "react";
import { useGetSavedQueries } from "@serene-ui/shared-frontend/entities";
import { Button, Input, PlusIcon } from "@serene-ui/shared-frontend/shared";
import { type ConsoleTab } from "../../ConsoleEditorTabsSelector";
import { useSavedQueriesModal } from "@serene-ui/shared-frontend/features";

interface ConsoleSavedTabProps {
    updateTab: (id: number, tabUpdate: Partial<ConsoleTab>) => void;
    selectedTabId: number;
    explorerRef?: React.RefObject<HTMLDivElement | null>;
}

export const ConsoleSavedTab = ({
    updateTab,
    selectedTabId,
    explorerRef,
}: ConsoleSavedTabProps) => {
    const { setOpen, setCurrentSavedQuery } = useSavedQueriesModal();
    const [searchTerm, setSearchTerm] = useState<string>();
    const [initialData, setInitialData] = useState<ExplorerNodeData[]>();
    const {
        data: savedQueries,
        isFetched: isDataFetched,
        isLoading: isDataLoading,
    } = useGetSavedQueries({
        refetchInterval: 30000,
    });

    const handleSetQueryToTab = useCallback(
        (query: string) => {
            updateTab(selectedTabId, { value: query });
        },
        [selectedTabId, updateTab],
    );

    useEffect(() => {
        if (savedQueries?.length) {
            setInitialData(
                savedQueries.map((savedQuery) => ({
                    id: "sq-" + savedQuery.id,
                    name: savedQuery.name,
                    type: "saved-query",
                    parentId: null,
                    context: {
                        query: savedQuery.query,
                        action: () => handleSetQueryToTab(savedQuery.query),
                    },
                })),
            );
        }
    }, [savedQueries, handleSetQueryToTab]);

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
                        <p className="text-sm">Saved Queries</p>
                        <div className="flex gap-1">
                            <Button
                                size={"iconSmall"}
                                variant={"ghost"}
                                onClick={() => {
                                    setCurrentSavedQuery({
                                        id: -1,
                                        name: "Untitled",
                                        query: "",
                                        bind_vars: [],
                                        usage_count: 0,
                                    });

                                    setOpen(true);
                                }}>
                                <PlusIcon className="size-3" />
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
                <div className="flex-col p-2 pt-1 flex-1 flex items-center justify-center">
                    <div className="flex flex-col items-center justify-center gap-1">
                        <div className="flex flex-col items-center justify-center bg-secondary py-2 px-3 rounded-md">
                            <p className="text-sm text-center">
                                Saved queries is empty
                            </p>
                        </div>
                        <Button
                            variant={"default"}
                            onClick={() => {
                                setCurrentSavedQuery({
                                    id: -1,
                                    name: "Untitled",
                                    query: "",
                                    bind_vars: [],
                                    usage_count: 0,
                                });

                                setOpen(true);
                            }}>
                            Add new
                        </Button>
                    </div>
                </div>
            )}
        </>
    );
};
