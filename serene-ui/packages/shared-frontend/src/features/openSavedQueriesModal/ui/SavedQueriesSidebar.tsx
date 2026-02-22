import { useGetSavedQueries } from "@serene-ui/shared-frontend/entities";
import { Button, PlusIcon } from "@serene-ui/shared-frontend/shared";
import { useEffect, useRef } from "react";
import { useSavedQueriesModal } from "../model/SavedQueriesModalContext";
import {
    ExportSavedQueriesButton,
    ImportSavedQueriesButton,
    SavedQueryButton,
} from "./buttons";

export const SavedQueriesSidebar = () => {
    const { setCurrentSavedQuery, currentSavedQuery } = useSavedQueriesModal();
    const { data: savedQueries } = useGetSavedQueries();
    const listContainerRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!currentSavedQuery) return;

        const selectedElement =
            listContainerRef.current?.querySelector<HTMLElement>(
                "[data-selected='true']",
            );

        selectedElement?.scrollIntoView({
            block: "nearest",
        });
    }, [currentSavedQuery?.id, currentSavedQuery?.name, savedQueries?.length]);

    const handleAddQuery = () => {
        setCurrentSavedQuery({
            id: -1,
            name: "Untitled",
            query: "",
            bind_vars: [],
            usage_count: 0,
        });
    };

    return (
        <div className="w-70 border-r border-border flex flex-col max-h-120">
            <div className="pl-4 pr-2 py-2 border-b border-border flex items-center justify-between">
                <p className="text-sm font-medium">Saved queries</p>
                <div className="flex gap-2">
                    <ExportSavedQueriesButton />
                    <ImportSavedQueriesButton />
                    <Button
                        onClick={handleAddQuery}
                        variant="secondary"
                        size="iconSmall">
                        <PlusIcon className="size-3.5" />
                    </Button>
                </div>
            </div>
            <div className="flex flex-col overflow-scroll " ref={listContainerRef}>
                {!savedQueries?.length && !currentSavedQuery ? (
                    <div className="flex flex-1 items-center justify-center">
                        <div className="px-6 py-4 bg-secondary rounded-md w-max flex flex-col items-center">
                            <p>No saved queries yet!</p>
                            <p className="text-sm font-light opacity-30">
                                Want to add some?
                            </p>
                        </div>
                    </div>
                ) : (
                    <div className="flex flex-1 flex-col gap-0">
                        {currentSavedQuery?.id === -1 && (
                            <div data-selected={currentSavedQuery.id === -1}>
                                <SavedQueryButton
                                    key={currentSavedQuery.id}
                                    name={currentSavedQuery.name}
                                    isSelected={currentSavedQuery.id === -1}
                                    isEditing={true}
                                    onClick={() => {
                                        setCurrentSavedQuery({
                                            ...currentSavedQuery,
                                        });
                                    }}
                                />
                            </div>
                        )}

                        {savedQueries?.map((savedQuery) => (
                            <div
                                key={savedQuery.id}
                                data-selected={
                                    savedQuery.id === currentSavedQuery?.id
                                }>
                                <SavedQueryButton
                                    isSelected={
                                        savedQuery.id === currentSavedQuery?.id
                                    }
                                    name={savedQuery.name}
                                    onClick={() => {
                                        setCurrentSavedQuery(savedQuery);
                                    }}
                                />
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
};
