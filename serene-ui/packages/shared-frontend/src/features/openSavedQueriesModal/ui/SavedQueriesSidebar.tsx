import { useGetSavedQueries } from "@serene-ui/shared-frontend/entities";
import { Button, PlusIcon } from "@serene-ui/shared-frontend/shared";
import { useSavedQueriesModal } from "../model/SavedQueriesModalContext";
import { SavedQueryButton } from "./buttons";

export const SavedQueriesSidebar = () => {
    const { setCurrentSavedQuery, currentSavedQuery } = useSavedQueriesModal();
    const { data: savedQueries } = useGetSavedQueries();

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
        <div className="w-65 border-r border-border flex flex-col">
            <div className="pl-4 pr-2 py-2 border-b border-border flex items-center justify-between">
                <p className="text-sm font-medium">Saved queries</p>
                <Button
                    onClick={handleAddQuery}
                    variant="secondary"
                    size="iconSmall">
                    <PlusIcon className="size-3.5" />
                </Button>
            </div>
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
                    {savedQueries?.map((savedQuery) => (
                        <SavedQueryButton
                            key={savedQuery.id}
                            isSelected={savedQuery.id === currentSavedQuery?.id}
                            name={savedQuery.name}
                            onClick={() => {
                                setCurrentSavedQuery(savedQuery);
                            }}
                        />
                    ))}
                    {currentSavedQuery?.id === -1 && (
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
                    )}
                </div>
            )}
        </div>
    );
};
