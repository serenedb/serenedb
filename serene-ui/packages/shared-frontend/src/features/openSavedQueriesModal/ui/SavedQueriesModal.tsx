import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogTitle,
} from "@serene-ui/shared-frontend/shared";
import { useEffect, useState } from "react";
import {
    BindVariables,
    QueryResults,
} from "@serene-ui/shared-frontend/widgets";
import { useSavedQueriesModal } from "../model";
import { SavedQueriesSidebar } from "./SavedQueriesSidebar";
import { SavedQueryData } from "./SavedQueryData";
import { BindVarSchema } from "@serene-ui/shared-core";

export const SavedQueriesModal = () => {
    const {
        open,
        setOpen,
        result,
        currentSavedQuery,
        setCurrentSavedQuery,
        isQueryRunning,
    } = useSavedQueriesModal();

    const handleChangeBindVars = (bind_vars: BindVarSchema[]) => {
        setCurrentSavedQuery((prev) => ({
            ...prev!,
            bind_vars,
        }));
    };

    const hasBindVars =
        currentSavedQuery?.bind_vars && currentSavedQuery.bind_vars.length > 0;
    const [selectedResultIndex, setSelectedResultIndex] = useState(0);
    const queryResults =
        result?.status === "success"
            ? result.results.map((item) => ({
                  rows: item.rows,
                  status: "success" as const,
                  message: item.message,
              }))
            : result
              ? [
                    {
                        rows: [],
                        status: isQueryRunning ? "running" : result.status,
                        error: result.status === "failed" ? result.error : undefined,
                    },
                ]
              : [];

    useEffect(() => {
        if (queryResults.length === 0) {
            setSelectedResultIndex(0);
            return;
        }

        setSelectedResultIndex((currentIndex) =>
            Math.min(currentIndex, queryResults.length - 1),
        );
    }, [queryResults.length]);

    return (
        <Dialog open={open} onOpenChange={(value) => setOpen(value)}>
            <DialogContent
                showCloseButton={false}
                className="bg-transparent border-0 items-center justify-center">
                <DialogDescription />
                <DialogTitle />
                <div className="flex flex-col gap-2">
                    <div className="flex gap-2">
                        <div className="flex flex-1 min-h-120 min-w-250 bg-sidebar rounded-md border border-border">
                            <SavedQueriesSidebar />
                            <SavedQueryData />
                        </div>
                        {hasBindVars ? (
                            <BindVariables
                                bind_vars={currentSavedQuery?.bind_vars}
                                setBindVars={handleChangeBindVars}
                                className="h-120"
                            />
                        ) : null}
                    </div>

                    <div className="flex-1 flex min-h-60 bg-background rounded-md border border-border">
                        <QueryResults
                            results={queryResults}
                            selectedResultIndex={
                                queryResults.length > 0 ? selectedResultIndex : -1
                            }
                            onSelectResult={setSelectedResultIndex}
                        />
                    </div>
                </div>
            </DialogContent>
        </Dialog>
    );
};
