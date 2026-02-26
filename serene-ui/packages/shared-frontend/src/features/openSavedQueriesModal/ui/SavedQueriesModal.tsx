import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogTitle,
} from "@serene-ui/shared-frontend/shared";
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
                            results={[
                                {
                                    rows:
                                        result?.status == "success"
                                            ? result?.result
                                            : [],
                                    status: isQueryRunning
                                        ? "running"
                                        : result?.status || "",
                                },
                            ]}
                            selectedResultIndex={
                                result?.status === "success" ||
                                result?.status === "running"
                                    ? 0
                                    : -1
                            }
                        />
                    </div>
                </div>
            </DialogContent>
        </Dialog>
    );
};
