import { useCallback, useEffect, useState, useMemo } from "react";
import { SavedQueriesModal } from "../ui";
import { useQuerySubscription } from "../../executeQuery";
import {
    syncBindVars,
    useAddSavedQuery,
    useDeleteSavedQuery,
    useUpdateSavedQuery,
} from "@serene-ui/shared-frontend/entities";
import { SavedQueriesModalContext } from "./SavedQueriesModalContext";
import { toast } from "sonner";
import { BindVarSchema, SavedQuerySchema } from "@serene-ui/shared-core";

type SavedQueryState = Omit<SavedQuerySchema, "bind_vars"> & {
    bind_vars: BindVarSchema[];
};

export const SavedQueriesModalProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const { mutateAsync: addSavedQuery } = useAddSavedQuery();
    const { mutateAsync: deleteSavedQuery } = useDeleteSavedQuery();
    const { mutateAsync: updateSavedQuery } = useUpdateSavedQuery();

    const [open, setOpen] = useState(false);
    const [currentSavedQuery, setCurrentSavedQuery] = useState<
        SavedQuerySchema | undefined
    >();
    const [jobId, setJobId] = useState<number | undefined>();
    const [isQueryRunning, setIsQueryRunning] = useState(false);

    const result = useQuerySubscription(jobId);

    const memoizedBindVars = useMemo(
        () => currentSavedQuery?.bind_vars ?? [],
        [currentSavedQuery?.bind_vars],
    );

    useEffect(() => {
        if (result) {
            setIsQueryRunning(false);
        }
    }, [result]);

    useEffect(() => {
        if (!open) {
            setCurrentSavedQuery(undefined);
            setJobId(undefined);
        }
    }, [open]);

    const saveQuery = async (payload: SavedQueryState, isNew: boolean) => {
        const result = isNew
            ? await addSavedQuery(payload)
            : await updateSavedQuery(payload);

        if (!result) {
            throw new Error("No data returned from API");
        }

        return result;
    };

    const handleSaveQuery = useCallback(async () => {
        if (!currentSavedQuery) return;

        try {
            const payload: SavedQueryState = {
                ...currentSavedQuery,
                bind_vars: currentSavedQuery.bind_vars ?? [],
            };

            const isNew = currentSavedQuery.id === -1;
            const savedData = await saveQuery(payload, isNew);

            setCurrentSavedQuery((prev) =>
                prev
                    ? { ...prev, id: savedData.id, bind_vars: prev.bind_vars }
                    : prev,
            );

            toast.success("Query saved successfully");
        } catch (error) {
            console.error(error);
            toast.error("Failed to save query");
        }
    }, [currentSavedQuery]);

    const handleDeleteSavedQuery = useCallback(async () => {
        if (!currentSavedQuery) return;

        try {
            await deleteSavedQuery({ id: currentSavedQuery.id });
            setCurrentSavedQuery(undefined);
            toast.success("Query deleted");
        } catch (error) {
            console.error(error);
            toast.error("Failed to delete query");
        }
    }, [currentSavedQuery, deleteSavedQuery]);

    useEffect(() => {
        if (!currentSavedQuery) return;

        const newBindVars = syncBindVars(
            currentSavedQuery.query,
            memoizedBindVars,
        );

        const bindVarsChanged =
            newBindVars.length !== memoizedBindVars.length ||
            newBindVars.some(
                (v, i) =>
                    v.value !== memoizedBindVars[i]?.value ||
                    v.name !== memoizedBindVars[i]?.name,
            );

        if (bindVarsChanged) {
            setCurrentSavedQuery((prev) =>
                prev ? { ...prev, bind_vars: newBindVars } : prev,
            );
        }
    }, [currentSavedQuery?.query, memoizedBindVars]);

    return (
        <SavedQueriesModalContext.Provider
            value={{
                open,
                setOpen,
                currentSavedQuery,
                setCurrentSavedQuery,
                setJobId,
                result,
                handleSaveQuery,
                handleDeleteSavedQuery,
                isQueryRunning,
                setIsQueryRunning,
            }}>
            {children}
            <SavedQueriesModal />
        </SavedQueriesModalContext.Provider>
    );
};
