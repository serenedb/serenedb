import { useCallback, useEffect, useMemo, useState } from "react";
import { SavedQueriesModal } from "../ui";
import {
    syncBindVars,
    useAddSavedQuery,
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
    const { mutateAsync: updateSavedQuery } = useUpdateSavedQuery();

    const [open, setOpen] = useState(false);
    const [modalMode, setModalMode] = useState<"create" | "edit">("create");
    const [currentSavedQuery, setCurrentSavedQuery] = useState<
        SavedQuerySchema | undefined
    >();

    const memoizedBindVars = useMemo(
        () => currentSavedQuery?.bind_vars ?? [],
        [currentSavedQuery?.bind_vars],
    );

    useEffect(() => {
        if (!open) {
            setCurrentSavedQuery(undefined);
            setModalMode("create");
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
            const trimmedName = currentSavedQuery.name.trim();
            if (!trimmedName) {
                toast.error("Query name is required");
                return;
            }

            const payload: SavedQueryState = {
                ...currentSavedQuery,
                name: trimmedName,
                bind_vars: currentSavedQuery.bind_vars ?? [],
            };

            const isNew = modalMode === "create" || currentSavedQuery.id === -1;
            const savedData = await saveQuery(payload, isNew);

            setCurrentSavedQuery((prev) =>
                prev
                    ? {
                          ...prev,
                          id: savedData.id,
                          name: savedData.name,
                          query: savedData.query,
                          bind_vars: savedData.bind_vars ?? prev.bind_vars,
                      }
                    : prev,
            );

            toast.success("Query saved successfully");
            setOpen(false);
        } catch (error) {
            console.error(error);
            toast.error("Failed to save query");
        }
    }, [currentSavedQuery, modalMode]);

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

    const openCreateModal = useCallback(
        (payload?: { query?: string; bindVars?: BindVarSchema[] }) => {
            setModalMode("create");
            setCurrentSavedQuery({
                id: -1,
                name: "Untitled",
                query: payload?.query ?? "",
                bind_vars: payload?.bindVars ?? [],
                usage_count: 0,
            });
            setOpen(true);
        },
        [],
    );

    const openEditModal = useCallback((savedQuery: SavedQuerySchema) => {
        setModalMode("edit");
        setCurrentSavedQuery(savedQuery);
        setOpen(true);
    }, []);

    return (
        <SavedQueriesModalContext.Provider
            value={{
                open,
                setOpen,
                modalMode,
                currentSavedQuery,
                setCurrentSavedQuery,
                openCreateModal,
                openEditModal,
                handleSaveQuery,
            }}>
            {children}
            <SavedQueriesModal />
        </SavedQueriesModalContext.Provider>
    );
};
