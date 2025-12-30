import { createContext, useContext, useEffect, useState } from "react";
import {
    useAddQueryHistory,
    useDeleteQueryHistory,
    useGetQueryHistory,
} from "../api";
import { QueryHistoryItemSchema } from "@serene-ui/shared-core";

interface QueryHistoryContextType {
    queryHistory: QueryHistoryItemSchema[] | [];
    setQueryHistory: React.Dispatch<
        React.SetStateAction<QueryHistoryItemSchema[]>
    >;
    addQueryHistory: (
        queryHistory: Omit<QueryHistoryItemSchema, "id">,
    ) => Promise<void>;
    deleteQueryHistory: (
        queryHistoryId: QueryHistoryItemSchema["id"],
    ) => Promise<void>;
}

const QueryHistoryContext = createContext<QueryHistoryContextType | undefined>(
    undefined,
);

export const QueryHistoryProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const { data: queryHistoryLoaded } = useGetQueryHistory();
    const { mutateAsync: addQueryHistoryMutation } = useAddQueryHistory();
    const { mutateAsync: deleteQueryHistoryMutation } = useDeleteQueryHistory();
    const [queryHistory, setQueryHistory] = useState<QueryHistoryItemSchema[]>(
        [],
    );

    useEffect(() => {
        if (queryHistoryLoaded) {
            setQueryHistory(queryHistoryLoaded || []);
        }
    }, [queryHistoryLoaded]);

    const addQueryHistory = async (
        queryHistory: Omit<QueryHistoryItemSchema, "id">,
    ) => {
        try {
            await addQueryHistoryMutation(queryHistory);
        } catch (error) {
            const message =
                error instanceof Error ? error.message : "Unknown error";
            throw new Error(message);
        }
    };

    const deleteQueryHistory = async (
        queryHistoryId: QueryHistoryItemSchema["id"],
    ) => {
        try {
            await deleteQueryHistoryMutation({
                id: queryHistoryId,
            });
        } catch (error) {
            const message =
                error instanceof Error ? error.message : "Unknown error";
            throw new Error(message);
        }
    };

    return (
        <QueryHistoryContext.Provider
            value={{
                queryHistory,
                setQueryHistory,
                addQueryHistory,
                deleteQueryHistory,
            }}>
            {children}
        </QueryHistoryContext.Provider>
    );
};

export const useQueryHistory = () => {
    const context = useContext(QueryHistoryContext);
    if (!context) {
        throw new Error(
            "useQueryHistory must be used within a QueryHistoryProvider",
        );
    }
    return context;
};
