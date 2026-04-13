import React, {
    useCallback,
    useContext,
    useEffect,
    useMemo,
    useState,
} from "react";
import type { SavedQuerySchema } from "@serene-ui/shared-core";

const DASHBOARDS_FAVORITE_SAVED_QUERIES_STORAGE_KEY =
    "dashboards:sidebar:favorite-saved-queries:v1";

type FavoriteSavedQuery = Pick<SavedQuerySchema, "id" | "name" | "query">;

interface DashboardsSidebarFavoriteSavedQueriesContextValue {
    isSavedQueryFavorite: (savedQueryId: number) => boolean;
    toggleFavoriteSavedQuery: (savedQuery: FavoriteSavedQuery) => void;
}

const DashboardsSidebarFavoriteSavedQueriesContext = React.createContext<
    DashboardsSidebarFavoriteSavedQueriesContextValue | undefined
>(undefined);

const toSavedQueryFavoriteKey = (savedQueryId: number) => String(savedQueryId);

const parseStoredSavedQueryKeys = (): string[] => {
    if (typeof window === "undefined") {
        return [];
    }

    const rawValue = localStorage.getItem(
        DASHBOARDS_FAVORITE_SAVED_QUERIES_STORAGE_KEY,
    );

    if (!rawValue) {
        return [];
    }

    try {
        const parsedValue = JSON.parse(rawValue);
        if (!Array.isArray(parsedValue)) {
            return [];
        }

        return parsedValue
            .map((value) => String(value))
            .filter((value) => value.length > 0);
    } catch (error) {
        console.warn(
            "Failed to restore dashboard favorite saved queries:",
            error,
        );
        return [];
    }
};

export const DashboardsSidebarFavoriteSavedQueriesProvider: React.FC<
    React.PropsWithChildren
> = ({ children }) => {
    const [favoriteSavedQueryKeys, setFavoriteSavedQueryKeys] = useState<
        string[]
    >(
        () => parseStoredSavedQueryKeys(),
    );

    useEffect(() => {
        try {
            localStorage.setItem(
                DASHBOARDS_FAVORITE_SAVED_QUERIES_STORAGE_KEY,
                JSON.stringify(favoriteSavedQueryKeys),
            );
        } catch (error) {
            console.warn(
                "Failed to persist dashboard favorite saved queries:",
                error,
            );
        }
    }, [favoriteSavedQueryKeys]);

    const toggleFavoriteSavedQuery = useCallback(
        (savedQuery: FavoriteSavedQuery) => {
            const favoriteKey = toSavedQueryFavoriteKey(savedQuery.id);

            setFavoriteSavedQueryKeys((currentKeys) => {
                if (currentKeys.includes(favoriteKey)) {
                    return currentKeys.filter((key) => key !== favoriteKey);
                }

                return [favoriteKey, ...currentKeys];
            });
        },
        [],
    );

    const isSavedQueryFavorite = useCallback(
        (savedQueryId: number) =>
            favoriteSavedQueryKeys.includes(
                toSavedQueryFavoriteKey(savedQueryId),
            ),
        [favoriteSavedQueryKeys],
    );

    const value = useMemo(
        () => ({
            isSavedQueryFavorite,
            toggleFavoriteSavedQuery,
        }),
        [isSavedQueryFavorite, toggleFavoriteSavedQuery],
    );

    return (
        <DashboardsSidebarFavoriteSavedQueriesContext.Provider value={value}>
            {children}
        </DashboardsSidebarFavoriteSavedQueriesContext.Provider>
    );
};

export const useDashboardsSidebarFavoriteSavedQueries = () => {
    const context = useContext(DashboardsSidebarFavoriteSavedQueriesContext);

    if (!context) {
        throw new Error(
            "useDashboardsSidebarFavoriteSavedQueries must be used within a DashboardsSidebarFavoriteSavedQueriesProvider",
        );
    }

    return context;
};
