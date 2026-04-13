import type { SavedQuerySchema } from "@serene-ui/shared-core";

export const SAVED_QUERY_DRAG_MIME = "application/x-serene-saved-query";

export interface SavedQueryDragPayload {
    id: SavedQuerySchema["id"];
    name: SavedQuerySchema["name"];
    query: SavedQuerySchema["query"];
}

export const createSavedQueryDragPayload = (
    savedQuery: Pick<SavedQuerySchema, "id" | "name" | "query">,
): SavedQueryDragPayload => ({
    id: savedQuery.id,
    name: savedQuery.name,
    query: savedQuery.query,
});

export const setSavedQueryDragData = (
    dataTransfer: DataTransfer,
    payload: SavedQueryDragPayload,
) => {
    dataTransfer.setData(SAVED_QUERY_DRAG_MIME, JSON.stringify(payload));
    // Firefox requires text/plain for drag initialization.
    dataTransfer.setData("text/plain", payload.query);
};

export const getSavedQueryDragPayload = (
    dataTransfer: DataTransfer,
): SavedQueryDragPayload | null => {
    const serializedPayload = dataTransfer.getData(SAVED_QUERY_DRAG_MIME);

    if (!serializedPayload) {
        return null;
    }

    try {
        const parsedPayload = JSON.parse(serializedPayload) as Partial<
            SavedQueryDragPayload
        >;

        if (
            typeof parsedPayload.id !== "number" ||
            typeof parsedPayload.name !== "string" ||
            typeof parsedPayload.query !== "string"
        ) {
            return null;
        }

        return {
            id: parsedPayload.id,
            name: parsedPayload.name,
            query: parsedPayload.query,
        };
    } catch {
        return null;
    }
};

export const hasSavedQueryDragData = (dataTransfer: DataTransfer) =>
    Array.from(dataTransfer.types).includes(SAVED_QUERY_DRAG_MIME);
