import type { SavedQuerySchema } from "@serene-ui/shared-core";

export const DASHBOARD_SAVED_QUERY_DRAG_MIME =
    "application/x-serene-dashboard-saved-query";
const DASHBOARD_SAVED_QUERY_DRAG_STATE_EVENT =
    "dashboard-saved-query-drag-state-change";
let isDashboardSavedQueryDragActive = false;

export interface DashboardSavedQueryDragPayload {
    id: SavedQuerySchema["id"];
    name: SavedQuerySchema["name"];
    query: SavedQuerySchema["query"];
}

export const createDashboardSavedQueryDragPayload = (
    savedQuery: Pick<SavedQuerySchema, "id" | "name" | "query">,
): DashboardSavedQueryDragPayload => ({
    id: savedQuery.id,
    name: savedQuery.name,
    query: savedQuery.query,
});

export const setDashboardSavedQueryDragData = (
    dataTransfer: DataTransfer,
    payload: DashboardSavedQueryDragPayload,
) => {
    const serializedPayload = JSON.stringify(payload);

    dataTransfer.setData(DASHBOARD_SAVED_QUERY_DRAG_MIME, serializedPayload);
    // Firefox requires text/plain for drag-and-drop initialization.
    dataTransfer.setData("text/plain", serializedPayload);
};

export const getDashboardSavedQueryDragPayload = (
    dataTransfer: DataTransfer,
): DashboardSavedQueryDragPayload | null => {
    const serializedPayload = dataTransfer.getData(DASHBOARD_SAVED_QUERY_DRAG_MIME);

    if (!serializedPayload) {
        return null;
    }

    try {
        const parsedPayload = JSON.parse(serializedPayload) as Partial<
            DashboardSavedQueryDragPayload
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

export const hasDashboardSavedQueryDragData = (dataTransfer: DataTransfer) => {
    return Array.from(dataTransfer.types).includes(
        DASHBOARD_SAVED_QUERY_DRAG_MIME,
    );
};

export const getDashboardSavedQueryDragActive = () =>
    isDashboardSavedQueryDragActive;

export const setDashboardSavedQueryDragActive = (isActive: boolean) => {
    if (isDashboardSavedQueryDragActive === isActive) {
        return;
    }

    isDashboardSavedQueryDragActive = isActive;

    if (typeof window === "undefined") {
        return;
    }

    window.dispatchEvent(
        new CustomEvent<boolean>(DASHBOARD_SAVED_QUERY_DRAG_STATE_EVENT, {
            detail: isActive,
        }),
    );
};

export const subscribeDashboardSavedQueryDragActive = (
    onChange: (isActive: boolean) => void,
) => {
    if (typeof window === "undefined") {
        return () => {};
    }

    const handleStateChange = (event: Event) => {
        const customEvent = event as CustomEvent<boolean>;
        onChange(customEvent.detail);
    };

    window.addEventListener(
        DASHBOARD_SAVED_QUERY_DRAG_STATE_EVENT,
        handleStateChange,
    );

    return () => {
        window.removeEventListener(
            DASHBOARD_SAVED_QUERY_DRAG_STATE_EVENT,
            handleStateChange,
        );
    };
};
