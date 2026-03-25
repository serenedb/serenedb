import { useEffect, useState } from "react";
import type { IDockviewPanelProps } from "dockview";
import type { EditorPanelParams } from "./types";

interface UseResultsPanelVisibilityParams {
    containerApi: IDockviewPanelProps<EditorPanelParams>["containerApi"];
    resultsPanelId: string;
}

export const useResultsPanelVisibility = ({
    containerApi,
    resultsPanelId,
}: UseResultsPanelVisibilityParams) => {
    const [isVisible, setIsVisible] = useState(
        Boolean(containerApi.getPanel(resultsPanelId)),
    );

    useEffect(() => {
        const syncVisibility = () => {
            setIsVisible(Boolean(containerApi.getPanel(resultsPanelId)));
        };

        syncVisibility();

        const onAdd = containerApi.onDidAddPanel(syncVisibility);
        const onRemove = containerApi.onDidRemovePanel(syncVisibility);

        return () => {
            onAdd.dispose();
            onRemove.dispose();
        };
    }, [containerApi, resultsPanelId]);

    return isVisible;
};
