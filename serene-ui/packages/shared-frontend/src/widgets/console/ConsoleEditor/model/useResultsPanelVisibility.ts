import { useCallback, useEffect, useState } from "react";
import type { DockviewIDisposable, IDockviewPanelProps } from "dockview";
import type { EditorPanelParams } from "./types";

interface UseResultsPanelVisibilityParams {
    containerApi: IDockviewPanelProps<EditorPanelParams>["containerApi"];
    resultsPanelId: string;
}

export const useResultsPanelVisibility = ({
    containerApi,
    resultsPanelId,
}: UseResultsPanelVisibilityParams) => {
    const getVisibility = useCallback(() => {
        const panel = containerApi.getPanel(resultsPanelId);

        return panel?.api.isVisible ?? false;
    }, [containerApi, resultsPanelId]);

    const [isVisible, setIsVisible] = useState(getVisibility);

    useEffect(() => {
        let panelSubscription: DockviewIDisposable | undefined;

        const syncVisibility = () => {
            setIsVisible(getVisibility());
        };

        const bindPanelEvents = () => {
            panelSubscription?.dispose();

            const panel = containerApi.getPanel(resultsPanelId);

            if (!panel) {
                panelSubscription = undefined;
                return;
            }

            const onVisibilityChange =
                panel.api.onDidVisibilityChange(syncVisibility);
            const onActiveChange = panel.api.onDidActiveChange(syncVisibility);
            const onActiveGroupChange =
                panel.api.onDidActiveGroupChange(syncVisibility);

            panelSubscription = {
                dispose: () => {
                    onVisibilityChange.dispose();
                    onActiveChange.dispose();
                    onActiveGroupChange.dispose();
                },
            };
        };

        const refresh = () => {
            bindPanelEvents();
            syncVisibility();
        };

        refresh();

        const onAdd = containerApi.onDidAddPanel((panel) => {
            if (panel.id === resultsPanelId) {
                refresh();
            }
        });
        const onRemove = containerApi.onDidRemovePanel((panel) => {
            if (panel.id === resultsPanelId) {
                refresh();
            }
        });
        const onActivePanelChange =
            containerApi.onDidActivePanelChange(syncVisibility);
        const onLayoutChange = containerApi.onDidLayoutChange(syncVisibility);

        return () => {
            panelSubscription?.dispose();
            onAdd.dispose();
            onRemove.dispose();
            onActivePanelChange.dispose();
            onLayoutChange.dispose();
        };
    }, [containerApi, getVisibility, resultsPanelId]);

    return isVisible;
};
