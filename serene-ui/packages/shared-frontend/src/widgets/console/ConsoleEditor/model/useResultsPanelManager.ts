import { useCallback } from "react";
import type { IDockviewPanelProps } from "dockview";
import { useNotifications } from "../../../../entities";
import {
    CONSOLE_RESULTS_PANEL_COMPONENT,
    createResultsPanelTitle,
    getResultsPanelId,
} from "./consts";
import type {
    EditorPanelParams,
    NormalizedEditorPanelParams,
    ResultsPanelParams,
} from "./types";
import { useResultsPanelVisibility } from "./useResultsPanelVisibility";

interface UseResultsPanelManagerParams {
    api: IDockviewPanelProps<EditorPanelParams>["api"];
    containerApi: IDockviewPanelProps<EditorPanelParams>["containerApi"];
    getPanelState: () => NormalizedEditorPanelParams;
}

export const useResultsPanelManager = ({
    api,
    containerApi,
    getPanelState,
}: UseResultsPanelManagerParams) => {
    const { addNotification } = useNotifications();
    const resultsPanelId = getResultsPanelId(api.id);
    const isResultsPanelVisible = useResultsPanelVisibility({
        containerApi,
        resultsPanelId,
    });
    const getResultsPanelVisibility = useCallback(() => {
        const panel = containerApi.getPanel(resultsPanelId);

        return panel?.api.isVisible ?? false;
    }, [containerApi, resultsPanelId]);

    const showResultsPanel = useCallback(
        (
            activate = false,
            initialState: NormalizedEditorPanelParams = getPanelState(),
        ) => {
            if (!initialState.results.length) {
                return;
            }

            const resultsPanelTitle = createResultsPanelTitle(api.title);
            const params: ResultsPanelParams = {
                sourcePanelId: api.id,
                initialState,
            };
            const existingPanel = containerApi.getPanel(resultsPanelId);

            if (existingPanel) {
                existingPanel.api.setTitle(resultsPanelTitle);
                existingPanel.api.updateParameters(params);
                if (activate) {
                    existingPanel.api.setActive();
                    return;
                }

                api.setActive();
                return;
            }

            containerApi.addPanel({
                id: resultsPanelId,
                component: CONSOLE_RESULTS_PANEL_COMPONENT,
                title: resultsPanelTitle,
                params,
                position: {
                    referencePanel: api.id,
                    direction: "below",
                },
            });

            if (activate) {
                const resultsPanel = containerApi.getPanel(resultsPanelId);
                resultsPanel?.api.setActive();
                return;
            }

            api.setActive();
        },
        [api, containerApi, getPanelState, resultsPanelId],
    );

    const notifyResultsReady = useCallback(
        (status: "success" | "failed") => {
            if (getResultsPanelVisibility()) {
                return;
            }

            const queryTitle = api.title || "Query";
            const message =
                status === "failed"
                    ? `${queryTitle} finished with errors`
                    : `${queryTitle} results are ready`;

            addNotification({
                id:
                    Date.now() * 1000 +
                    Math.floor(Math.random() * 1000),
                message,
                type: status === "failed" ? "error" : "success",
                createdAt: Date.now(),
            });
        },
        [addNotification, api.title, getResultsPanelVisibility],
    );

    const hideResultsPanel = useCallback(() => {
        const currentPanel = containerApi.getPanel(resultsPanelId);
        if (currentPanel) {
            containerApi.removePanel(currentPanel);
        }
    }, [containerApi, resultsPanelId]);

    return {
        isResultsPanelVisible,
        notifyResultsReady,
        showResultsPanel,
        hideResultsPanel,
    };
};
