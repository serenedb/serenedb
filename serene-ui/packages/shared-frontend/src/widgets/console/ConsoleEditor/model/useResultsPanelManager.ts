import { useCallback } from "react";
import type { IDockviewPanelProps } from "dockview";
import {
    CONSOLE_RESULTS_PANEL_COMPONENT,
    getResultsPanelId,
} from "./consts";
import type {
    EditorPanelParams,
    NormalizedEditorPanelParams,
    ResultsPanelParams,
} from "./types";
import { hasResolvedResults } from "./utils";
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
    const resultsPanelId = getResultsPanelId(api.id);
    const isResultsPanelVisible = useResultsPanelVisibility({
        containerApi,
        resultsPanelId,
    });

    const showResultsPanel = useCallback(
        (
            activate = false,
            initialState: NormalizedEditorPanelParams = getPanelState(),
        ) => {
            if (!hasResolvedResults(initialState.results)) {
                return;
            }

            const params: ResultsPanelParams = {
                sourcePanelId: api.id,
                initialState,
            };
            const existingPanel = containerApi.getPanel(resultsPanelId);

            if (existingPanel) {
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
                title: "Results",
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

    const hideResultsPanel = useCallback(() => {
        const currentPanel = containerApi.getPanel(resultsPanelId);
        if (currentPanel) {
            containerApi.removePanel(currentPanel);
        }
    }, [containerApi, resultsPanelId]);

    return {
        isResultsPanelVisible,
        showResultsPanel,
        hideResultsPanel,
    };
};
