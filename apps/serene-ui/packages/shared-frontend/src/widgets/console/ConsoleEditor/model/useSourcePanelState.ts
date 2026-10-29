import { useCallback, useEffect, useState } from "react";
import type { IDockviewPanelProps } from "dockview";
import type {
    EditorPanelParams,
    NormalizedEditorPanelParams,
    ResultsPanelParams,
} from "./types";
import { hasResolvedResults, normalizePanelParams } from "./utils";

interface UseSourcePanelStateParams {
    api: IDockviewPanelProps<ResultsPanelParams>["api"];
    containerApi: IDockviewPanelProps<ResultsPanelParams>["containerApi"];
    params: ResultsPanelParams;
}

export const useSourcePanelState = ({
    api,
    containerApi,
    params,
}: UseSourcePanelStateParams) => {
    const resolveSourceState = useCallback(() => {
        const sourceState = normalizePanelParams(
            containerApi
                .getPanel(params.sourcePanelId)
                ?.api.getParameters<EditorPanelParams>(),
        );
        const initialState = params.initialState;

        if (!initialState) {
            return sourceState;
        }

        if (
            hasResolvedResults(initialState.results) &&
            !hasResolvedResults(sourceState.results)
        ) {
            return initialState;
        }

        if (
            initialState.results.length > sourceState.results.length &&
            !sourceState.results.length
        ) {
            return initialState;
        }

        return sourceState;
    }, [containerApi, params.initialState, params.sourcePanelId]);

    const [sourceState, setSourceState] =
        useState<NormalizedEditorPanelParams>(resolveSourceState);

    useEffect(() => {
        const sourcePanel = containerApi.getPanel(params.sourcePanelId);

        if (!sourcePanel) {
            const currentPanel = containerApi.getPanel(api.id);
            if (currentPanel) {
                containerApi.removePanel(currentPanel);
            }
            return;
        }

        setSourceState(resolveSourceState());

        const subscription = sourcePanel.api.onDidParametersChange(() => {
            setSourceState(resolveSourceState());
        });

        return () => {
            subscription.dispose();
        };
    }, [api.id, containerApi, params.sourcePanelId, resolveSourceState]);

    return {
        sourceState,
        setSourceState,
    };
};
