import { useCallback, useEffect, useRef } from "react";
import type { IDockviewPanelProps } from "dockview";
import type {
    EditorPanelParams,
    EditorPanelParamsUpdater,
} from "./types";
import { normalizePanelParams } from "./utils";

interface UseEditorPanelStateParams {
    api: IDockviewPanelProps<EditorPanelParams>["api"];
    params?: EditorPanelParams;
}

export const useEditorPanelState = ({
    api,
    params,
}: UseEditorPanelStateParams) => {
    const panelState = normalizePanelParams(params);
    const paramsRef = useRef(panelState);

    useEffect(() => {
        paramsRef.current = panelState;
    }, [panelState]);

    const updatePanelParams = useCallback(
        (updater: EditorPanelParamsUpdater) => {
            const current = paramsRef.current;
            const nextPatch =
                typeof updater === "function" ? updater(current) : updater;
            const next = normalizePanelParams({
                ...current,
                ...nextPatch,
            });

            paramsRef.current = next;
            api.updateParameters(next);
        },
        [api],
    );

    return {
        panelState,
        paramsRef,
        updatePanelParams,
    };
};
