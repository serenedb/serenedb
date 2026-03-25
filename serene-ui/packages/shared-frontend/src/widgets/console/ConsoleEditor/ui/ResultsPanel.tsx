import { useCallback } from "react";
import type { FC } from "react";
import { type IDockviewPanelProps } from "dockview";
import { Button } from "../../../../shared";
import { QueryResults } from "../../../shared/QueryResults";
import {
    getSelectedResultIndex,
    normalizePanelParams,
    useSourcePanelState,
    type EditorPanelParams,
    type ResultsPanelParams,
} from "../model";

export const ResultsPanel: FC<IDockviewPanelProps<ResultsPanelParams>> = (
    props,
) => {
    const { sourceState, setSourceState } = useSourcePanelState({
        api: props.api,
        containerApi: props.containerApi,
        params: props.params,
    });

    const hideResults = useCallback(() => {
        const currentPanel = props.containerApi.getPanel(props.api.id);
        if (currentPanel) {
            props.containerApi.removePanel(currentPanel);
        }
    }, [props.api.id, props.containerApi]);

    const selectedResultIndex = getSelectedResultIndex(
        sourceState.results,
        sourceState.selectedResultIndex,
    );

    return (
        <div className="flex h-full flex-col bg-muted">
            <div className="min-h-0 flex-1">
                <QueryResults
                    results={sourceState.results}
                    selectedResultIndex={selectedResultIndex}
                    onSelectResult={(index) => {
                        const sourcePanel = props.containerApi.getPanel(
                            props.params.sourcePanelId,
                        );

                        if (!sourcePanel) {
                            return;
                        }

                        const current = normalizePanelParams(
                            sourcePanel.api.getParameters<EditorPanelParams>(),
                        );

                        sourcePanel.api.updateParameters({
                            ...current,
                            selectedResultIndex: index,
                        });
                        setSourceState((prev) => ({
                            ...prev,
                            selectedResultIndex: index,
                        }));
                    }}
                />
            </div>
        </div>
    );
};
