import type { FC } from "react";
import { type IDockviewPanelProps } from "dockview";
import { QueryResults } from "../../../shared/QueryResults";
import { useConsole } from "../../Console/model";
import {
    getSelectedResultIndex,
    useSourcePanelState,
    type ResultsPanelParams,
} from "../model";

export const ResultsPanel: FC<IDockviewPanelProps<ResultsPanelParams>> = (
    props,
) => {
    const { colorfulTypesInResults, showJsonByDefault } = useConsole();
    const { sourceState, setSourceState } = useSourcePanelState({
        api: props.api,
        containerApi: props.containerApi,
        params: props.params,
    });

    const selectedResultIndex = getSelectedResultIndex(
        sourceState.results,
        sourceState.selectedResultIndex,
    );

    return (
        <div className="flex h-full flex-col bg-muted">
            <div className="min-h-0 flex-1">
                <QueryResults
                    results={sourceState.results}
                    sourcePanelId={props.params.sourcePanelId}
                    selectedResultIndex={selectedResultIndex}
                    colorfulTypes={colorfulTypesInResults}
                    showJsonByDefault={showJsonByDefault}
                    onSelectResult={(index) => {
                        const sourcePanel = props.containerApi.getPanel(
                            props.params.sourcePanelId,
                        );

                        if (!sourcePanel) {
                            return;
                        }

                        sourcePanel.api.updateParameters({
                            ...sourceState,
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
