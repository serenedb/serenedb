import type { FC } from "react";
import { type IDockviewPanelProps } from "dockview";
import { ExecuteQueryButton } from "../../../../features/executeQuery";
import { useConsole } from "../../Console/model";
import { PGSQLEditor } from "../../../shared/PGSQLEditor";
import {
    getSelectedResultIndex,
    useConsoleQueryExecution,
    useEditorPanelState,
    useResultsPanelManager,
    type EditorPanelParams,
} from "../model";

export const EditorPanel: FC<IDockviewPanelProps<EditorPanelParams>> = (
    props,
) => {
    const { limit } = useConsole();
    const { panelState, paramsRef, updatePanelParams } = useEditorPanelState({
        api: props.api,
        params: props.params,
    });
    const { showResultsPanel } =
        useResultsPanelManager({
            api: props.api,
            containerApi: props.containerApi,
            getPanelState: () => paramsRef.current,
        });
    const { handleExecute, handleExecuteInNewTab } = useConsoleQueryExecution({
        containerApi: props.containerApi,
        panelState,
        paramsRef,
        updatePanelParams,
        showResultsPanel,
        limit,
    });

    const selectedResultIndex = getSelectedResultIndex(
        panelState.results,
        panelState.selectedResultIndex,
    );
    const activeResult =
        selectedResultIndex >= 0
            ? panelState.results[selectedResultIndex]
            : undefined;
    const highlightRange =
        activeResult &&
        activeResult.sourceQuery === panelState.query &&
        activeResult.statementRange
            ? activeResult.statementRange
            : undefined;
    const highlightVariant =
        activeResult?.status === "failed" ? "error" : "default";

    return (
        <div className="relative h-full pt-4">
            <PGSQLEditor
                value={panelState.query}
                onChange={(query) => {
                    updatePanelParams({ query });
                }}
                highlightRange={highlightRange}
                highlightVariant={highlightRange ? highlightVariant : undefined}
                onExecute={handleExecute}
                onExecuteInNewTab={handleExecuteInNewTab}
            />
            <div className="absolute right-5.5 bottom-2 flex gap-2">
                <ExecuteQueryButton
                    query={panelState.query}
                    limit={limit}
                    saveToHistory={true}
                    onExecute={handleExecute}
                    onExecuteInNewTab={handleExecuteInNewTab}
                />
            </div>
        </div>
    );
};
