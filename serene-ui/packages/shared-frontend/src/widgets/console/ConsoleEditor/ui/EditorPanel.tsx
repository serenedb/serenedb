import { useMemo, type FC } from "react";
import { type IDockviewPanelProps } from "dockview";
import { ExecuteQueryButton } from "../../../../features/executeQuery";
import { useConsole } from "../../Console/model";
import { PGSQLEditor } from "../../../shared/PGSQLEditor";
import { useConnectionAutocomplete } from "../../../shared/PGSQLEditor/model";
import {
    type ConsoleResult,
    getSelectedResultIndex,
    useConsoleQueryExecution,
    useEditorPanelState,
    useResultsPanelManager,
    type EditorPanelParams,
} from "../model";

type StatementHighlightVariant = "success" | "warning" | "error";

const getStatementHighlightVariant = (
    status: ConsoleResult["status"],
): StatementHighlightVariant | undefined => {
    if (status === "failed") {
        return "error";
    }

    if (status === "pending" || status === "running") {
        return "warning";
    }

    if (status === "success") {
        return "success";
    }

    return undefined;
};

const getStatementHighlightPriority = (variant: StatementHighlightVariant) => {
    if (variant === "error") {
        return 3;
    }

    if (variant === "warning") {
        return 2;
    }

    return 1;
};

export const EditorPanel: FC<IDockviewPanelProps<EditorPanelParams>> = (
    props,
) => {
    const { limit } = useConsole();
    const autocomplete = useConnectionAutocomplete();
    const { panelState, paramsRef, updatePanelParams } = useEditorPanelState({
        api: props.api,
        params: props.params,
    });
    const { notifyResultsReady, showResultsPanel } =
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
        notifyResultsReady,
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
    const highlightJobIdsSet = useMemo(
        () => new Set(panelState.highlightJobIds),
        [panelState.highlightJobIds],
    );
    const highlightRanges = useMemo(() => {
        const currentExecutionResults = panelState.results.filter(
            (result) =>
                highlightJobIdsSet.has(result.jobId) &&
                result.sourceQuery === panelState.query &&
                result.statementRange,
        );
        const highlightsByRange = new Map<
            string,
            {
                startOffset: number;
                endOffset: number;
                variant: StatementHighlightVariant;
            }
        >();

        currentExecutionResults.forEach((result) => {
            if (!result.statementRange) {
                return;
            }

            const variant = getStatementHighlightVariant(result.status);

            if (!variant) {
                return;
            }

            const key = `${result.statementRange.startOffset}:${result.statementRange.endOffset}`;
            const existingHighlight = highlightsByRange.get(key);

            if (
                !existingHighlight ||
                getStatementHighlightPriority(variant) >=
                    getStatementHighlightPriority(existingHighlight.variant)
            ) {
                highlightsByRange.set(key, {
                    startOffset: result.statementRange.startOffset,
                    endOffset: result.statementRange.endOffset,
                    variant,
                });
            }
        });

        return Array.from(highlightsByRange.values()).sort(
            (left, right) =>
                left.startOffset - right.startOffset ||
                left.endOffset - right.endOffset,
        );
    }, [highlightJobIdsSet, panelState.query, panelState.results]);
    const highlightRange =
        activeResult &&
        highlightJobIdsSet.has(activeResult.jobId) &&
        activeResult.sourceQuery === panelState.query &&
        activeResult.statementRange
            ? activeResult.statementRange
            : undefined;
    const highlightVariant =
        getStatementHighlightVariant(activeResult?.status || "") || "default";

    return (
        <div className="relative h-full pt-4">
            <PGSQLEditor
                value={panelState.query}
                autocomplete={autocomplete}
                onChange={(query) => {
                    updatePanelParams({ query });
                }}
                highlightRanges={highlightRanges}
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
