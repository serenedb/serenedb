import {
    useCallback,
    useEffect,
    useMemo,
    useRef,
    type DragEvent,
    type FC,
} from "react";
import { type IDockviewPanelProps } from "dockview";
import type * as Monaco from "monaco-editor";
import { ExecuteQueryButton } from "../../../../features/executeQuery";
import { OpenSavedQueriesModalButton } from "../../../../features/openSavedQueriesModal";
import { useConsole } from "../../Console/model";
import { DropZone } from "../../../shared/DropZone";
import { PGSQLEditor } from "../../../shared/PGSQLEditor";
import { useConnectionAutocomplete } from "../../../shared/PGSQLEditor/model";
import {
    getSavedQueryDragPayload,
    hasSavedQueryDragData,
} from "@serene-ui/shared-frontend/entities";
import {
    type ConsoleResult,
    getSelectedResultIndex,
    addEditorPanel,
    useConsoleQueryExecution,
    useEditorPanelState,
    useResultsPanelManager,
    type EditorPanelParams,
} from "../model";
import { toast } from "sonner";

type StatementHighlightVariant = "success" | "warning" | "error";
const isResolvedTabExecutionStatus = (
    status: EditorPanelParams["tabExecutionStatus"],
) => status === "success" || status === "failed";

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

type EditorHostElement = HTMLElement & {
    __monacoEditor?: Monaco.editor.IStandaloneCodeEditor;
};

export const EditorPanel: FC<IDockviewPanelProps<EditorPanelParams>> = (
    props,
) => {
    const {
        alertOnExecution,
        executeInNewTabByDefault,
        executeSequentiallyByDefault,
        limit,
        showAutocomplete,
        showExecutionHistoryInAutocomplete,
        showSavedQueriesInAutocomplete,
        spawnResultsInFirstTab,
    } = useConsole();
    const autocomplete = useConnectionAutocomplete();
    const editorAutocomplete = useMemo(() => {
        if (!showAutocomplete) {
            return {
                ...autocomplete,
                savedQueries: [],
                queryHistory: [],
            };
        }

        return {
            ...autocomplete,
            savedQueries: showSavedQueriesInAutocomplete
                ? autocomplete.savedQueries
                : [],
            queryHistory: showExecutionHistoryInAutocomplete
                ? autocomplete.queryHistory
                : [],
        };
    }, [
        autocomplete,
        showAutocomplete,
        showExecutionHistoryInAutocomplete,
        showSavedQueriesInAutocomplete,
    ]);
    const { panelState, paramsRef, updatePanelParams } = useEditorPanelState({
        api: props.api,
        params: props.params,
    });
    const { notifyResultsReady, showResultsPanel } = useResultsPanelManager({
        api: props.api,
        alertOnExecution,
        containerApi: props.containerApi,
        getPanelState: () => paramsRef.current,
        spawnResultsInFirstTab,
    });
    const { handleExecute, handleExecuteInNewTab } = useConsoleQueryExecution({
        containerApi: props.containerApi,
        panelId: props.api.id,
        panelState,
        paramsRef,
        updatePanelParams,
        showResultsPanel,
        notifyResultsReady,
        limit,
    });
    const defaultExecutionMode = executeSequentiallyByDefault
        ? "sequential"
        : "transaction";

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
    const editorHostRef = useRef<EditorHostElement | null>(null);

    const focusEditor = useCallback(() => {
        const monacoEditor = editorHostRef.current?.__monacoEditor;

        if (!monacoEditor) {
            return;
        }

        if (typeof window === "undefined") {
            monacoEditor.focus();
            return;
        }

        window.requestAnimationFrame(() => {
            editorHostRef.current?.__monacoEditor?.focus();
        });
    }, []);

    const dismissResolvedExecutionStatus = useCallback(() => {
        updatePanelParams((current) => {
            if (!isResolvedTabExecutionStatus(current.tabExecutionStatus)) {
                return {};
            }

            return {
                tabExecutionStatus: "",
            };
        });
    }, [updatePanelParams]);

    useEffect(() => {
        const subscription = props.api.onDidFocusChange((event) => {
            if (!event.isFocused) {
                return;
            }

            dismissResolvedExecutionStatus();
        });

        return () => subscription.dispose();
    }, [dismissResolvedExecutionStatus, props.api]);

    useEffect(() => {
        const subscription = props.api.onDidActiveChange((event) => {
            if (!event.isActive) {
                return;
            }

            focusEditor();
        });

        return () => subscription.dispose();
    }, [focusEditor, props.api]);

    const handleSavedQueryDrop = useCallback(
        (event: DragEvent<HTMLDivElement>) => {
            if (!hasSavedQueryDragData(event.dataTransfer)) {
                return;
            }

            event.preventDefault();
            event.stopPropagation();

            const payload = getSavedQueryDragPayload(event.dataTransfer);

            if (!payload) {
                return;
            }

            updatePanelParams({
                query: payload.query,
                tabExecutionStatus: isResolvedTabExecutionStatus(
                    panelState.tabExecutionStatus,
                )
                    ? ""
                    : panelState.tabExecutionStatus,
            });
        },
        [panelState.tabExecutionStatus, updatePanelParams],
    );

    const handleFilesDrop = useCallback(
        (files: File[]) => {
            void (async () => {
                try {
                    const fileContents = await Promise.all(
                        files.map(async (file) => ({
                            value: await file.text(),
                        })),
                    );

                    if (!fileContents.length) {
                        return;
                    }

                    updatePanelParams({
                        query: fileContents[0].value,
                        tabExecutionStatus: isResolvedTabExecutionStatus(
                            panelState.tabExecutionStatus,
                        )
                            ? ""
                            : panelState.tabExecutionStatus,
                    });

                    fileContents.slice(1).forEach((fileContent) => {
                        addEditorPanel(props.containerApi, {
                            query: fileContent.value,
                        });
                    });
                } catch (error) {
                    console.error(error);
                    toast.error("Failed to open SQL file", {
                        description: "Please try dropping the file again.",
                    });
                }
            })();
        },
        [panelState.tabExecutionStatus, props.containerApi, updatePanelParams],
    );

    const handleRejectedFiles = useCallback((files: File[]) => {
        if (!files.length) {
            return;
        }

        toast.error("Unsupported file type", {
            description: "Only .sql files can be opened here.",
        });
    }, []);

    return (
        <DropZone
            supportedExtensions={["sql"]}
            onFilesDrop={handleFilesDrop}
            onRejectedFiles={handleRejectedFiles}
            isCustomDragEvent={(event) =>
                hasSavedQueryDragData(event.dataTransfer)
            }
            onCustomDrop={handleSavedQueryDrop}
            customDropLabel="Drop query here"
            className="relative h-full pt-4 ">
            <div
                className="relative h-full w-full"
                onPointerDownCapture={dismissResolvedExecutionStatus}>
                <PGSQLEditor
                    ref={editorHostRef}
                    value={panelState.query}
                    autocomplete={editorAutocomplete}
                    autocompleteEnabled={showAutocomplete}
                    onChange={(query) => {
                        updatePanelParams((current) => ({
                            query,
                            tabExecutionStatus: isResolvedTabExecutionStatus(
                                current.tabExecutionStatus,
                            )
                                ? ""
                                : current.tabExecutionStatus,
                        }));
                    }}
                    highlightRanges={highlightRanges}
                    highlightRange={highlightRange}
                    highlightVariant={
                        highlightRange ? highlightVariant : undefined
                    }
                    onExecute={() => {
                        if (executeInNewTabByDefault) {
                            handleExecuteInNewTab(defaultExecutionMode);
                            return;
                        }

                        void handleExecute(defaultExecutionMode);
                    }}
                    onExecuteInNewTab={() => {
                        handleExecuteInNewTab(defaultExecutionMode);
                    }}
                />
                <div className="absolute right-5.5 bottom-2 flex gap-2">
                    <OpenSavedQueriesModalButton query={panelState.query} />
                    <ExecuteQueryButton
                        query={panelState.query}
                        limit={limit}
                        saveToHistory={true}
                        executeSequentiallyByDefault={
                            executeSequentiallyByDefault
                        }
                        executeInNewTabByDefault={executeInNewTabByDefault}
                        onExecute={handleExecute}
                        onExecuteInNewTab={handleExecuteInNewTab}
                    />
                </div>
            </div>
        </DropZone>
    );
};
