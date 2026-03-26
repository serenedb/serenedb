import { MonacoEditor } from "@serene-ui/shared-frontend/shared";
import React, { useEffect, useRef } from "react";
import type * as Monaco from "monaco-editor";
import { pgsqlFunctions, pgsqlKeywords } from "../model";

const ACTIVE_STATEMENT_DECORATION_CLASS = "serene-active-statement-decoration";
const ACTIVE_SUCCESS_STATEMENT_DECORATION_CLASS =
    "serene-active-success-statement-decoration";
const ACTIVE_WARNING_STATEMENT_DECORATION_CLASS =
    "serene-active-warning-statement-decoration";
const ACTIVE_ERROR_STATEMENT_DECORATION_CLASS =
    "serene-active-error-statement-decoration";
const STATEMENT_DECORATION_CLASSES = [
    ACTIVE_STATEMENT_DECORATION_CLASS,
    ACTIVE_SUCCESS_STATEMENT_DECORATION_CLASS,
    ACTIVE_WARNING_STATEMENT_DECORATION_CLASS,
    ACTIVE_ERROR_STATEMENT_DECORATION_CLASS,
];

type PGSQLEditorHighlightVariant = "default" | "success" | "warning" | "error";

interface PGSQLEditorHighlightRange {
    startOffset: number;
    endOffset: number;
    variant?: PGSQLEditorHighlightVariant;
}

interface PGSQLEditorProps {
    value?: string;
    onChange?: (value: string) => void;
    readOnly?: boolean;
    onExecute?: (mode: "sequential" | "transaction") => void;
    onExecuteInNewTab?: () => void;
    autocomplete?: {
        tables: string[];
        views: string[];
        indexes: string[];
        savedQueries: Array<{
            name: string;
            query: string;
        }>;
        queryHistory: Array<{
            query: string;
            executedAt: string;
        }>;
    };
    highlightRange?: PGSQLEditorHighlightRange;
    highlightRanges?: PGSQLEditorHighlightRange[];
    highlightVariant?: PGSQLEditorHighlightVariant;
}

let pgsqlCompletionProvider: Monaco.IDisposable | null = null;
let pgsqlInlineCompletionProvider: Monaco.IDisposable | null = null;

const EMPTY_AUTOCOMPLETE: NonNullable<PGSQLEditorProps["autocomplete"]> = {
    tables: [],
    views: [],
    indexes: [],
    savedQueries: [],
    queryHistory: [],
};

let pgsqlAutocompleteData: NonNullable<PGSQLEditorProps["autocomplete"]> =
    EMPTY_AUTOCOMPLETE;

type InlineAutocompleteEntry = {
    acceptedText: string;
    categoryPriority: number;
    detail: string;
    filterText: string;
    order: number;
    previewText: string;
};

const appendInlineAutocompletePreview = (query: string, suffix: string) => {
    if (!query) {
        return `--${suffix}`;
    }

    const separator = /\s$/.test(query) ? "" : " ";

    return `${query}${separator}--${suffix}`;
};

const formatExecutionHistoryTime = (executedAt: string) => {
    const date = new Date(executedAt);

    if (Number.isNaN(date.getTime())) {
        return executedAt;
    }

    const pad = (value: number) => String(value).padStart(2, "0");

    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
};

const buildInlineAutocompleteEntries = (
    autocomplete: NonNullable<PGSQLEditorProps["autocomplete"]>,
): InlineAutocompleteEntry[] => {
    const entries: InlineAutocompleteEntry[] = [];
    const seenEntries = new Set<string>();

    autocomplete.savedQueries.forEach((savedQuery, index) => {
        if (!savedQuery?.query || !savedQuery.name) {
            return;
        }

        const previewText = appendInlineAutocompletePreview(
            savedQuery.query,
            savedQuery.name,
        );
        const key = `saved:${savedQuery.query}\u0000${previewText}`;

        if (seenEntries.has(key)) {
            return;
        }

        seenEntries.add(key);
        entries.push({
            acceptedText: savedQuery.query,
            categoryPriority: 0,
            detail: `Saved Query: ${savedQuery.name}`,
            filterText: `${savedQuery.query} ${savedQuery.name}`,
            order: index,
            previewText,
        });
    });

    autocomplete.queryHistory.forEach((queryHistoryItem, index) => {
        if (!queryHistoryItem?.query || !queryHistoryItem.executedAt) {
            return;
        }

        const executionTime = formatExecutionHistoryTime(
            queryHistoryItem.executedAt,
        );
        const previewText = appendInlineAutocompletePreview(
            queryHistoryItem.query,
            executionTime,
        );
        const key = `history:${queryHistoryItem.query}\u0000${previewText}`;

        if (seenEntries.has(key)) {
            return;
        }

        seenEntries.add(key);
        entries.push({
            acceptedText: queryHistoryItem.query,
            categoryPriority: 1,
            detail: `Execution History: ${executionTime}`,
            filterText: `${queryHistoryItem.query} ${executionTime}`,
            order: index,
            previewText,
        });
    });

    return entries;
};

const getMatchingInlineAutocompleteEntries = (
    currentValue: string,
    autocomplete: NonNullable<PGSQLEditorProps["autocomplete"]>,
) => {
    if (!currentValue.trim()) {
        return [];
    }

    const currentValueLower = currentValue.toLowerCase();

    return buildInlineAutocompleteEntries(autocomplete)
        .filter(
            (entry) =>
                entry.acceptedText.length > currentValue.length &&
                entry.acceptedText
                    .toLowerCase()
                    .startsWith(currentValueLower),
        )
        .sort(
            (left, right) =>
                left.categoryPriority - right.categoryPriority ||
                left.acceptedText.length - right.acceptedText.length ||
                left.order - right.order,
        );
};

const isCursorAtModelEnd = (
    model: Monaco.editor.ITextModel,
    position: Monaco.IPosition,
) => {
    const fullRange = model.getFullModelRange();

    return (
        position.lineNumber === fullRange.endLineNumber &&
        position.column === fullRange.endColumn
    );
};

const ensurePgsqlAutocompleteProviders = (monaco: typeof Monaco) => {
    if (!pgsqlCompletionProvider) {
        pgsqlCompletionProvider =
            monaco.languages.registerCompletionItemProvider("pgsql", {
                triggerCharacters: ["."],

                provideCompletionItems: (
                    model: Monaco.editor.ITextModel,
                    position: Monaco.IPosition,
                ) => {
                    const autocomplete = pgsqlAutocompleteData;
                    const word = model.getWordUntilPosition(position);
                    const typedText = word.word.toLowerCase();
                    const wordRange = {
                        startLineNumber: position.lineNumber,
                        endLineNumber: position.lineNumber,
                        startColumn: word.startColumn,
                        endColumn: word.endColumn,
                    };

                    const getSortText = (
                        text: string,
                        categoryPrefix: string,
                    ) => {
                        if (!text) {
                            return `${categoryPrefix}_3_`;
                        }
                        const lowerText = text.toLowerCase();
                        if (!typedText) {
                            return `${categoryPrefix}_2_${text}`;
                        }
                        if (lowerText === typedText) {
                            return `${categoryPrefix}_0_${text}`;
                        }
                        if (lowerText.startsWith(typedText)) {
                            return `${categoryPrefix}_1_${text}`;
                        }
                        if (lowerText.includes(typedText)) {
                            return `${categoryPrefix}_2_${text}`;
                        }
                        return `${categoryPrefix}_3_${text}`;
                    };

                    const isRelevant = (text: string) => {
                        if (!typedText) return true;
                        const lowerText = text.toLowerCase();
                        return (
                            lowerText.startsWith(typedText) ||
                            lowerText.includes(typedText)
                        );
                    };

                    const keywordSuggestions = pgsqlKeywords
                        .filter((kw) => isRelevant(kw))
                        .map((kw) => ({
                            label: kw,
                            kind: monaco.languages.CompletionItemKind.Keyword,
                            insertText: kw,
                            filterText: kw,
                            range: wordRange,
                            sortText: getSortText(kw, "0"),
                        }));

                    const functionSuggestions = pgsqlFunctions
                        .filter((fn) => isRelevant(fn))
                        .map((fn) => ({
                            label: fn,
                            kind: monaco.languages.CompletionItemKind.Function,
                            insertText: `${fn}()`,
                            filterText: fn,
                            range: wordRange,
                            sortText: getSortText(fn, "2"),
                        }));

                    const tableSuggestions = autocomplete.tables
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Class,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, "1"),
                        }));

                    const viewSuggestions = autocomplete.views
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Interface,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, "1"),
                        }));

                    const indexSuggestions = autocomplete.indexes
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Property,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, "1"),
                        }));

                    return {
                        suggestions: [
                            ...tableSuggestions,
                            ...viewSuggestions,
                            ...indexSuggestions,
                            ...functionSuggestions,
                            ...keywordSuggestions,
                        ],
                    };
                },
            });
    }

    if (!pgsqlInlineCompletionProvider) {
        pgsqlInlineCompletionProvider =
            monaco.languages.registerInlineCompletionsProvider("pgsql", {
                provideInlineCompletions: (
                    model: Monaco.editor.ITextModel,
                    position: Monaco.Position,
                ) => {
                    if (!isCursorAtModelEnd(model, position)) {
                        return {
                            items: [],
                        };
                    }

                    const currentValue = model.getValue();
                    const matchingEntries = getMatchingInlineAutocompleteEntries(
                        currentValue,
                        pgsqlAutocompleteData,
                    );

                    if (!matchingEntries.length) {
                        return {
                            items: [],
                        };
                    }

                    const range = {
                        startLineNumber: position.lineNumber,
                        endLineNumber: position.lineNumber,
                        startColumn: position.column,
                        endColumn: position.column,
                    };

                    return {
                        items: matchingEntries.map((entry) => ({
                            insertText: entry.previewText.slice(
                                currentValue.length,
                            ),
                            filterText: entry.filterText,
                            range,
                        })),
                        suppressSuggestions: true,
                    };
                },
                disposeInlineCompletions: () => undefined,
            });
    }
};

const getStatementDecorationClassName = (
    variant?: PGSQLEditorHighlightVariant,
) => {
    if (variant === "success") {
        return ACTIVE_SUCCESS_STATEMENT_DECORATION_CLASS;
    }

    if (variant === "warning") {
        return ACTIVE_WARNING_STATEMENT_DECORATION_CLASS;
    }

    if (variant === "error") {
        return ACTIVE_ERROR_STATEMENT_DECORATION_CLASS;
    }

    return ACTIVE_STATEMENT_DECORATION_CLASS;
};

const getStatementDecorationPriority = (
    variant?: PGSQLEditorHighlightVariant,
) => {
    if (variant === "error") {
        return 3;
    }

    if (variant === "warning") {
        return 2;
    }

    if (variant === "success") {
        return 1;
    }

    return 0;
};

export const PGSQLEditor = React.forwardRef<HTMLElement, PGSQLEditorProps>(
    (
        {
            value,
            onChange,
            readOnly,
            onExecute,
            onExecuteInNewTab,
            autocomplete: autocompleteProp,
            highlightRange,
            highlightRanges,
            highlightVariant = "default",
        },
        ref,
    ) => {
        const autocomplete = autocompleteProp ?? EMPTY_AUTOCOMPLETE;
        const editorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(
            null,
        );
        const pendingInlineAutocompleteRef =
            useRef<InlineAutocompleteEntry | null>(null);
        const updatePendingInlineAutocompleteRef = useRef<() => void>(
            () => undefined,
        );

        const registerAutocompletion = (monaco: typeof Monaco) => {
            ensurePgsqlAutocompleteProviders(monaco);
        };

        useEffect(() => {
            if (
                typeof document === "undefined" ||
                document.getElementById("serene-statement-decoration-styles")
            ) {
                return;
            }

            const style = document.createElement("style");
            style.id = "serene-statement-decoration-styles";
            style.textContent = `
                .monaco-editor .lines-content > .view-lines > .view-line > span.${ACTIVE_STATEMENT_DECORATION_CLASS} {
                    background-color: rgba(59, 130, 246, 0.18);
                    border-bottom: 1px solid rgba(59, 130, 246, 0.45);
                    border-radius: 2px;
                }

                .monaco-editor .lines-content > .view-lines > .view-line > span.${ACTIVE_SUCCESS_STATEMENT_DECORATION_CLASS} {
                    background-color: rgba(34, 197, 94, 0.16);
                    border-bottom: 1px solid rgba(34, 197, 94, 0.45);
                    border-radius: 2px;
                }

                .monaco-editor .lines-content > .view-lines > .view-line > span.${ACTIVE_WARNING_STATEMENT_DECORATION_CLASS} {
                    background-image: linear-gradient(
                        90deg,
                        rgba(234, 179, 8, 0.18) 0%,
                        rgba(250, 204, 21, 0.4) 50%,
                        rgba(234, 179, 8, 0.18) 100%
                    );
                    background-size: 220% 100%;
                    animation: serene-statement-warning-sweep 1.4s ease-in-out infinite;
                    border-bottom: 1px solid rgba(234, 179, 8, 0.55);
                    border-radius: 2px;
                }

                .monaco-editor .lines-content > .view-lines > .view-line > span.${ACTIVE_ERROR_STATEMENT_DECORATION_CLASS} {
                    background-color: rgba(239, 68, 68, 0.18);
                    border-bottom: 1px solid rgba(239, 68, 68, 0.5);
                    border-radius: 2px;
                }

                @keyframes serene-statement-warning-sweep {
                    0% {
                        background-position: 200% 0;
                    }

                    100% {
                        background-position: -20% 0;
                    }
                }
            `;
            document.head.appendChild(style);
        }, []);

        useEffect(() => {
            pgsqlAutocompleteData = autocomplete;
            updatePendingInlineAutocompleteRef.current();
            editorRef.current?.trigger(
                "serene-pgsql-autocomplete",
                "editor.action.inlineSuggest.trigger",
                {},
            );
        }, [autocomplete]);

        useEffect(() => {
            const editor = editorRef.current;
            const model = editor?.getModel();
            const nextHighlightRanges =
                highlightRanges && highlightRanges.length > 0
                    ? highlightRanges
                    : highlightRange
                      ? [
                            {
                                ...highlightRange,
                                variant: highlightVariant,
                            },
                        ]
                      : [];

            if (!editor || !model) {
                return;
            }

            let frameId: number | null = null;

            const clearStatementDecorations = () => {
                const editorNode = editor.getDomNode();

                if (!editorNode) {
                    return;
                }

                editorNode
                    .querySelectorAll<HTMLElement>(
                        ".lines-content > .view-lines > .view-line > span",
                    )
                    .forEach((lineSpan) => {
                        lineSpan.classList.remove(
                            ...STATEMENT_DECORATION_CLASSES,
                        );
                    });
            };

            const lineClassNames = new Map<number, string>();
            const lineClassPriorities = new Map<number, number>();

            nextHighlightRanges.forEach((currentHighlightRange) => {
                const start = model.getPositionAt(
                    currentHighlightRange.startOffset,
                );
                const endOffset = Math.max(
                    currentHighlightRange.startOffset,
                    currentHighlightRange.endOffset - 1,
                );
                const end = model.getPositionAt(endOffset);
                const decorationPriority = getStatementDecorationPriority(
                    currentHighlightRange.variant,
                );
                const decorationClassName = getStatementDecorationClassName(
                    currentHighlightRange.variant,
                );

                for (
                    let lineNumber = start.lineNumber;
                    lineNumber <= end.lineNumber;
                    lineNumber += 1
                ) {
                    const currentPriority =
                        lineClassPriorities.get(lineNumber) ?? -1;

                    if (decorationPriority >= currentPriority) {
                        lineClassPriorities.set(lineNumber, decorationPriority);
                        lineClassNames.set(lineNumber, decorationClassName);
                    }
                }
            });

            const applyStatementDecorations = () => {
                clearStatementDecorations();

                if (!lineClassNames.size) {
                    return;
                }

                const editorNode = editor.getDomNode();

                if (!editorNode) {
                    return;
                }

                lineClassNames.forEach((decorationClassName, lineNumber) => {
                    const targetTop = editor.getTopForLineNumber(lineNumber);
                    const lineNode = Array.from(
                        editorNode.querySelectorAll<HTMLElement>(
                            ".lines-content > .view-lines > .view-line",
                        ),
                    ).find((candidate) => {
                        const candidateTop = Number.parseFloat(
                            candidate.style.top || "",
                        );

                        return (
                            Number.isFinite(candidateTop) &&
                            Math.abs(candidateTop - targetTop) < 0.5
                        );
                    });
                    const lineSpan = lineNode?.firstElementChild;

                    if (!(lineSpan instanceof HTMLElement)) {
                        return;
                    }

                    lineSpan.classList.add(decorationClassName);
                });
            };

            const scheduleApplyStatementDecorations = () => {
                if (typeof window === "undefined") {
                    applyStatementDecorations();
                    return;
                }

                if (frameId !== null) {
                    window.cancelAnimationFrame(frameId);
                }

                frameId = window.requestAnimationFrame(() => {
                    frameId = null;
                    applyStatementDecorations();
                });
            };

            scheduleApplyStatementDecorations();

            const scrollDisposable = editor.onDidScrollChange(() => {
                scheduleApplyStatementDecorations();
            });
            const layoutDisposable = editor.onDidLayoutChange(() => {
                scheduleApplyStatementDecorations();
            });

            return () => {
                if (typeof window !== "undefined" && frameId !== null) {
                    window.cancelAnimationFrame(frameId);
                }

                scrollDisposable.dispose();
                layoutDisposable.dispose();
                clearStatementDecorations();
            };
        }, [highlightRange, highlightRanges, highlightVariant, value]);

        useEffect(() => {
            const editor = editorRef.current;

            if (!editor) {
                return;
            }

            const updatePendingInlineAutocomplete = () => {
                const model = editor.getModel();
                const position = editor.getPosition();

                if (!model || !position || !isCursorAtModelEnd(model, position)) {
                    pendingInlineAutocompleteRef.current = null;
                    return;
                }

                pendingInlineAutocompleteRef.current =
                    getMatchingInlineAutocompleteEntries(
                        model.getValue(),
                        pgsqlAutocompleteData,
                    )[0] ?? null;
            };

            updatePendingInlineAutocompleteRef.current =
                updatePendingInlineAutocomplete;
            updatePendingInlineAutocomplete();

            const handleKeyDown = (event: KeyboardEvent) => {
                if (
                    event.key !== "Tab" ||
                    event.shiftKey ||
                    event.altKey ||
                    event.ctrlKey ||
                    event.metaKey
                ) {
                    return;
                }

                const pendingInlineAutocomplete =
                    pendingInlineAutocompleteRef.current;

                if (!pendingInlineAutocomplete) {
                    return;
                }

                const model = editor.getModel();
                const position = editor.getPosition();

                if (!model || !position || !isCursorAtModelEnd(model, position)) {
                    pendingInlineAutocompleteRef.current = null;
                    return;
                }

                const currentValue = model.getValue();

                if (
                    !pendingInlineAutocomplete.acceptedText
                        .toLowerCase()
                        .startsWith(currentValue.toLowerCase()) ||
                    pendingInlineAutocomplete.acceptedText.length <=
                        currentValue.length
                ) {
                    pendingInlineAutocompleteRef.current = null;
                    return;
                }

                const textToInsert =
                    pendingInlineAutocomplete.acceptedText.slice(
                        currentValue.length,
                    );

                event.preventDefault();
                event.stopPropagation();
                event.stopImmediatePropagation();

                editor.pushUndoStop();
                editor.executeEdits("serene-pgsql-autocomplete", [
                    {
                        forceMoveMarkers: true,
                        range: {
                            startLineNumber: position.lineNumber,
                            endLineNumber: position.lineNumber,
                            startColumn: position.column,
                            endColumn: position.column,
                        },
                        text: textToInsert,
                    },
                ]);
                editor.pushUndoStop();

                pendingInlineAutocompleteRef.current = null;
                editor.trigger(
                    "serene-pgsql-autocomplete",
                    "editor.action.inlineSuggest.hide",
                    {},
                );
            };

            const domNode = editor.getDomNode();
            const modelChangeDisposable = editor.onDidChangeModelContent(() => {
                updatePendingInlineAutocomplete();
            });
            const cursorChangeDisposable = editor.onDidChangeCursorPosition(() => {
                updatePendingInlineAutocomplete();
            });

            domNode?.addEventListener("keydown", handleKeyDown, true);

            return () => {
                updatePendingInlineAutocompleteRef.current = () => undefined;
                pendingInlineAutocompleteRef.current = null;
                modelChangeDisposable.dispose();
                cursorChangeDisposable.dispose();
                domNode?.removeEventListener("keydown", handleKeyDown, true);
            };
        }, []);

        return (
            <MonacoEditor
                ref={ref}
                language="pgsql"
                beforeMount={registerAutocompletion}
                onMount={(editor) => {
                    editorRef.current = editor;
                }}
                options={{
                    suggestOnTriggerCharacters: true,
                    quickSuggestions: true,
                    inlineSuggest: {
                        enabled: true,
                        mode: "prefix",
                        showToolbar: "onHover",
                    },
                    tabCompletion: "on",
                    wordBasedSuggestions: "allDocuments",
                    minimap: {
                        enabled: false,
                    },
                    readOnly,
                }}
                value={value}
                onChange={onChange}
                onExecute={onExecute}
                onExecuteInNewTab={onExecuteInNewTab}
            />
        );
    },
);
