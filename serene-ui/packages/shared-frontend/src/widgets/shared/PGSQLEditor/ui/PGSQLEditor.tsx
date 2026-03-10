import { MonacoEditor } from "@serene-ui/shared-frontend/shared";
import React, { useEffect, useRef } from "react";
import type * as Monaco from "monaco-editor";
import { pgsqlFunctions, pgsqlKeywords } from "../model";

const ACTIVE_STATEMENT_DECORATION_CLASS = "serene-active-statement-decoration";
const ACTIVE_ERROR_STATEMENT_DECORATION_CLASS =
    "serene-active-error-statement-decoration";

interface PGSQLEditorProps {
    value: string;
    onChange: (value: string) => void;
    readOnly?: boolean;
    onExecute?: (mode: "sequential" | "transaction") => void;
    onExecuteInNewTab?: () => void;
    autocomplete?: {
        tables: string[];
        views: string[];
        indexes: string[];
        savedQueries: string[];
        queryHistory: string[];
    };
    highlightRange?: {
        startOffset: number;
        endOffset: number;
    };
    highlightVariant?: "default" | "error";
}

let pgsqlCompletionProvider: Monaco.IDisposable | null = null;

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
            highlightVariant = "default",
        },
        ref,
    ) => {
        const autocomplete = autocompleteProp ?? {
            tables: [],
            views: [],
            indexes: [],
            savedQueries: [],
            queryHistory: [],
        };
        const monacoRef = useRef<typeof Monaco | null>(null);
        const editorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(
            null,
        );
        const decorationsRef = useRef<string[]>([]);

        const registerAutocompletion = (monaco: typeof Monaco) => {
            monacoRef.current = monaco;
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
                .monaco-editor .${ACTIVE_STATEMENT_DECORATION_CLASS} {
                    background-color: rgba(59, 130, 246, 0.18);
                    border-bottom: 1px solid rgba(59, 130, 246, 0.45);
                    border-radius: 2px;
                }

                .monaco-editor .${ACTIVE_ERROR_STATEMENT_DECORATION_CLASS} {
                    background-color: rgba(239, 68, 68, 0.18);
                    border-bottom: 1px solid rgba(239, 68, 68, 0.5);
                    border-radius: 2px;
                }
            `;
            document.head.appendChild(style);
        }, []);

        useEffect(() => {
            const hasAutocomplete =
                autocomplete.tables.length > 0 ||
                autocomplete.views.length > 0 ||
                autocomplete.indexes.length > 0 ||
                autocomplete.savedQueries.length > 0 ||
                autocomplete.queryHistory?.length > 0;

            if (!hasAutocomplete) return;
            const monaco = monacoRef.current;
            if (!monaco) return;

            if (pgsqlCompletionProvider) {
                pgsqlCompletionProvider.dispose();
                pgsqlCompletionProvider = null;
            }

            pgsqlCompletionProvider =
                monaco.languages.registerCompletionItemProvider("pgsql", {
                    triggerCharacters: ["."],

                    provideCompletionItems: (
                        model: Monaco.editor.ITextModel,
                        position: Monaco.IPosition,
                    ) => {
                        const word = model.getWordUntilPosition(position);
                        const typedText = word.word.toLowerCase();
                        const wordRange = {
                            startLineNumber: position.lineNumber,
                            endLineNumber: position.lineNumber,
                            startColumn: word.startColumn,
                            endColumn: word.endColumn,
                        };
                        const linePrefix = model.getValueInRange({
                            startLineNumber: position.lineNumber,
                            endLineNumber: position.lineNumber,
                            startColumn: 1,
                            endColumn: position.column,
                        });
                        const linePrefixLower = linePrefix.toLowerCase();
                        const linePrefixRange = {
                            startLineNumber: position.lineNumber,
                            endLineNumber: position.lineNumber,
                            startColumn: 1,
                            endColumn: position.column,
                        };

                        const getInsertRange = (text: string) => {
                            if (
                                linePrefixLower &&
                                text.toLowerCase().startsWith(linePrefixLower)
                            ) {
                                return linePrefixRange;
                            }
                            return wordRange;
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
                                kind: monaco.languages.CompletionItemKind
                                    .Keyword,
                                insertText: kw,
                                filterText: kw,
                                range: wordRange,
                                sortText: getSortText(kw, "0"),
                            }));

                        const functionSuggestions = pgsqlFunctions
                            .filter((fn) => isRelevant(fn))
                            .map((fn) => ({
                                label: fn,
                                kind: monaco.languages.CompletionItemKind
                                    .Function,
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
                                kind: monaco.languages.CompletionItemKind
                                    .Interface,
                                insertText: value,
                                filterText: value,
                                range: wordRange,
                                sortText: getSortText(value, "1"),
                            }));

                        const indexSuggestions = autocomplete.indexes
                            .filter((value) => value && isRelevant(value))
                            .map((value) => ({
                                label: value,
                                kind: monaco.languages.CompletionItemKind
                                    .Property,
                                insertText: value,
                                filterText: value,
                                range: wordRange,
                                sortText: getSortText(value, "1"),
                            }));

                        const savedQueriesSuggestions =
                            autocomplete.savedQueries
                                .filter((value) => value && isRelevant(value))
                                .map((value) => ({
                                    label: value,
                                    kind: monaco.languages.CompletionItemKind
                                        .Enum,
                                    insertText: value,
                                    detail: "Saved Query",
                                    filterText: value,
                                    range: getInsertRange(value),
                                    sortText: getSortText(value, "3"),
                                }));

                        const queryHistorySuggestions =
                            autocomplete.queryHistory
                                .filter((value) => value && isRelevant(value))
                                .map((value) => ({
                                    label: value,
                                    kind: monaco.languages.CompletionItemKind
                                        .Event,
                                    insertText: value,
                                    detail: "Query History",
                                    filterText: value,
                                    range: getInsertRange(value),
                                    sortText: getSortText(value, "3"),
                                }));

                        return {
                            suggestions: [
                                ...tableSuggestions,
                                ...viewSuggestions,
                                ...indexSuggestions,
                                ...savedQueriesSuggestions,
                                ...queryHistorySuggestions,
                                ...functionSuggestions,
                                ...keywordSuggestions,
                            ],
                        };
                    },
                });
        }, [autocomplete]);

        useEffect(() => {
            const editor = editorRef.current;
            const monaco = monacoRef.current;
            const model = editor?.getModel();

            if (!editor || !model || !monaco || !highlightRange) {
                if (editor) {
                    decorationsRef.current = editor.deltaDecorations(
                        decorationsRef.current,
                        [],
                    );
                }
                return;
            }

            const start = model.getPositionAt(highlightRange.startOffset);
            const end = model.getPositionAt(highlightRange.endOffset);

            decorationsRef.current = editor.deltaDecorations(
                decorationsRef.current,
                [
                    {
                        range: new monaco.Range(
                            start.lineNumber,
                            start.column,
                            end.lineNumber,
                            end.column,
                        ),
                        options: {
                            inlineClassName:
                                highlightVariant === "error"
                                    ? ACTIVE_ERROR_STATEMENT_DECORATION_CLASS
                                    : ACTIVE_STATEMENT_DECORATION_CLASS,
                        },
                    },
                ],
            );
        }, [highlightRange, highlightVariant, value]);

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
