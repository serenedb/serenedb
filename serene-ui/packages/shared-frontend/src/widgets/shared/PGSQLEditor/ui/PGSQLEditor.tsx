import { MonacoEditor } from "@serene-ui/shared-frontend/shared";
import React, { useEffect, useRef } from "react";
import type * as Monaco from "monaco-editor";
import { pgsqlFunctions, pgsqlKeywords } from "../model";

interface PGSQLEditorProps {
    value: string;
    onChange: (value: string) => void;
    readOnly?: boolean;
    onExecute?: () => void;
    onExecuteInNewTab?: () => void;
    autocomplete?: {
        tables: string[];
        views: string[];
        indexes: string[];
        savedQueries: string[];
        queryHistory: string[];
    };
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

        const registerAutocompletion = (monaco: typeof Monaco) => {
            monacoRef.current = monaco;
        };

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

        return (
            <MonacoEditor
                ref={ref}
                language="pgsql"
                beforeMount={registerAutocompletion}
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
