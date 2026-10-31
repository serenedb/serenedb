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
type ExecuteQueryMode =
    | "sequential"
    | "sequentialIgnoreErrors"
    | "transaction";

interface PGSQLEditorHighlightRange {
    startOffset: number;
    endOffset: number;
    variant?: PGSQLEditorHighlightVariant;
}

interface PGSQLEditorProps {
    value?: string;
    onChange?: (value: string) => void;
    readOnly?: boolean;
    onExecute?: (mode: ExecuteQueryMode) => void;
    onExecuteInNewTab?: (mode?: ExecuteQueryMode) => void;
    autocompleteEnabled?: boolean;
    autocomplete?: {
        tables: string[];
        systemTables: string[];
        views: string[];
        indexes: string[];
        sequences: string[];
        schemas: string[];
        columns: string[];
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
let pgsqlAutocompleteEnabled = true;
const INLINE_AUTOCOMPLETE_ENABLED = true;

const EMPTY_AUTOCOMPLETE: NonNullable<PGSQLEditorProps["autocomplete"]> = {
    tables: [],
    systemTables: [],
    views: [],
    indexes: [],
    sequences: [],
    schemas: [],
    columns: [],
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
                entry.acceptedText.toLowerCase().startsWith(currentValueLower),
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

const isSuggestWidgetVisible = (
    editor: Monaco.editor.IStandaloneCodeEditor,
) => {
    const domNode = editor.getDomNode();

    if (!domNode) {
        return false;
    }

    return Boolean(domNode.querySelector(".suggest-widget.visible"));
};

const isEditorModelAlive = (editor: Monaco.editor.IStandaloneCodeEditor) => {
    try {
        const model = editor.getModel();

        return Boolean(model && !model.isDisposed());
    } catch {
        return false;
    }
};

type SqlEntityContext =
    | "relations"
    | "columns"
    | "indexes"
    | "schemas"
    | "none";

const SQL_STATEMENT_CONTEXT_RULES: Array<{
    pattern: RegExp;
    keywords: string[];
    entityContext?: SqlEntityContext;
}> = [
    {
        pattern: /^\s*SELECT\b[\s\S]*\*\s*$/i,
        keywords: ["FROM"],
    },
    {
        pattern: /^\s*SELECT\b[\s\S]*\b(FROM|JOIN)\s*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*SELECT\b[\s\S]*\b(FROM|JOIN)\s+[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*SELECT\b[\s\S]*\bFROM\s+[\w."$]+\s+$/i,
        keywords: [
            "WHERE",
            "JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "INNER JOIN",
            "FULL JOIN",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
        ],
    },
    {
        pattern: /^\s*SELECT\b[\s\S]*\b(WHERE|HAVING)\s*$/i,
        keywords: ["AND", "OR", "NOT", "IS", "IN", "LIKE", "BETWEEN"],
        entityContext: "columns",
    },
    {
        pattern: /^\s*SELECT\b[\s\S]*\b(WHERE|HAVING)\s+[\w."$]*$/i,
        keywords: [
            "AND",
            "OR",
            "IS",
            "IN",
            "LIKE",
            "BETWEEN",
            "ORDER BY",
            "LIMIT",
        ],
        entityContext: "columns",
    },
    {
        pattern: /^\s*SELECT\b[\s\S]*\bORDER\s+BY\s+[\w."$,\s]*$/i,
        keywords: [
            "ASC",
            "DESC",
            "NULLS FIRST",
            "NULLS LAST",
            "LIMIT",
            "OFFSET",
        ],
        entityContext: "columns",
    },
    {
        pattern: /^\s*INSERT\s+INTO\s*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*INSERT\b[\s\S]*\bINTO\s+[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*INSERT\b[\s\S]*\bINTO\s+[\w."$]+\s+$/i,
        keywords: ["(", "VALUES", "SELECT", "DEFAULT VALUES", "RETURNING"],
    },
    {
        pattern: /^\s*UPDATE\s*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*UPDATE\s+[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*UPDATE\s+[\w."$]+\s+$/i,
        keywords: ["SET", "FROM", "WHERE", "RETURNING"],
    },
    {
        pattern: /^\s*UPDATE\b[\s\S]*\bSET\s+[\w."$,\s=()*+\-/'"]*$/i,
        keywords: ["WHERE", "FROM", "RETURNING"],
        entityContext: "columns",
    },
    {
        pattern: /^\s*DELETE\s*$/i,
        keywords: ["FROM"],
    },
    {
        pattern: /^\s*DELETE\b[\s\S]*\bFROM\s*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*DELETE\b[\s\S]*\bFROM\s+[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*DELETE\b[\s\S]*\bFROM\s+[\w."$]+\s+$/i,
        keywords: ["USING", "WHERE", "RETURNING"],
    },
    {
        pattern: /^\s*CREATE\s+TABLE\s+[\w."$]*$/i,
        keywords: ["IF NOT EXISTS", "AS", "(", "INHERITS", "PARTITION BY"],
        entityContext: "relations",
    },
    {
        pattern: /^\s*CREATE\s+TABLE\b[\s\S]*$/i,
        keywords: [
            "IF NOT EXISTS",
            "AS",
            "(",
            "INHERITS",
            "PARTITION BY",
            "TABLESPACE",
        ],
        entityContext: "relations",
    },
    {
        pattern: /^\s*CREATE\s+(MATERIALIZED\s+)?VIEW\b[\s\S]*$/i,
        keywords: ["AS", "WITH", "SELECT"],
        entityContext: "relations",
    },
    {
        pattern: /^\s*CREATE\s+INDEX\b[\s\S]*\bON\s*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*CREATE\s+INDEX\b[\s\S]*\bON\s+[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*CREATE\s+INDEX\b[\s\S]*$/i,
        keywords: ["ON", "USING", "(", "WHERE", "TABLESPACE", "CONCURRENTLY"],
        entityContext: "indexes",
    },
    {
        pattern: /^\s*ALTER\s+TABLE\s*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*ALTER\s+TABLE\s+[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*ALTER\s+TABLE\s+[\w."$]+\s+$/i,
        keywords: [
            "ADD COLUMN",
            "DROP COLUMN",
            "ALTER COLUMN",
            "RENAME COLUMN",
            "RENAME TO",
            "SET SCHEMA",
        ],
    },
    {
        pattern: /^\s*DROP\s+(TABLE|VIEW|INDEX|SEQUENCE)\s*$/i,
        keywords: ["IF EXISTS"],
        entityContext: "relations",
    },
    {
        pattern:
            /^\s*DROP\s+(TABLE|VIEW|INDEX|SEQUENCE)\s+(IF\s+EXISTS\s+)?[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern:
            /^\s*DROP\s+(TABLE|VIEW|INDEX|SEQUENCE)\s+(IF\s+EXISTS\s+)?[\w."$]+\s+$/i,
        keywords: ["CASCADE", "RESTRICT"],
    },
    {
        pattern: /^\s*TRUNCATE\s*$/i,
        keywords: ["TABLE"],
    },
    {
        pattern: /^\s*TRUNCATE\b[\s\S]*\bTABLE\s*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*TRUNCATE\b[\s\S]*\bTABLE\s+[\w."$]*$/i,
        keywords: [],
        entityContext: "relations",
    },
    {
        pattern: /^\s*TRUNCATE\b[\s\S]*\bTABLE\s+[\w."$]+\s+$/i,
        keywords: [
            "RESTART IDENTITY",
            "CONTINUE IDENTITY",
            "CASCADE",
            "RESTRICT",
        ],
    },
    {
        pattern: /^\s*TRUNCATE\b[\s\S]*$/i,
        keywords: [
            "TABLE",
            "RESTART IDENTITY",
            "CONTINUE IDENTITY",
            "CASCADE",
            "RESTRICT",
        ],
        entityContext: "relations",
    },
    {
        pattern: /^\s*GRANT\b[\s\S]*$/i,
        keywords: ["ON", "TO", "WITH GRANT OPTION"],
    },
    {
        pattern: /^\s*REVOKE\b[\s\S]*$/i,
        keywords: ["ON", "FROM", "CASCADE", "RESTRICT"],
    },
];

const COMMON_SQL_KEYWORDS = new Set([
    "SELECT",
    "FROM",
    "WHERE",
    "JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "INNER JOIN",
    "FULL JOIN",
    "GROUP BY",
    "ORDER BY",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "INSERT",
    "INTO",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "VALUES",
    "RETURNING",
]);

const getKeywordPriority = (
    keyword: string,
    context: SqlEntityContext,
    isContextualKeyword: boolean,
) => {
    const normalizedKeyword = keyword.toUpperCase();
    const isCommonKeyword = COMMON_SQL_KEYWORDS.has(normalizedKeyword);

    if (isContextualKeyword) {
        if (context === "relations") {
            return isCommonKeyword ? 2 : 3;
        }

        if (context === "columns") {
            return isCommonKeyword ? 2 : 3;
        }

        return isCommonKeyword ? 0 : 1;
    }

    return isCommonKeyword ? 3 : 5;
};

const getCurrentStatementText = (textBeforeCursor: string) => {
    const statements = textBeforeCursor.split(";");
    return statements.length
        ? statements[statements.length - 1]
        : textBeforeCursor;
};

const getStatementLeadingKeyword = (statementText: string) => {
    const trimmed = statementText.trim().toUpperCase();
    if (!trimmed) {
        return "";
    }

    if (trimmed.startsWith("WITH")) {
        const candidate = trimmed.match(
            /\b(SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b/,
        );
        return candidate?.[1] ?? "WITH";
    }

    const firstWord = trimmed.match(/^[A-Z_]+/);
    return firstWord?.[0] ?? "";
};

const getContextualSuggestionConfig = (statementText: string) => {
    const rawStatement = statementText;
    const trimmed = statementText.trim();
    if (!trimmed) {
        return {
            keywords: [
                "SELECT",
                "WITH",
                "INSERT",
                "UPDATE",
                "DELETE",
                "CREATE",
                "ALTER",
                "DROP",
                "TRUNCATE",
                "GRANT",
                "REVOKE",
            ],
            entityContext: "none" as SqlEntityContext,
        };
    }

    const matchedRule = SQL_STATEMENT_CONTEXT_RULES.find((rule) =>
        rule.pattern.test(rawStatement),
    );

    if (matchedRule) {
        return {
            keywords: matchedRule.keywords,
            entityContext: matchedRule.entityContext ?? "none",
        };
    }

    const statementKeyword = getStatementLeadingKeyword(trimmed);

    if (statementKeyword === "SELECT") {
        return {
            keywords: [
                "FROM",
                "WHERE",
                "GROUP BY",
                "HAVING",
                "ORDER BY",
                "LIMIT",
                "OFFSET",
                "UNION",
            ],
            entityContext: "columns" as SqlEntityContext,
        };
    }

    if (
        statementKeyword === "INSERT" ||
        statementKeyword === "UPDATE" ||
        statementKeyword === "DELETE"
    ) {
        return {
            keywords: ["FROM", "WHERE", "RETURNING"],
            entityContext: "relations" as SqlEntityContext,
        };
    }

    if (
        statementKeyword === "CREATE" ||
        statementKeyword === "ALTER" ||
        statementKeyword === "DROP"
    ) {
        return {
            keywords: [
                "TABLE",
                "VIEW",
                "INDEX",
                "SEQUENCE",
                "SCHEMA",
                "FUNCTION",
            ],
            entityContext: "relations" as SqlEntityContext,
        };
    }

    return {
        keywords: [],
        entityContext: "none" as SqlEntityContext,
    };
};

const ensurePgsqlAutocompleteProviders = (monaco: typeof Monaco) => {
    if (!pgsqlCompletionProvider) {
        pgsqlCompletionProvider =
            monaco.languages.registerCompletionItemProvider("pgsql", {
                triggerCharacters: [".", " "],

                provideCompletionItems: (
                    model: Monaco.editor.ITextModel,
                    position: Monaco.IPosition,
                ) => {
                    if (!pgsqlAutocompleteEnabled) {
                        return {
                            suggestions: [],
                        };
                    }

                    const autocomplete = pgsqlAutocompleteData;
                    const word = model.getWordUntilPosition(position);
                    const typedText = word.word.toLowerCase();
                    const statementBeforeCursor = getCurrentStatementText(
                        model.getValueInRange({
                            startLineNumber: 1,
                            startColumn: 1,
                            endLineNumber: position.lineNumber,
                            endColumn: position.column,
                        }),
                    );
                    const contextualConfig = getContextualSuggestionConfig(
                        statementBeforeCursor,
                    );
                    const wordRange = {
                        startLineNumber: position.lineNumber,
                        endLineNumber: position.lineNumber,
                        startColumn: word.startColumn,
                        endColumn: word.endColumn,
                    };

                    const getSortText = (
                        text: string,
                        priorityGroup: number,
                        categoryPrefix: string,
                    ) => {
                        if (!text) {
                            return `${priorityGroup}_${categoryPrefix}_3_`;
                        }
                        const lowerText = text.toLowerCase();
                        if (!typedText) {
                            return `${priorityGroup}_${categoryPrefix}_2_${text}`;
                        }
                        if (lowerText === typedText) {
                            return `${priorityGroup}_${categoryPrefix}_0_${text}`;
                        }
                        if (lowerText.startsWith(typedText)) {
                            return `${priorityGroup}_${categoryPrefix}_1_${text}`;
                        }
                        if (lowerText.includes(typedText)) {
                            return `${priorityGroup}_${categoryPrefix}_2_${text}`;
                        }
                        return `${priorityGroup}_${categoryPrefix}_3_${text}`;
                    };

                    const isRelevant = (text: string) => {
                        if (!typedText) return true;
                        const lowerText = text.toLowerCase();
                        return (
                            lowerText.startsWith(typedText) ||
                            lowerText.includes(typedText)
                        );
                    };

                    const contextualKeywords = new Set(
                        contextualConfig.keywords.map((value) =>
                            value.toUpperCase(),
                        ),
                    );
                    const contextKeywordSuggestions = Array.from(
                        contextualKeywords,
                    )
                        .filter((kw) => isRelevant(kw))
                        .map((kw) => ({
                            label: kw,
                            kind: monaco.languages.CompletionItemKind.Keyword,
                            insertText: kw,
                            filterText: kw,
                            range: wordRange,
                            sortText: getSortText(
                                kw,
                                getKeywordPriority(
                                    kw,
                                    contextualConfig.entityContext,
                                    true,
                                ),
                                "0",
                            ),
                        }));

                    const keywordSuggestions = pgsqlKeywords
                        .filter((kw) => !contextualKeywords.has(kw))
                        .filter((kw) => isRelevant(kw))
                        .map((kw) => ({
                            label: kw,
                            kind: monaco.languages.CompletionItemKind.Keyword,
                            insertText: kw,
                            filterText: kw,
                            range: wordRange,
                            sortText: getSortText(
                                kw,
                                getKeywordPriority(
                                    kw,
                                    contextualConfig.entityContext,
                                    false,
                                ),
                                "0",
                            ),
                        }));

                    const functionSuggestions = pgsqlFunctions
                        .filter((fn) => isRelevant(fn))
                        .map((fn) => ({
                            label: fn,
                            kind: monaco.languages.CompletionItemKind.Function,
                            insertText: `${fn}()`,
                            filterText: fn,
                            range: wordRange,
                            sortText: getSortText(fn, 4, "2"),
                        }));

                    const userTablePriority =
                        contextualConfig.entityContext === "relations" ? 0 : 3;
                    const systemTablePriority =
                        contextualConfig.entityContext === "relations" ? 1 : 4;
                    const viewPriority =
                        contextualConfig.entityContext === "relations" ? 2 : 4;
                    const schemaPriority =
                        contextualConfig.entityContext === "schemas" ? 1 : 4;
                    const columnPriority =
                        contextualConfig.entityContext === "columns" ? 0 : 3;
                    const indexPriority =
                        contextualConfig.entityContext === "indexes" ? 1 : 4;
                    const sequencePriority =
                        contextualConfig.entityContext === "relations" ? 3 : 4;

                    const userTableSuggestions = autocomplete.tables
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Class,
                            insertText: value,
                            filterText: value,
                            detail: "User table",
                            range: wordRange,
                            sortText: getSortText(
                                value,
                                userTablePriority,
                                "1",
                            ),
                        }));

                    const systemTableSuggestions = autocomplete.systemTables
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Struct,
                            insertText: value,
                            filterText: value,
                            detail: "System table",
                            range: wordRange,
                            sortText: getSortText(
                                value,
                                systemTablePriority,
                                "1",
                            ),
                        }));

                    const viewSuggestions = autocomplete.views
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Interface,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, viewPriority, "1"),
                        }));

                    const indexSuggestions = autocomplete.indexes
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Property,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, indexPriority, "1"),
                        }));

                    const sequenceSuggestions = autocomplete.sequences
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Enum,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, sequencePriority, "1"),
                        }));

                    const schemaSuggestions = autocomplete.schemas
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Module,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, schemaPriority, "1"),
                        }));

                    const columnSuggestions = autocomplete.columns
                        .filter((value) => value && isRelevant(value))
                        .map((value) => ({
                            label: value,
                            kind: monaco.languages.CompletionItemKind.Field,
                            insertText: value,
                            filterText: value,
                            range: wordRange,
                            sortText: getSortText(value, columnPriority, "1"),
                        }));

                    return {
                        suggestions: [
                            ...userTableSuggestions,
                            ...systemTableSuggestions,
                            ...viewSuggestions,
                            ...sequenceSuggestions,
                            ...schemaSuggestions,
                            ...columnSuggestions,
                            ...indexSuggestions,
                            ...contextKeywordSuggestions,
                            ...functionSuggestions,
                            ...keywordSuggestions,
                        ],
                    };
                },
            });
    }

    if (INLINE_AUTOCOMPLETE_ENABLED && !pgsqlInlineCompletionProvider) {
        pgsqlInlineCompletionProvider =
            monaco.languages.registerInlineCompletionsProvider("pgsql", {
                provideInlineCompletions: (
                    model: Monaco.editor.ITextModel,
                    position: Monaco.Position,
                ) => {
                    if (!pgsqlAutocompleteEnabled) {
                        return {
                            items: [],
                        };
                    }

                    if (!isCursorAtModelEnd(model, position)) {
                        return {
                            items: [],
                        };
                    }

                    const currentValue = model.getValue();
                    const matchingEntries =
                        getMatchingInlineAutocompleteEntries(
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
                        suppressSuggestions: false,
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
            autocompleteEnabled = true,
            autocomplete: autocompleteProp,
            highlightRange,
            highlightRanges,
            highlightVariant = "default",
        },
        ref,
    ) => {
        const autocomplete =
            autocompleteEnabled && autocompleteProp
                ? autocompleteProp
                : EMPTY_AUTOCOMPLETE;
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
            pgsqlAutocompleteEnabled = autocompleteEnabled;
            pgsqlAutocompleteData = autocomplete;
            updatePendingInlineAutocompleteRef.current();
        }, [autocomplete, autocompleteEnabled]);

        useEffect(() => {
            const editor = editorRef.current;
            let model: Monaco.editor.ITextModel | null = null;
            if (editor) {
                try {
                    model = editor.getModel();
                } catch {
                    model = null;
                }
            }
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

            if (!editor || !model || model.isDisposed()) {
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
            if (!INLINE_AUTOCOMPLETE_ENABLED) {
                return;
            }

            const editor = editorRef.current;

            if (!editor || !isEditorModelAlive(editor)) {
                return;
            }

            const updatePendingInlineAutocomplete = () => {
                if (!isEditorModelAlive(editor)) {
                    pendingInlineAutocompleteRef.current = null;
                    return;
                }

                const model = editor.getModel();
                const position = editor.getPosition();

                if (
                    !model ||
                    !position ||
                    !isCursorAtModelEnd(model, position)
                ) {
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
                if (!isEditorModelAlive(editor)) {
                    pendingInlineAutocompleteRef.current = null;
                    return;
                }

                if (
                    event.key !== "Tab" ||
                    event.shiftKey ||
                    event.altKey ||
                    event.ctrlKey ||
                    event.metaKey
                ) {
                    return;
                }

                if (isSuggestWidgetVisible(editor)) {
                    return;
                }

                const pendingInlineAutocomplete =
                    pendingInlineAutocompleteRef.current;

                if (!pgsqlAutocompleteEnabled || !pendingInlineAutocomplete) {
                    return;
                }

                const model = editor.getModel();
                const position = editor.getPosition();

                if (
                    !model ||
                    !position ||
                    !isCursorAtModelEnd(model, position)
                ) {
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
            };

            const domNode = editor.getDomNode();
            const modelChangeDisposable = editor.onDidChangeModelContent(() => {
                updatePendingInlineAutocomplete();
            });
            const cursorChangeDisposable = editor.onDidChangeCursorPosition(
                () => {
                    updatePendingInlineAutocomplete();
                },
            );

            domNode?.addEventListener("keydown", handleKeyDown, true);

            return () => {
                updatePendingInlineAutocompleteRef.current = () => undefined;
                pendingInlineAutocompleteRef.current = null;
                modelChangeDisposable.dispose();
                cursorChangeDisposable.dispose();
                domNode?.removeEventListener("keydown", handleKeyDown, true);
            };
        }, []);

        useEffect(() => {
            return () => {
                editorRef.current = null;
                updatePendingInlineAutocompleteRef.current = () => undefined;
                pendingInlineAutocompleteRef.current = null;
            };
        }, []);

        return (
            <MonacoEditor
                ref={ref}
                language="pgsql"
                beforeMount={registerAutocompletion}
                onMount={(editor) => {
                    editorRef.current = editor;
                    editor.onDidDispose(() => {
                        if (editorRef.current === editor) {
                            editorRef.current = null;
                        }
                        updatePendingInlineAutocompleteRef.current =
                            () => undefined;
                        pendingInlineAutocompleteRef.current = null;
                    });
                }}
                options={{
                    suggestOnTriggerCharacters: autocompleteEnabled,
                    quickSuggestions: autocompleteEnabled,
                    inlineSuggest: {
                        enabled:
                            INLINE_AUTOCOMPLETE_ENABLED && autocompleteEnabled,
                        mode: "prefix",
                        showToolbar: "onHover",
                    },
                    tabCompletion:
                        INLINE_AUTOCOMPLETE_ENABLED && autocompleteEnabled
                            ? "on"
                            : "off",
                    wordBasedSuggestions: autocompleteEnabled
                        ? "allDocuments"
                        : "off",
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
