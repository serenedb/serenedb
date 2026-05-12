import { parse } from "libpg-query";
import type { StatementRange } from "../types";

export interface SplitQueryStatement {
    query: string;
    statementIndex: number;
    startOffset: number;
    endOffset: number;
    statementType?: string;
}

interface ParsedStatement {
    stmt_location?: number;
    stmt_len?: number;
    stmt?: Record<string, unknown>;
}

interface ParseResult {
    stmts?: ParsedStatement[];
}

const isWhitespace = (c: string) =>
    c === " " || c === "\n" || c === "\t" || c === "\r";

const getStatementType = (statement?: Record<string, unknown>) =>
    statement ? Object.keys(statement)[0] : undefined;

const toTrimmedStatement = (
    input: string,
    startOffset: number,
    endOffset: number,
    statementIndex: number,
    statementType?: string,
): SplitQueryStatement | null => {
    let trimmedStart = startOffset;
    let trimmedEnd = endOffset;

    while (trimmedStart < trimmedEnd && isWhitespace(input[trimmedStart])) {
        trimmedStart += 1;
    }

    while (trimmedEnd > trimmedStart && isWhitespace(input[trimmedEnd - 1])) {
        trimmedEnd -= 1;
    }

    if (trimmedStart >= trimmedEnd) {
        return null;
    }

    return {
        query: input.slice(trimmedStart, trimmedEnd),
        statementIndex,
        startOffset: trimmedStart,
        endOffset: trimmedEnd,
        statementType,
    };
};

const buildSingleStatementFallback = (
    query: string,
    statementType?: string,
): SplitQueryStatement[] => {
    const statement = toTrimmedStatement(
        query,
        0,
        query.length,
        0,
        statementType,
    );
    return statement ? [statement] : [];
};

const parseStatements = async (query: string): Promise<ParsedStatement[]> => {
    const parsed = (await parse(query)) as ParseResult;
    return parsed.stmts || [];
};

export const getSingleStatementType = async (
    query: string | undefined,
): Promise<string | undefined> => {
    if (!query?.trim()) {
        return undefined;
    }

    try {
        const parsedStatements = await parseStatements(query);

        if (parsedStatements.length !== 1) {
            return undefined;
        }

        return getStatementType(parsedStatements[0]?.stmt);
    } catch {
        return undefined;
    }
};

export const splitQueries = async (
    query: string,
): Promise<SplitQueryStatement[]> => {
    try {
        const parsedStatements = await parseStatements(query);

        if (!parsedStatements.length) {
            return buildSingleStatementFallback(query);
        }

        const statements = parsedStatements
            .map((statement, index) => {
                const startOffset = statement.stmt_location ?? 0;
                const nextStatementStart =
                    parsedStatements[index + 1]?.stmt_location;
                const endOffset =
                    typeof statement.stmt_len === "number"
                        ? startOffset + statement.stmt_len
                        : typeof nextStatementStart === "number"
                          ? nextStatementStart
                          : query.length;

                return toTrimmedStatement(
                    query,
                    startOffset,
                    endOffset,
                    index,
                    getStatementType(statement.stmt),
                );
            })
            .filter(
                (statement): statement is SplitQueryStatement =>
                    statement !== null,
            );

        return statements.length
            ? statements
            : buildSingleStatementFallback(query);
    } catch (error) {
        return buildSingleStatementFallback(query);
    }
};

export const toStatementRange = (
    statement: Pick<SplitQueryStatement, "startOffset" | "endOffset">,
): StatementRange => ({
    startOffset: statement.startOffset,
    endOffset: statement.endOffset,
});
