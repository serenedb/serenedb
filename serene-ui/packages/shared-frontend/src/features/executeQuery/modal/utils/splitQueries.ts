import { parse } from "libpg-query";
import type { StatementRange } from "../types";

export interface SplitQueryStatement {
    query: string;
    statementIndex: number;
    startOffset: number;
    endOffset: number;
}

interface ParsedStatement {
    stmt_location?: number;
    stmt_len?: number;
}

interface ParseResult {
    stmts?: ParsedStatement[];
}

const isWhitespace = (c: string) =>
    c === " " || c === "\n" || c === "\t" || c === "\r";

const toTrimmedStatement = (
    input: string,
    startOffset: number,
    endOffset: number,
    statementIndex: number,
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
    };
};

const buildSingleStatementFallback = (query: string): SplitQueryStatement[] => {
    const statement = toTrimmedStatement(query, 0, query.length, 0);
    return statement ? [statement] : [];
};

export const splitQueries = async (
    query: string,
): Promise<SplitQueryStatement[]> => {
    try {
        const parsed = (await parse(query)) as ParseResult;
        const parsedStatements = parsed.stmts || [];

        console.log(parsed);
        if (!parsedStatements.length) {
            return buildSingleStatementFallback(query);
        }

        const statements = parsedStatements
            .map((statement, index) => {
                const startOffset = statement.stmt_location ?? 0;
                const nextStatementStart =
                    parsedStatements[index + 1]?.stmt_location;
                console.log(
                    "startOffset: ",
                    startOffset,
                    "\n nextStatementStart: ",
                    nextStatementStart,
                );
                const endOffset =
                    typeof statement.stmt_len === "number"
                        ? startOffset + statement.stmt_len
                        : typeof nextStatementStart === "number"
                          ? nextStatementStart
                          : query.length;

                return toTrimmedStatement(query, startOffset, endOffset, index);
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
