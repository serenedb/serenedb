interface QueryTextResultsProps {
    rows: Record<string, unknown>[] | undefined;
}

const formatExplainValue = (value: unknown): string => {
    if (value === null || value === undefined) {
        return "";
    }

    if (
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean" ||
        typeof value === "bigint"
    ) {
        return String(value);
    }

    try {
        return JSON.stringify(value, null, 2);
    } catch {
        return String(value);
    }
};

const toExplainText = (rows: Record<string, unknown>[] | undefined): string => {
    const normalizedRows = Array.isArray(rows) ? rows : [];

    return normalizedRows
        .map((row) => {
            const entries = Object.entries(row || {});

            if (!entries.length) {
                return "";
            }

            // EXPLAIN usually returns a single plan column, so collapse rows
            // into a readable text block instead of a one-column grid.
            if (entries.length === 1) {
                return formatExplainValue(entries[0]?.[1]);
            }

            return entries
                .map(([key, value]) => `${key}: ${formatExplainValue(value)}`)
                .join("\n");
        })
        .filter(Boolean)
        .join("\n");
};

export const QueryTextResults: React.FC<QueryTextResultsProps> = ({ rows }) => {
    return (
        <pre className="h-full w-full overflow-auto whitespace-pre-wrap break-words p-3 text-sm leading-5 text-foreground font-mono">
            {toExplainText(rows)}
        </pre>
    );
};
