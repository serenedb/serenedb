/**
 * Escapes a cell value for CSV format
 * Handles quotes, newlines, and special characters
 *
 * @param value - The cell value to escape
 * @returns Properly escaped CSV cell
 */
const escapeCell = (value: unknown): string => {
    if (value === null || value === undefined) return "";

    const stringValue =
        typeof value === "object" ? JSON.stringify(value) : String(value);
    const needsQuotes = /[",\n\r]/.test(stringValue);
    const escaped = stringValue.replace(/"/g, '""');

    return needsQuotes ? `"${escaped}"` : escaped;
};

/**
 * Collects all unique header keys from normalized rows
 * Preserves order of first occurrence
 *
 * @param rows - Normalized array of objects
 * @returns Array of unique header keys
 */
const collectHeaders = (rows: Record<string, unknown>[]): string[] => {
    const headerSet = new Set<string>();

    rows.forEach((row) => {
        Object.keys(row ?? {}).forEach((key) => headerSet.add(key));
    });

    return Array.from(headerSet);
};

/**
 * Converts normalized rows to CSV format
 * Includes proper escaping and quote handling
 *
 * @param rows - Normalized array of objects
 * @param options - CSV export options
 * @returns CSV string content
 */
export const toCSV = (
    rows: Record<string, unknown>[],
    options: {
        includeHeaders?: boolean;
        includeBOM?: boolean;
        includeSeparator?: boolean;
        flatten?: boolean;
    } = {},
): string => {
    const {
        includeHeaders = true,
        includeBOM = false,
        includeSeparator = false,
        flatten = false,
    } = options;

    if (!rows || rows.length === 0) return includeBOM ? "\uFEFF" : "";

    const processedRows = flatten
        ? rows.map((row) => flattenRowForCSV(row))
        : rows;

    const headers = collectHeaders(processedRows);

    const lines: string[] = [];

    if (includeSeparator) {
        lines.push("sep=,");
    }

    if (includeHeaders) {
        lines.push(headers.map(escapeCell).join(","));
    }

    for (const row of processedRows) {
        const line = headers
            .map((header) => escapeCell((row as any)[header]))
            .join(",");
        lines.push(line);
    }

    const content = lines.join("\r\n");
    return includeBOM ? `\uFEFF${content}` : content;
};

/**
 * Flattens an object for CSV export (converts nested objects to dot notation)
 * @param obj - Object to flatten
 * @param prefix - Prefix for nested keys
 * @returns Flattened object
 */
const flattenRowForCSV = (
    obj: Record<string, unknown>,
    prefix = "",
): Record<string, unknown> => {
    const result: Record<string, unknown> = {};

    Object.entries(obj).forEach(([key, value]) => {
        const newKey = prefix ? `${prefix}.${key}` : key;

        if (
            value !== null &&
            typeof value === "object" &&
            !Array.isArray(value) &&
            !(value instanceof Date)
        ) {
            Object.assign(
                result,
                flattenRowForCSV(value as Record<string, unknown>, newKey),
            );
        } else if (Array.isArray(value)) {
            result[newKey] = JSON.stringify(value);
        } else {
            result[newKey] = value;
        }
    });

    return result;
};
