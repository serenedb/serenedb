/**
 * Formats data as pretty-printed JSON
 * @param data - Data to format
 * @param indent - Number of spaces for indentation
 * @returns Pretty-printed JSON string
 */
export const formatJSON = (
    data: Record<string, unknown>[],
    indent = 2,
): string => {
    return JSON.stringify(data, null, indent);
};

/**
 * Compacts JSON to single line (minified)
 * @param data - Data to format
 * @returns Minified JSON string
 */
export const minifyJSON = (data: Record<string, unknown>[]): string => {
    return JSON.stringify(data);
};
