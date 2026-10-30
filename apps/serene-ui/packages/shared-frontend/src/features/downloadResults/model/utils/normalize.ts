/**
 * Normalizes various input formats into a consistent array of objects
 * Handles strings (JSON), arrays, and objects
 *
 * @param input - The data to normalize (string, array, object, or primitives)
 * @returns Normalized array of records or undefined if normalization fails
 */
export const normalizeRows = (
    input: any,
): Record<string, unknown>[] | undefined => {
    if (!input) return undefined;

    try {
        if (typeof input === "string") {
            const parsed = JSON.parse(input);
            return normalizeRows(parsed);
        }

        if (Array.isArray(input)) {
            if (input.length === 0) return [];

            if (typeof input[0] === "string") {
                const parsed = input
                    .map((s) => {
                        try {
                            return JSON.parse(s);
                        } catch {
                            return undefined;
                        }
                    })
                    .filter(Boolean);
                return parsed as Record<string, unknown>[];
            }

            if (typeof input[0] === "object") {
                return input as Record<string, unknown>[];
            }

            return input.map((v) => ({ value: v })) as Record<
                string,
                unknown
            >[];
        }

        if (typeof input === "object") {
            return [input as Record<string, unknown>];
        }
    } catch {
        return undefined;
    }

    return undefined;
};

/**
 * Flattens nested objects into a single level with dot notation
 * @param obj - Object to flatten
 * @param prefix - Prefix for nested keys
 * @returns Flattened object
 */
export const flattenObject = (
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
                flattenObject(value as Record<string, unknown>, newKey),
            );
        } else {
            result[newKey] = value;
        }
    });

    return result;
};

/**
 * Converts deeply nested rows to flattened structure for better CSV export
 * @param rows - Array of potentially nested objects
 * @returns Array of flattened objects
 */
export const flattenRows = (
    rows: Record<string, unknown>[],
): Record<string, unknown>[] => {
    return rows.map((row) => flattenObject(row));
};
