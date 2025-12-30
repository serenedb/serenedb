/**
 * Validates the query string.
 * @param query The query string to be validated.
 * @returns An error message if the query is invalid, or null if it is valid.
 */
export const validateQuery = (query: string): string | null => {
    if (!query.trim()) {
        return "Query cannot be empty";
    }
    return null;
};

/**
 * Validates the connection details.
 * @param connectionId The ID of the connection.
 * @param database The name of the database.
 * @returns An error message if the connection is invalid, or null if it is valid.
 */
export const validateConnection = (
    connectionId: number | undefined,
    database: string | undefined,
): string | null => {
    if (!connectionId || !database) {
        return "Connection not found";
    }
    return null;
};

/**
 * Validates the limit value.
 * @param limit The limit value to be validated.
 * @returns An error message if the limit is invalid, or null if it is valid.
 */
export const validateLimit = (limit: number): string | null => {
    if (limit <= 0) {
        return "Limit must be greater than 0";
    }
    return null;
};

/**
 * Validates the job ID.
 * @param jobId The job ID to be validated.
 * @returns An error message if the job ID is invalid, or null if it is valid.
 */
export const validateJobId = (jobId: unknown): string | null => {
    if (typeof jobId !== "number") {
        return "Invalid job ID received from server";
    }
    const numJobId = Number(jobId);
    if (!Number.isInteger(numJobId) || numJobId <= 0) {
        return "Invalid job ID received from server";
    }
    return null;
};
