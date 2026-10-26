import { useCallback, useState } from "react";
import {
    normalizeRows,
    toCSV,
    formatJSON,
    generateFilename,
    triggerDownload,
    createBlob,
    copyToClipboard,
    isClipboardAvailable,
    DownloadError,
    ClipboardError,
    DOWNLOAD_ERROR_CODES,
    CLIPBOARD_ERROR_CODES,
    createErrorNotification,
    createSuccessNotification,
} from "./utils";

export interface DownloadResultsContextType {
    downloadJSON: (results: Record<string, unknown>[]) => Promise<void>;
    downloadCSV: (results: Record<string, unknown>[]) => Promise<void>;
    copyJSON: (results: Record<string, unknown>[]) => Promise<void>;
    copyCSV: (results: Record<string, unknown>[]) => Promise<void>;
    isLoading: boolean;
    error: Error | null;
}

/**
 * Hook for downloading and copying results in various formats
 * Provides error handling and notifications via callback
 *
 * @param onNotify - Optional callback for user notifications (success/error messages)
 * @returns Object with download/copy functions and state
 */
export const useDownloadResults = (): DownloadResultsContextType => {
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<Error | null>(null);

    const handleError = useCallback((err: unknown, operation: string) => {
        const message =
            err instanceof Error ? err.message : "Unknown error occurred";
        const newError = new Error(`${operation} failed: ${message}`);
        setError(newError);
        createErrorNotification(err, `Failed to ${operation}`);
    }, []);

    const handleSuccess = useCallback((message: string) => {
        setError(null);
        createSuccessNotification(message);
    }, []);

    /**
     * Downloads results as JSON file
     */
    const downloadJSON = useCallback(
        async (results: Record<string, unknown>[]) => {
            try {
                setIsLoading(true);
                setError(null);

                const normalized = normalizeRows(results);
                if (!normalized) {
                    throw new DownloadError(
                        "Invalid data format",
                        DOWNLOAD_ERROR_CODES.INVALID_DATA,
                    );
                }

                const content = formatJSON(normalized);
                const blob = createBlob(content, "application/json");
                const filename = generateFilename("results", "json");
                triggerDownload(blob, filename);

                handleSuccess("JSON file downloaded successfully");
            } catch (err) {
                handleError(err, "JSON download");
            } finally {
                setIsLoading(false);
            }
        },
        [handleError, handleSuccess],
    );

    /**
     * Downloads results as CSV file
     */
    const downloadCSV = useCallback(
        async (results: Record<string, unknown>[]) => {
            try {
                setIsLoading(true);
                setError(null);

                const normalized = normalizeRows(results);
                if (!normalized) {
                    throw new DownloadError(
                        "Invalid data format",
                        DOWNLOAD_ERROR_CODES.INVALID_DATA,
                    );
                }

                const csv = toCSV(normalized, {
                    includeHeaders: true,
                    includeBOM: true,
                    includeSeparator: true,
                    flatten: true,
                });

                const blob = createBlob(csv, "text/csv");
                const filename = generateFilename("results", "csv");
                triggerDownload(blob, filename);

                handleSuccess("CSV file downloaded successfully");
            } catch (err) {
                handleError(err, "CSV download");
            } finally {
                setIsLoading(false);
            }
        },
        [handleError, handleSuccess],
    );

    /**
     * Copies results as JSON to clipboard
     */
    const copyJSON = useCallback(
        async (results: Record<string, unknown>[]) => {
            try {
                setIsLoading(true);
                setError(null);

                if (!isClipboardAvailable()) {
                    throw new ClipboardError(
                        "Clipboard API is not available",
                        CLIPBOARD_ERROR_CODES.NOT_AVAILABLE,
                    );
                }

                const normalized = normalizeRows(results);
                if (!normalized) {
                    throw new DownloadError(
                        "Invalid data format",
                        DOWNLOAD_ERROR_CODES.INVALID_DATA,
                    );
                }

                const content = formatJSON(normalized);
                await copyToClipboard(content);

                handleSuccess("JSON copied to clipboard");
            } catch (err) {
                handleError(err, "JSON copy");
            } finally {
                setIsLoading(false);
            }
        },
        [handleError, handleSuccess],
    );

    /**
     * Copies results as CSV to clipboard
     */
    const copyCSV = useCallback(
        async (results: Record<string, unknown>[]) => {
            try {
                setIsLoading(true);
                setError(null);

                if (!isClipboardAvailable()) {
                    throw new ClipboardError(
                        "Clipboard API is not available",
                        CLIPBOARD_ERROR_CODES.NOT_AVAILABLE,
                    );
                }

                const normalized = normalizeRows(results);
                if (!normalized) {
                    throw new DownloadError(
                        "Invalid data format",
                        DOWNLOAD_ERROR_CODES.INVALID_DATA,
                    );
                }

                const csv = toCSV(normalized, {
                    includeHeaders: true,
                    includeBOM: true,
                    includeSeparator: false,
                    flatten: true,
                });

                await copyToClipboard(csv);

                handleSuccess("CSV copied to clipboard");
            } catch (err) {
                handleError(err, "CSV copy");
            } finally {
                setIsLoading(false);
            }
        },
        [handleError, handleSuccess],
    );

    return {
        downloadJSON,
        downloadCSV,
        copyJSON,
        copyCSV,
        isLoading,
        error,
    };
};
