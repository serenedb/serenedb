import { toast } from "sonner";

export class DownloadError extends Error {
    constructor(
        message: string,
        public readonly code: string,
        public readonly originalError?: Error,
    ) {
        super(message);
        this.name = "DownloadError";
    }
}

export class ClipboardError extends Error {
    constructor(
        message: string,
        public readonly code: string,
        public readonly originalError?: Error,
    ) {
        super(message);
        this.name = "ClipboardError";
    }
}

export const DOWNLOAD_ERROR_CODES = {
    INVALID_DATA: "INVALID_DATA",
    BLOB_CREATION_FAILED: "BLOB_CREATION_FAILED",
    DOWNLOAD_FAILED: "DOWNLOAD_FAILED",
    NO_DOCUMENT_BODY: "NO_DOCUMENT_BODY",
} as const;

export const CLIPBOARD_ERROR_CODES = {
    NOT_AVAILABLE: "NOT_AVAILABLE",
    PERMISSION_DENIED: "PERMISSION_DENIED",
    COPY_FAILED: "COPY_FAILED",
} as const;

export const ERROR_MESSAGES: Record<string, string> = {
    [DOWNLOAD_ERROR_CODES.INVALID_DATA]:
        "Invalid data format. Please check your data and try again.",
    [DOWNLOAD_ERROR_CODES.BLOB_CREATION_FAILED]:
        "Failed to create file. Please try again.",
    [DOWNLOAD_ERROR_CODES.DOWNLOAD_FAILED]:
        "Download failed. Please try again.",
    [DOWNLOAD_ERROR_CODES.NO_DOCUMENT_BODY]:
        "Unable to download: document not ready.",
    [CLIPBOARD_ERROR_CODES.NOT_AVAILABLE]:
        "Clipboard is not available. Please copy manually.",
    [CLIPBOARD_ERROR_CODES.PERMISSION_DENIED]:
        "Clipboard permission denied. Please enable it in settings.",
    [CLIPBOARD_ERROR_CODES.COPY_FAILED]:
        "Failed to copy to clipboard. Please try again.",
};

/**
 * Creates a user-friendly error message
 * @param error - The error to format
 * @returns User-friendly message
 */
export const getErrorMessage = (error: unknown): string => {
    if (error instanceof DownloadError) {
        return ERROR_MESSAGES[error.code] || error.message;
    }

    if (error instanceof ClipboardError) {
        return ERROR_MESSAGES[error.code] || error.message;
    }

    if (error instanceof Error) {
        return error.message;
    }

    return "An unknown error occurred. Please try again.";
};

export interface NotificationPayload {
    type: "success" | "error" | "info" | "warning";
    message: string;
    duration?: number;
    code?: string;
}

/**
 * Validates and creates a normalized error payload
 * @param error - Error that occurred
 * @param defaultMessage - Default message if error can't be parsed
 * @returns Notification payload
 */
export const createErrorNotification = (
    error: unknown,
    defaultMessage = "Operation failed",
    duration = 5000,
) => {
    const message = getErrorMessage(error);

    toast.error(message || defaultMessage, {
        duration: duration,
    });
};

/**
 * Creates a success notification payload
 * @param message - Success message
 * @param duration - Duration in milliseconds
 * @returns Notification payload
 */
export const createSuccessNotification = (message: string, duration = 3000) => {
    toast.success(message, {
        duration: duration,
    });
};
