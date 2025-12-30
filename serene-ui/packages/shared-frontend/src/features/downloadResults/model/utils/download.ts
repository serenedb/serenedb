/**
 * Generates a timestamp string for filenames
 * Includes milliseconds to avoid collisions with rapid successive calls
 * @returns ISO timestamp string formatted for filenames
 */
export const generateTimestamp = (): string => {
    return new Date()
        .toISOString()
        .replace(/[:]/g, "-")
        .replace(/\./g, "-")
        .slice(0, -1); // Remove trailing 'Z'
};

/**
 * Generates a unique filename with counter if needed
 * @param baseName - Base filename without extension
 * @param extension - File extension (without dot)
 * @param counter - Optional counter for uniqueness
 * @returns Complete filename
 */
export const generateFilename = (
    baseName: string,
    extension: string,
    counter?: number,
): string => {
    const timestamp = generateTimestamp();
    const counterStr = counter ? `-${counter}` : "";
    return `${baseName}-${timestamp}${counterStr}.${extension}`;
};

/**
 * Triggers a file download by creating a temporary link and clicking it
 * Properly cleans up resources after download
 *
 * @param blob - Blob object containing file data
 * @param filename - Name for the downloaded file
 * @throws Error if document body is not available
 */
export const triggerDownload = (blob: Blob, filename: string): void => {
    if (!document.body) {
        throw new Error("Document body is not available");
    }

    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");

    try {
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
    } finally {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }
};

/**
 * Creates a Blob from string content with specified type
 * @param content - String content
 * @param mimeType - MIME type for the blob
 * @returns Blob object
 */
export const createBlob = (content: string, mimeType: string): Blob => {
    return new Blob([content], { type: `${mimeType};charset=utf-8` });
};

/**
 * Copies text to clipboard with error handling
 * @param text - Text to copy
 * @returns Promise that resolves when copy is complete
 * @throws Error if clipboard API is not available or copy fails
 */
export const copyToClipboard = async (text: string): Promise<void> => {
    if (!navigator.clipboard) {
        throw new Error("Clipboard API is not available");
    }

    try {
        await navigator.clipboard.writeText(text);
    } catch (error) {
        throw new Error(
            `Failed to copy to clipboard: ${error instanceof Error ? error.message : "Unknown error"}`,
        );
    }
};

/**
 * Checks if clipboard API is available
 * @returns Boolean indicating clipboard availability
 */
export const isClipboardAvailable = (): boolean => {
    return !!navigator.clipboard;
};
