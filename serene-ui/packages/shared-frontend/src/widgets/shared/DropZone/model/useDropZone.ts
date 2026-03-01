import React, { useMemo, useState } from "react";

export interface UseDropZoneOptions {
    supportedExtensions: readonly string[];
    onFilesDrop: (files: File[]) => void;
    onRejectedFiles?: (files: File[]) => void;
}

interface SplitFilesResult {
    allowedFiles: File[];
    rejectedFiles: File[];
}

const normalizeExtension = (extension: string) =>
    extension.trim().toLowerCase().replace(/^\./, "");

const getFileExtension = (fileName: string) => {
    const lastDotIndex = fileName.lastIndexOf(".");

    if (lastDotIndex <= 0 || lastDotIndex === fileName.length - 1) {
        return null;
    }

    return fileName.slice(lastDotIndex + 1).toLowerCase();
};

const isFileDragEvent = (event: React.DragEvent<HTMLDivElement>) =>
    Array.from(event.dataTransfer.types).includes("Files");

const splitFilesByExtension = (
    fileList: FileList,
    supportedExtensions: ReadonlySet<string>,
): SplitFilesResult => {
    const files = Array.from(fileList);

    if (!supportedExtensions.size) {
        return {
            allowedFiles: files,
            rejectedFiles: [],
        };
    }

    return files.reduce<SplitFilesResult>(
        (result, file) => {
            const extension = getFileExtension(file.name);

            if (extension && supportedExtensions.has(extension)) {
                result.allowedFiles.push(file);
                return result;
            }

            result.rejectedFiles.push(file);
            return result;
        },
        {
            allowedFiles: [],
            rejectedFiles: [],
        },
    );
};

export const useDropZone = ({
    supportedExtensions,
    onFilesDrop,
    onRejectedFiles,
}: UseDropZoneOptions) => {
    const [dragDepth, setDragDepth] = useState(0);

    const normalizedExtensions = useMemo(
        () =>
            supportedExtensions
                .map(normalizeExtension)
                .filter((extension) => extension.length > 0),
        [supportedExtensions],
    );

    const supportedExtensionSet = useMemo(
        () => new Set(normalizedExtensions),
        [normalizedExtensions],
    );

    const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
        if (!isFileDragEvent(event)) {
            return;
        }

        event.preventDefault();
        event.dataTransfer.dropEffect = "copy";
    };

    const handleDragEnter = (event: React.DragEvent<HTMLDivElement>) => {
        if (!isFileDragEvent(event)) {
            return;
        }

        event.preventDefault();
        setDragDepth((currentDepth) => currentDepth + 1);
    };

    const handleDragLeave = (event: React.DragEvent<HTMLDivElement>) => {
        if (!isFileDragEvent(event)) {
            return;
        }

        event.preventDefault();

        if (event.relatedTarget === null) {
            setDragDepth(0);
            return;
        }

        setDragDepth((currentDepth) => Math.max(currentDepth - 1, 0));
    };

    const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
        if (!isFileDragEvent(event)) {
            return;
        }

        event.preventDefault();
        setDragDepth(0);

        const { allowedFiles, rejectedFiles } = splitFilesByExtension(
            event.dataTransfer.files,
            supportedExtensionSet,
        );

        if (allowedFiles.length > 0) {
            onFilesDrop(allowedFiles);
        }

        if (rejectedFiles.length > 0) {
            onRejectedFiles?.(rejectedFiles);
        }
    };

    return {
        isDragging: dragDepth > 0,
        normalizedExtensions,
        rootProps: {
            onDragEnter: handleDragEnter,
            onDragOver: handleDragOver,
            onDragLeave: handleDragLeave,
            onDrop: handleDrop,
        },
    };
};
